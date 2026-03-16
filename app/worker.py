import asyncio
import json
import logging
from typing import Any

import httpx
from langchain_openai import ChatOpenAI
from openai import AsyncOpenAI

from app.config import Settings, get_settings
from app.services.job import (
    claim_due_postprocess_work,
    claim_recovery_job_ids,
    process_postprocess_work,
    process_sqs_message,
    recover_job,
)
from app.services.llm import get_async_client, get_langchain_client
from app.services.sqs import build_sqs_client

logger = logging.getLogger(__name__)


class BackgroundWorkerManager:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self._shutdown_event = asyncio.Event()
        self._work_available_event = asyncio.Event()
        self._submit_semaphore = asyncio.Semaphore(
            self.settings.worker_submit_concurrency
        )
        self._postprocess_semaphore = asyncio.Semaphore(
            self.settings.worker_postprocess_concurrency
        )
        self._submit_tasks: set[asyncio.Task[None]] = set()
        self._postprocess_tasks: set[asyncio.Task[None]] = set()
        self._loop_tasks: list[asyncio.Task[None]] = []
        self._openai_client: AsyncOpenAI | None = None
        self._langchain_client: ChatOpenAI | None = None
        self._downstream_client: httpx.AsyncClient | None = None
        self._sqs_client: Any | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        if not self.settings.resolved_sqs_listener_queue_url:
            raise RuntimeError(
                "SQS listener queue must be configured when ENABLE_INPROCESS_WORKER=true."
            )

        self._openai_client = get_async_client()
        self._langchain_client = get_langchain_client()
        self._downstream_client = httpx.AsyncClient()
        self._sqs_client = build_sqs_client(self.settings)
        self._loop_tasks = [
            asyncio.create_task(self._poll_loop(), name="pickle-sqs-poll-loop"),
            asyncio.create_task(
                self._postprocess_loop(), name="pickle-postprocess-loop"
            ),
            asyncio.create_task(self._recovery_loop(), name="pickle-recovery-loop"),
        ]
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return

        self._shutdown_event.set()
        self._work_available_event.set()

        await self._drain_tasks(
            self._loop_tasks, self.settings.worker_shutdown_grace_seconds
        )
        await self._drain_tasks(
            list(self._submit_tasks), self.settings.worker_shutdown_grace_seconds
        )
        await self._drain_tasks(
            list(self._postprocess_tasks), self.settings.worker_shutdown_grace_seconds
        )

        if self._downstream_client is not None:
            await self._downstream_client.aclose()
        if self._openai_client is not None:
            await self._openai_client.close()

        self._loop_tasks = []
        self._submit_tasks.clear()
        self._postprocess_tasks.clear()
        self._started = False

    def notify_work_available(self) -> None:
        self._work_available_event.set()

    async def _poll_loop(self) -> None:
        if self._sqs_client is None:
            return

        while not self._shutdown_event.is_set():
            try:
                messages = await self._receive_messages()
            except Exception:
                logger.exception("Failed to receive SQS messages")
                await asyncio.sleep(self.settings.worker_idle_sleep_seconds)
                continue

            if not messages:
                await asyncio.sleep(self.settings.worker_idle_sleep_seconds)
                continue

            for message in messages:
                if self._shutdown_event.is_set():
                    break
                await self._submit_semaphore.acquire()
                task = asyncio.create_task(
                    self._run_submit_task(message),
                    name=f"pickle-submit-{message.get('MessageId', 'unknown')}",
                )
                self._track_task(
                    task=task,
                    task_set=self._submit_tasks,
                    semaphore=self._submit_semaphore,
                )

    async def _postprocess_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._work_available_event.wait(),
                    timeout=self.settings.postprocess_poll_interval_seconds,
                )
            except TimeoutError:
                pass

            self._work_available_event.clear()
            if self._shutdown_event.is_set():
                break

            available_slots = self._available_postprocess_slots()
            if available_slots <= 0:
                continue

            work_items = await claim_due_postprocess_work(
                min(self.settings.postprocess_batch_size, available_slots)
            )
            for work_kind, job_id in work_items:
                await self._postprocess_semaphore.acquire()
                task = asyncio.create_task(
                    self._run_postprocess_task(work_kind, job_id),
                    name=f"pickle-postprocess-{work_kind}-{job_id}",
                )
                self._track_task(
                    task=task,
                    task_set=self._postprocess_tasks,
                    semaphore=self._postprocess_semaphore,
                )

    async def _recovery_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.settings.recovery_poll_interval_seconds,
                )
            except TimeoutError:
                pass

            if self._shutdown_event.is_set():
                break

            available_slots = self._available_postprocess_slots()
            if available_slots <= 0:
                continue

            job_ids = await claim_recovery_job_ids(
                min(self.settings.recovery_batch_size, available_slots)
            )
            for job_id in job_ids:
                await self._postprocess_semaphore.acquire()
                task = asyncio.create_task(
                    self._run_recovery_task(job_id),
                    name=f"pickle-recovery-{job_id}",
                )
                self._track_task(
                    task=task,
                    task_set=self._postprocess_tasks,
                    semaphore=self._postprocess_semaphore,
                )

    async def _receive_messages(self) -> list[dict[str, Any]]:
        queue_url = self.settings.resolved_sqs_listener_queue_url
        if not queue_url:
            return []
        response = await asyncio.to_thread(
            self._sqs_client.receive_message,
            QueueUrl=queue_url,
            MaxNumberOfMessages=self.settings.sqs_max_messages,
            WaitTimeSeconds=self.settings.sqs_wait_time_seconds,
            VisibilityTimeout=self.settings.sqs_visibility_timeout_seconds,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        print(f"📥 Received {len(messages)} messages from SQS queue")
        return messages

    async def _delete_message(self, receipt_handle: str) -> None:
        queue_url = self.settings.resolved_sqs_listener_queue_url
        if not queue_url:
            return
        await asyncio.to_thread(
            self._sqs_client.delete_message,
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )

    async def _run_submit_task(self, message: dict[str, Any]) -> None:
        try:
            should_delete = await process_sqs_message(
                message,
                langchain_client=self._langchain_client,
            )
            if should_delete:
                await self._delete_message(message["ReceiptHandle"])
                self.notify_work_available()
        except Exception:
            logger.exception(
                "Failed to process SQS message: %s",
                json.dumps(message, default=str),
            )

    async def _run_postprocess_task(self, work_kind: str, job_id: int) -> None:
        try:
            await process_postprocess_work(
                work_kind,
                job_id,
                downstream_client=self._downstream_client,
                sqs_client=self._sqs_client,
            )
            self.notify_work_available()
        except Exception:
            logger.exception(
                "Failed to process postprocess work kind=%s job_id=%s",
                work_kind,
                job_id,
            )

    async def _run_recovery_task(self, job_id: int) -> None:
        try:
            await recover_job(job_id, openai_client=self._openai_client)
            self.notify_work_available()
        except Exception:
            logger.exception("Failed to recover stale job job_id=%s", job_id)

    def _available_postprocess_slots(self) -> int:
        return max(
            0,
            self.settings.worker_postprocess_concurrency - len(self._postprocess_tasks),
        )

    def _track_task(
        self,
        *,
        task: asyncio.Task[None],
        task_set: set[asyncio.Task[None]],
        semaphore: asyncio.Semaphore,
    ) -> None:
        task_set.add(task)

        def _cleanup(completed_task: asyncio.Task[None]) -> None:
            task_set.discard(completed_task)
            semaphore.release()
            try:
                completed_task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Background task failed")

        task.add_done_callback(_cleanup)

    async def _drain_tasks(
        self,
        tasks: list[asyncio.Task[None]],
        timeout: float,
    ) -> None:
        if not tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=timeout,
            )
        except TimeoutError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

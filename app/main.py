from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import get_settings
from app.database import close_database, init_database
from app.routers.health import router as health_router
from app.routers.job import router as job_router
from app.worker import BackgroundWorkerManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    manager: BackgroundWorkerManager | None = None
    if settings.db_auto_create_tables:
        await init_database()
    if settings.enable_inprocess_worker:
        manager = BackgroundWorkerManager(settings)
        app.state.background_manager = manager
        await manager.start()
    try:
        yield
    finally:
        if manager is not None:
            await manager.stop()
        await close_database()


def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    app.include_router(health_router)
    app.include_router(job_router)
    return app


app = create_app()

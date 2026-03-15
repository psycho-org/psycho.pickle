# psycho.pickle

Relay server for asynchronous LLM API processing.
Consumes inbound jobs from AWS SQS and can publish completion notifications back to AWS SQS or HTTP.

## Overview

Business server forwards job requests to this relay server. The relay handles all LLM API egress, awaits callbacks, and persists results to the database. The business server is notified upon completion.

```plain text
Business Server → Request SQS → Relay (psycho.pickle) → LLM API
                                  ↓
                                 DB
                                  ↓
                    Result SQS or Business Server (notification)
```

## Architecture

Uses a 2-layer (Router → Service) architecture.
No separate Repository/Persistence layer — SQLAlchemy already provides DB abstraction, and the table count is small enough for Services to handle sessions directly.

``` plain text
Router (Controller)  →  Service (Business Logic)  →  외부 의존성
routers/job          →  services/job              →  services/llm (OpenAI)
                                                   →  database (DB)
```

``` plain text
app/
├── main.py          # FastAPI app, router registration
├── database.py      # DB engine, session, Base
├── models.py        # SQLAlchemy table mappings
├── schemas.py       # Pydantic request/response models
├── routers/
│   ├── health.py    # /health
│   └── job.py       # Job request, webhook callback endpoints
└── services/
    ├── llm.py       # OpenAI API client
    └── job.py       # Job business logic + DB handling
```

## Tech Stack

- **Runtime**: Python 3.12
- **Framework**: FastAPI
- **Server**: Uvicorn (standard)
- **Database**: PostgreSQL 17 via SQLAlchemy (async) + asyncpg
- **Package manager**: uv

## Infrastructure

- Fixed EC2 instances (t3.small) in AZ-B and AZ-D
- In-place deployment via AWS CodeDeploy
- Health check: `GET /health` on port 8000
- Environment variables injected via AWS SSM Parameter Store at deploy time

## Development

```bash
# Install dependencies
uv sync

# Install pre-commit hooks (auto lint + format on commit)
uv run pre-commit install

# Create local profile
cp .env.local.example .env.local

# Run locally
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- Local development reads `.env.local`.
- Runtime config is loaded from `.env`, then overridden by `.env.local` when present.
- Tests read `.env.test`.
- Production does not depend on `.env.local`; CodeDeploy injects runtime environment variables from SSM.
- Queue config:
  - `SQS_LISTENER_QUEUE_URL` or legacy `SQS_QUEUE_URL`: inbound job queue
  - `BUSINESS_NOTIFY_SQS_QUEUE_URL`: outbound result queue
  - `BUSINESS_NOTIFY_URL`: optional HTTP fallback for result notifications

## Deployment

Triggered automatically via GitHub Actions on push to `master`.
CodeDeploy performs an in-place deployment across both AZ-B and AZ-D instances simultaneously.

# psycho.pickle

Relay server for asynchronous LLM API processing.

## Overview

Business server forwards job requests to this relay server. The relay handles all LLM API egress, awaits callbacks, and persists results to the database. The business server is notified upon completion.

```plain text
Business Server → Relay (psycho.pickle) → LLM API
                            ↓
                           DB
                            ↓
                    Business Server (notification)
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

# Run locally
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Deployment

Triggered automatically via GitHub Actions on push to `master`.
CodeDeploy performs an in-place deployment across both AZ-B and AZ-D instances simultaneously.

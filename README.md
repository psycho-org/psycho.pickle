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

``` plain text
app/
├── main.py          # FastAPI app, router registration, lifespan
├── database.py      # DB engine, session, Base
├── models.py        # SQLAlchemy table mappings
├── routers/
│   ├── health.py    # /health
│   └── jobs.py      # Job request endpoints
├── services/
│   ├── llm.py       # OpenAI API calls, callback handling
│   └── job.py       # Job state management (DB read/write)
└── schemas.py       # Pydantic request/response models
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

# Run locally
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Deployment

Triggered automatically via GitHub Actions on push to `main`.
CodeDeploy performs an in-place deployment across both AZ-B and AZ-D instances simultaneously.

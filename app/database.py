from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import get_settings


def _build_database_url() -> str:
    s = get_settings()
    return (
        f"postgresql+asyncpg://{s.db_username}:{s.db_password}"
        f"@{s.db_host}:{s.db_port}/{s.db_name}"
    )


engine = create_async_engine(_build_database_url(), echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

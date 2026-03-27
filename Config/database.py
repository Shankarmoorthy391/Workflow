

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic_settings import BaseSettings
from contextlib import asynccontextmanager

# ✅ Load settings from .env
class Settings(BaseSettings):
    pgdb_uri: str  # Example: "postgresql://user:password@localhost/dbname"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra='allow'

settings = Settings()
# print("Database URI:", settings.pgdb_uri)  # Debugging line to check the database URI
# ✅ Create Synchronous Engine (For Scripts, Legacy Code)
sync_engine = create_engine(settings.pgdb_uri, echo=True)
SyncSessionLocal = sessionmaker(bind=sync_engine, autoflush=False, autocommit=False)

# ✅ Create Asynchronous Engine (For FastAPI)
async_engine = create_async_engine(settings.pgdb_uri.replace("postgresql://", "postgresql+asyncpg://"), echo=True)
AsyncSessionLocal = sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)

# ✅ Base Model for ORM
Base = declarative_base()

# ✅ Get Synchronous DB Session
def get_sync_db():
    """Provides a synchronous database session."""
    db = SyncSessionLocal()
    try:
        yield db
    finally:
        db.close()

# ✅ Get Asynchronous DB Session
@asynccontextmanager
async def get_async_db():
    async with AsyncSessionLocal() as db:
        try:
            yield db  # ✅ Correctly yield AsyncSession
            await db.commit()
        except Exception as e:
            await db.rollback()
            raise e

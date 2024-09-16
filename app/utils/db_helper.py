from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

# Database connection URL
DATABASE_URL = "postgresql+asyncpg://postgres:abb442003@localhost/superjoin_db"

# Create an async engine for handling database connections
engine = create_async_engine(DATABASE_URL, echo=True)

# Create a session maker to manage sessions with the async engine
SessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

# Base class for all models
Base = declarative_base()

# Model definition
class DataModel(Base):
    __tablename__ = "data"
    
    field_1 = Column(String, primary_key=True)  # First field (used as a unique identifier)
    field_2 = Column(String)  # Second field
    last_synced = Column(DateTime, default=func.now(), onupdate=func.now())  # Last sync timestamp, auto-updated on changes

async def init_db():
    """Initialize the database and create tables if they don't exist."""
    async with engine.begin() as conn:
        # Create all tables defined by the models
        await conn.run_sync(Base.metadata.create_all)

async def get_db_session():
    """Yield a new database session for every request."""
    async with SessionLocal() as session:
        yield session

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String

DATABASE_URL = "postgresql+asyncpg://postgres:abb442003@localhost/superjoin_db"


engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

class DataModel(Base):
    __tablename__ = "data_table"
    
    id = Column(Integer, primary_key=True, index=True)
    field_1 = Column(String, index=True)
    field_2 = Column(String, index=True)

async def init_db():
    """Initialize the database and create tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_db_session():
    """Provide a database session."""
    async with SessionLocal() as session:
        yield session

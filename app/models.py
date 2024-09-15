from sqlalchemy import Column, Integer, String
from app.utils.db_helper import Base

class DataModel(Base):
    __tablename__ = "data_table"

    id = Column(Integer, primary_key=True, index=True)
    field_1 = Column(String, index=True)
    field_2 = Column(String, index=True)
from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Case(Base):
    __tablename__ = 'cases'

    id = Column(Integer, primary_key=True)
    case_name = Column(String)
    notice_start_date = Column(DateTime)
    notice_end_date = Column(DateTime)
    source_url = Column(String)
    region = Column(String)  # Add region column
    attachment_path = Column(String)
    created_at = Column(DateTime, default=datetime.now)
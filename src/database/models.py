from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class FXRate(Base):
    __tablename__ = "fx_rates"

    id = Column(Integer, primary_key=True)
    provider = Column(String(50))
    base_currency = Column(String(10))
    target_currency = Column(String(10))
    exchange_rate = Column(Numeric(18,8))
    fetched_at = Column(TIMESTAMP)
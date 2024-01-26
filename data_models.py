from database import Base
from sqlalchemy import (
    Column, String, Date, DateTime, Integer, Float,
    ForeignKey,
)


class IdTable(Base):
    __tablename__ = 'execution_id'
    
    id = Column(Integer, primary_key=True, unique=True, autoincrement=True, index=True)
    execution_timestamp = Column(DateTime(), unique=True)
    delayed_timestamp = Column(DateTime())
    raw_data = Column(String())
    mongodb_id = Column(String())
    
class GammaFlip(Base):
    __tablename__ = 'gamma_flip'
    
    id = Column(Integer, ForeignKey('execution_id.id'), primary_key=True, index=True)
    date = Column(Date())
    gamma_flip = Column(Float())
    # dealer_cluster .....
    
class OHLCPrices(Base):
    __tablename__ = 'ohlc_prices'
    
    date = Column(Date(), primary_key=True, unique=True)
    timestamp = Column(DateTime(), ForeignKey('execution_id.execution_timestamp'), index=True)
    open = Column(Float())
    high = Column(Float())
    low = Column(Float())
    close = Column(Float())
    volume = Column(Float())
    
class GexLevels(Base):
    __tablename__ = 'gex_levels'
    
    id = Column(Integer, ForeignKey('execution_id.id'), primary_key=True, index=True)
    strikes = Column(Float(), primary_key=True, index=True)
    gex = Column(Float())
    gex_ex_next_exp = Column(Float())
    gex_ex_next_fri = Column(Float())
    
    
    
# MISSING MONGODB MODELS!
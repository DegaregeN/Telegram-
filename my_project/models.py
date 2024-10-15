
# models.py
from sqlalchemy import Column, Integer, String
from .database import Base

class Item(Base):
    doctorset_raw = 'items'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    description = Column(String, index=True)
    price = Column(Integer)



from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class BTRUser(Base):
    __tablename__ = "btr_users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True)
    email = Column(String(50), unique=True, index=True)
    hashed_password = Column(String(200))
    is_active = Column(Integer, default=True)


class BTRTable(Base):
    __tablename__ = "btr_tables"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, index=True)
    display_name = Column(String(50), unique=True, index=True)
    owner = Column(Integer, index=True, default=0) # 0:root
    column_list = Column(String(500), index=True) 
    deleted = Column(Integer, default=0) # 0: not deleted, 1: deleted
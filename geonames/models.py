from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Geoname(Base):
    __tablename__ = "geonames"
    id = Column(Integer, primary_key=True, autoincrement=True)
    country_code = Column(String, index=True)
    postal_code = Column(String, index=True)
    place_name = Column(String)
    admin_name1 = Column(String)
    admin_code1 = Column(String)
    admin_name2 = Column(String)
    admin_code2 = Column(String)
    admin_name3 = Column(String)
    admin_code3 = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    accuracy = Column(Integer)

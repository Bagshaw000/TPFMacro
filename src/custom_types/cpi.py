from datetime import datetime

from pydantic import BaseModel
from sqlmodel import Field, SQLModel, UniqueConstraint


class CPIType(SQLModel, table=True):
    __tablename__ = "cpi"
    __table_args__ = (
    UniqueConstraint('report_date', 'country_code', 
                        name='cpi_unique'),
    )
    
    id:int | None = Field( primary_key=True, default=None)
    country_code:str
    freq: str
    report_date: datetime
    index_value: float
    
class PPIType(SQLModel, table=True):
    __tablename__ = "ppi"
    __table_args__ = (
    UniqueConstraint('report_date', 'country_code', 
                        name='ppi_unique'),
    )
    
    id:int | None = Field( primary_key=True, default=None)
    country_code:str
    freq: str
    report_date: datetime
    index_value: float
    
    
class GDPType(SQLModel, table=True):
    __tablename__ = "gdp"
    __table_args__ = (
        UniqueConstraint(
            'report_date', 'country_code',
              name='gdp_unique'
        ),
    )
    id:int | None = Field( primary_key=True, default=None)
    country_code:str
    freq: str
    report_date: datetime
    index_value: float|None 
    forecast_value: float|None = Field(  default=None)
    

class UNEMPType(SQLModel, table=True):
    __tablename__ = "unemp"
    __table_args__ = (
        UniqueConstraint(
            'report_date', 'country_code',
              name='unemp_unique'
        ),
    )
    id:int | None = Field( primary_key=True, default=None)
    country_code:str
    freq: str
    report_date: datetime
    index_value: float|None 
    forecast_value: float|None = Field(  default=None)
    
class EconomicEventType(BaseModel):
    event:str
    country_code: str
    event_time: datetime | str
    last_value: float | str | None
    expiration: int | None = None
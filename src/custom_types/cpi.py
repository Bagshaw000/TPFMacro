from datetime import datetime

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
    
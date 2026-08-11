from datetime import datetime

from pydantic import BaseModel
from sqlmodel import Field, SQLModel, UniqueConstraint
countries = ['USA','CAN','JPN','DEU','GBR','AUS','IND','CHN','KOR','BRA','FRA']

# Comprehensive mapping dictionary
country_mapping= {
    # North America
    'US': 'USA',
    'CA': 'CAN',
    'MX': 'MEX',
    
    # Europe
    'DE': 'DEU',
    'FR': 'FRA',
    'UK': 'GBR',
    'GB': 'GBR',  # Alternative for UK
    'IT': 'ITA',
    'ES': 'ESP',
    'NL': 'NLD',
    'BE': 'BEL',
    'CH': 'CHE',
    'SE': 'SWE',
    'NO': 'NOR',
    'DK': 'DNK',
    'FI': 'FIN',
    'PL': 'POL',
    'RU': 'RUS',
    
    # Asia
    'JP': 'JPN',
    'CN': 'CHN',
    'IN': 'IND',
    'KR': 'KOR',
    'HK': 'HKG',
    'TW': 'TWN',
    'SG': 'SGP',
    'MY': 'MYS',
    'ID': 'IDN',
    'TH': 'THA',
    'VN': 'VNM',
    'PH': 'PHL',
    'PK': 'PAK',
    'BD': 'BGD',
    'TR': 'TUR',
    'IL': 'ISR',
    'SA': 'SAU',
    'AE': 'ARE',
    
    # Oceania
    'AU': 'AUS',
    'NZ': 'NZL',
    
    # South America
    'BR': 'BRA',
    'AR': 'ARG',
    'CL': 'CHL',
    'CO': 'COL',
    'PE': 'PER',
    'VE': 'VEN',
    
    # Africa
    'ZA': 'ZAF',
    'EG': 'EGY',
    'NG': 'NGA',
    'KE': 'KEN',
    'MA': 'MAR',
}

def map_country_codes(country_codes, mapping=country_mapping):
    """Map 2-letter to 3-letter country codes"""
    return [mapping.get(code.upper(), code) for code in country_codes]

# Usage
# country_2letter = ["US", "CA", "JP", "DE", "UK", "AU", "IN", "CN", "KR", "BR", "FR"]
# country_3letter = map_country_codes(country_2letter)

# print(country_3letter)
# # ['USA', 'CAN', 'JPN', 'DEU', 'GBR', 'AUS', 'IND', 'CHN', 'KOR', 'BRA', 'FRA']
# countries = ['USA','CAN','JPN','DEU','GBR','AUS','IND','CHN','KOR','BRA','FRA']
class CPIType(SQLModel, table=True):
    __tablename__ = "cpi"
    __table_args__ = (
    UniqueConstraint('report_date', 'country_code', 
                        name='cpi_unique'),
    )
    
    id:int | None = Field( primary_key=True, default=None)
    country_code:str
    freq: str|None = Field( primary_key=True, default=None)
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
    freq: str|None = Field( primary_key=True, default=None)
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
    freq:str|None = Field( primary_key=True, default=None)
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
    freq: str|None = Field( primary_key=True, default=None)
    report_date: datetime
    index_value: float|None 
    forecast_value: float|None = Field(  default=None)
    
class EconomicEventType(BaseModel):
    event:str
    country_code: str
    event_time: datetime | str
    last_value: float | str | None
    expiration: int | None = None
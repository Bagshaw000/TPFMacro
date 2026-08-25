from datetime import datetime
from typing import Any, Literal, Optional, overload

from pydantic import BaseModel
from sqlmodel import Field, SQLModel, UniqueConstraint
months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
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
    'GB': 'GBR',
    'UK': 'GBR',  # Alternative alias for GB, ISO 3166-1 alpha-2 is GB
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

def get_country_code(country_codes, mapping:dict=country_mapping):
    
    key = next((k for k, v in mapping.items() if v == country_codes), None)
    
    return key

def get_key(values, mapping:dict):
    key = next((k for k, v in mapping.items() if v == values), None)
   
    return key

class _EconIndicatorBase(SQLModel):
    id: int | None = Field(primary_key=True, default=None)
    country_code: str
    freq: str | None = Field(default=None)
    report_date: datetime


class _EconIndicatorNoForecast(_EconIndicatorBase):
    index_value: float


class _EconIndicatorWithForecast(_EconIndicatorBase):
    index_value: float | None
    forecast_value: float | None = Field(default=None)
    lse_forecast: float | None = Field(default=None)


@overload
def make_econ_indicator_type(
    tablename: str,
    class_name: str | None = None,
    constraint_name: str | None = None,
    with_forecast: Literal[True] = True,
) -> type[_EconIndicatorWithForecast]: ...
@overload
def make_econ_indicator_type(
    tablename: str,
    class_name: str | None = None,
    constraint_name: str | None = None,
    *,
    with_forecast: Literal[False],
) -> type[_EconIndicatorNoForecast]: ...
def make_econ_indicator_type(
    tablename: str,
    class_name: str | None = None,
    constraint_name: str | None = None,
    with_forecast: bool = True,
) -> type[SQLModel]:
    """Build a SQLModel table class for an economic indicator, with the
    table name (and unique-constraint name) supplied as parameters instead
    of being hardcoded per class."""
    class_name = class_name or f"{tablename.upper()}Type"
    constraint_name = constraint_name or f"{tablename}_unique"
    base = _EconIndicatorWithForecast if with_forecast else _EconIndicatorNoForecast

    namespace = {
        "__tablename__": tablename,
        "__table_args__": (
            UniqueConstraint("report_date", "country_code", name=constraint_name),
        ),
        "__module__": __name__,
    }
    metaclass: Any = type(base)
    return metaclass(class_name, (base,), namespace, table=True)


CPIType = make_econ_indicator_type("cpi")
PPIType = make_econ_indicator_type("ppi")
GDPType = make_econ_indicator_type("gdp")
UNEMPType = make_econ_indicator_type("unemp")
INFType = make_econ_indicator_type("inflation")
RETAILType = make_econ_indicator_type("retail")
LSEType = make_econ_indicator_type("lse")

ECON_INDICATOR_TYPES: dict[str, type[_EconIndicatorWithForecast]] = {
    str(t.__tablename__): t
    for t in [CPIType, PPIType, GDPType, UNEMPType, INFType, RETAILType, LSEType]
}


class EconomicEventType(BaseModel):
    event:str
    country_code: str
    event_time: datetime | str
    last_value: float | str | None
    expiration: int | None = None
    
    
class LSEResponseType(BaseModel):
    id: int
    date: str
    time: str
    datetime: str
    region_code: str
    event: str
    period_hint: Optional[str] = None
    actual: Optional[str] = None
    previous: Optional[str] = None
    consensus: Optional[str] = None
    forecast: Optional[str] = None
    actual_revised: int 
    previous_revised: int
    consensus_revised: int 
    forecast_revised: int 
    created_at: str
    updated_at: str

    
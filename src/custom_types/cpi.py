"""Shared types and lookups for the macro-indicator pipeline.

Holds:
  - `months`, `countries` : the fixed universe - 12 month abbreviations and the
    11 tracked countries as 3-letter codes.
  - `country_mapping` / `map_country_codes` : 2-letter (provider) -> 3-letter
    (internal) country-code translation.
  - `make_econ_indicator_type()` : factory that builds the SQLModel table class
    for one LSE indicator (cpi / ppi / unemp / inflation / retail), all sharing
    a `(country_code, period)` unique constraint. `ECON_INDICATOR_TYPES` is the
    registry of the built classes.
  - `EconomicEventType`, other small enums/models used across controllers.

`period` is the "YYYY-MM" reference month - see sql/add_period_column.sql.
"""

from datetime import datetime
from typing import Any, Literal, Optional, overload

from pydantic import BaseModel
from sqlmodel import Field, SQLModel, UniqueConstraint
months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
countries = ['USA','CAN','JPN','DEU','GBR','AUS','IND','CHN','KOR','BRA','FRA']

# 2-letter provider code -> 3-letter internal code. Includes aliases
# (e.g. both 'GB' and 'UK' -> 'GBR'); superset of `countries`.
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
    # Actual publication/release date of the reading.
    report_date: datetime
    # Reference month the reading describes, as "YYYY-MM" (derived in
    # controller/lse_.py's process_event from period_hint + the release year).
    # This - not report_date - is a reading's real identity: a flash estimate,
    # the final print and any later revision for the same month all share a
    # period and collapse to one row (see the UniqueConstraint below).
    # Optional at the model level only so a stale `{table}_recent` view can't
    # break model_validate; the DB column is NOT NULL.
    period: str | None = Field(default=None)


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
    # One reading per (country, reference month). Keyed on `period`, not
    # `report_date`, so re-releases of the same month (flash -> final ->
    # revision) upsert onto one row instead of piling up.
    constraint_name = constraint_name or f"{tablename}_country_period_unique"
    base = _EconIndicatorWithForecast if with_forecast else _EconIndicatorNoForecast

    namespace = {
        "__tablename__": tablename,
        "__table_args__": (
            UniqueConstraint("country_code", "period", name=constraint_name),
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

    
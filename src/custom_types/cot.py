from datetime import datetime

from pydantic import BaseModel,  field_validator,ConfigDict
from typing import Any, Optional
import pandas as pd
from sqlmodel import  SQLModel, Field, Table, UniqueConstraint

class CFTCData(BaseModel):
    """CFTC Commitments of Traders Report Data Model matching database schema"""
    
    # Identification fields
    Market_and_Exchange_Names: Optional[str] = None
    As_of_Date_In_Form_YYMMDD: Optional[int] = None  # bigint
    Report_Date_as_YYYY_MM_DD: Optional[str] = None  # text
    CFTC_Contract_Market_Code: Optional[str] = None
    CFTC_Market_Code: Optional[str] = None
    CFTC_Region_Code: Optional[int] = None  # bigint
    CFTC_Commodity_Code: Optional[int] = None  # bigint
    
    # Open Interest
    Open_Interest_All: Optional[int] = None  # bigint
    
    # Dealer Positions
    Dealer_Positions_Long_All: Optional[int] = None  # bigint
    Dealer_Positions_Short_All: Optional[int] = None  # bigint
    Dealer_Positions_Spread_All: Optional[int] = None  # bigint
    
    # Asset Manager Positions
    Asset_Mgr_Positions_Long_All: Optional[int] = None  # bigint
    Asset_Mgr_Positions_Short_All: Optional[int] = None  # bigint
    Asset_Mgr_Positions_Spread_All: Optional[int] = None  # bigint
    
    # Leveraged Money Positions
    Lev_Money_Positions_Long_All: Optional[int] = None  # bigint
    Lev_Money_Positions_Short_All: Optional[int] = None  # bigint
    Lev_Money_Positions_Spread_All: Optional[int] = None  # bigint
    
    # Other Reportable Positions
    Other_Rept_Positions_Long_All: Optional[int] = None  # bigint
    Other_Rept_Positions_Short_All: Optional[int] = None  # bigint
    Other_Rept_Positions_Spread_All: Optional[int] = None  # bigint
    
    # Total Reportable Positions
    Tot_Rept_Positions_Long_All: Optional[int] = None  # bigint
    Tot_Rept_Positions_Short_All: Optional[int] = None  # bigint
    
    # Non-Reportable Positions
    NonRept_Positions_Long_All: Optional[int] = None  # bigint
    NonRept_Positions_Short_All: Optional[int] = None  # bigint
    
    # Changes (all text in database)
    Change_in_Open_Interest_All: Optional[float] = None
    Change_in_Dealer_Long_All: Optional[float] = None
    Change_in_Dealer_Short_All: Optional[float] = None
    Change_in_Dealer_Spread_All: Optional[float] = None
    Change_in_Asset_Mgr_Long_All: Optional[float] = None
    Change_in_Asset_Mgr_Short_All: Optional[float] = None
    Change_in_Asset_Mgr_Spread_All: Optional[float] = None
    Change_in_Lev_Money_Long_All: Optional[float] = None
    Change_in_Lev_Money_Short_All: Optional[float] = None
    Change_in_Lev_Money_Spread_All: Optional[float] = None
    Change_in_Other_Rept_Long_All: Optional[float] = None
    Change_in_Other_Rept_Short_All: Optional[float] = None
    Change_in_Other_Rept_Spread_All: Optional[float] = None
    Change_in_Tot_Rept_Long_All: Optional[float] = None
    Change_in_Tot_Rept_Short_All: Optional[float] = None
    Change_in_NonRept_Long_All: Optional[float] = None
    Change_in_NonRept_Short_All: Optional[float] = None
    
    # Percent of Open Interest
    Pct_of_Open_Interest_All: Optional[int] = None  # bigint
    Pct_of_OI_Dealer_Long_All: Optional[float] = None  # double precision
    Pct_of_OI_Dealer_Short_All: Optional[float] = None
    Pct_of_OI_Dealer_Spread_All: Optional[float] = None
    Pct_of_OI_Asset_Mgr_Long_All: Optional[float] = None
    Pct_of_OI_Asset_Mgr_Short_All: Optional[float] = None
    Pct_of_OI_Asset_Mgr_Spread_All: Optional[float] = None
    Pct_of_OI_Lev_Money_Long_All: Optional[float] = None
    Pct_of_OI_Lev_Money_Short_All: Optional[float] = None
    Pct_of_OI_Lev_Money_Spread_All: Optional[float] = None
    Pct_of_OI_Other_Rept_Long_All: Optional[float] = None
    Pct_of_OI_Other_Rept_Short_All: Optional[float] = None
    Pct_of_OI_Other_Rept_Spread_All: Optional[float] = None
    Pct_of_OI_Tot_Rept_Long_All: Optional[float] = None
    Pct_of_OI_Tot_Rept_Short_All: Optional[float] = None
    Pct_of_OI_NonRept_Long_All: Optional[float] = None
    Pct_of_OI_NonRept_Short_All: Optional[float] = None
    
    # Traders Count
    Traders_Tot_All: Optional[int] = None  # bigint
    Traders_Dealer_Long_All: Optional[float] = None  # text
    Traders_Dealer_Short_All: Optional[float] = None
    Traders_Dealer_Spread_All: Optional[float] = None
    Traders_Asset_Mgr_Long_All: Optional[float] = None
    Traders_Asset_Mgr_Short_All: Optional[float] = None
    Traders_Asset_Mgr_Spread_All: Optional[float] = None
    Traders_Lev_Money_Long_All: Optional[float] = None
    Traders_Lev_Money_Short_All: Optional[float] = None
    Traders_Lev_Money_Spread_All: Optional[float] = None
    Traders_Other_Rept_Long_All: Optional[float] = None
    Traders_Other_Rept_Short_All: Optional[float] = None
    Traders_Other_Rept_Spread_All: Optional[float] = None
    Traders_Tot_Rept_Long_All: Optional[int] = None  # bigint
    Traders_Tot_Rept_Short_All: Optional[int] = None  # bigint
    
    # Concentration (Gross)
    Conc_Gross_LE_4_TDR_Long_All: Optional[float] = None  # double precision
    Conc_Gross_LE_4_TDR_Short_All: Optional[float] = None
    Conc_Gross_LE_8_TDR_Long_All: Optional[float] = None
    Conc_Gross_LE_8_TDR_Short_All: Optional[float] = None
    
    # Concentration (Net)
    Conc_Net_LE_4_TDR_Long_All: Optional[float] = None  # double precision
    Conc_Net_LE_4_TDR_Short_All: Optional[float] = None
    Conc_Net_LE_8_TDR_Long_All: Optional[float] = None
    Conc_Net_LE_8_TDR_Short_All: Optional[float] = None
    
    # Additional fields
    Contract_Units: Optional[str] = None
    CFTC_Contract_Market_Code_Quotes: Optional[str] = None
    CFTC_Market_Code_Quotes: Optional[str] = None
    CFTC_Commodity_Code_Quotes: Optional[int] = None  # bigint
    CFTC_SubGroup_Code: Optional[str] = None
    FutOnly_or_Combined: Optional[str] = None
    
    # Primary key and additional required fields
    id: Optional[int] = None  # bigint not null
    Market: str  # text not null
    model_config = ConfigDict(from_attributes=True)
    @field_validator('Change_in_Open_Interest_All', 'Change_in_Dealer_Long_All', 
                     'Change_in_Dealer_Short_All', 'Change_in_Dealer_Spread_All',
                     'Change_in_Asset_Mgr_Long_All', 'Change_in_Asset_Mgr_Short_All',
                     'Change_in_Asset_Mgr_Spread_All', 'Change_in_Lev_Money_Long_All',
                     'Change_in_Lev_Money_Short_All', 'Change_in_Lev_Money_Spread_All',
                     'Change_in_Other_Rept_Long_All', 'Change_in_Other_Rept_Short_All',
                     'Change_in_Other_Rept_Spread_All', 'Change_in_Tot_Rept_Long_All',
                     'Change_in_Tot_Rept_Short_All', 'Change_in_NonRept_Long_All',
                     'Change_in_NonRept_Short_All',
                     'Traders_Dealer_Long_All', 'Traders_Dealer_Short_All',
                     'Traders_Dealer_Spread_All', 'Traders_Asset_Mgr_Long_All',
                     'Traders_Asset_Mgr_Short_All', 'Traders_Asset_Mgr_Spread_All',
                     'Traders_Lev_Money_Long_All', 'Traders_Lev_Money_Short_All',
                     'Traders_Lev_Money_Spread_All', 'Traders_Other_Rept_Long_All',
                     'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All',
                     mode='before')
    @classmethod
    def parse_dot_as_none(cls, v: Any) -> Any:
        return 0 if v == '.' else v



class CotData(SQLModel, table=True):
    # Composite Primary Key
    __tablename__= "cot_ttf"
    __table_args__ = (
        UniqueConstraint('market_and_exchange_names', 'report_date_as_yyyy_mm_dd', 
                        name='cot_ttf_market_exchange_date_key'),
    )
    
    id: int | None = Field(default=None, primary_key=True)
    market_and_exchange_names: str | None = Field(
        default=None
    )
    report_date_as_yyyy_mm_dd: str | None = Field(
        default=None
    )
    
    # Market Identifiers
    as_of_date_in_form_yymmdd: int | None = Field(default=None)
    cftc_contract_market_code: str | None = Field(default=None)
    cftc_market_code: str | None = Field(default=None)
    cftc_region_code: int | None = Field(default=None)
    cftc_commodity_code: int | None = Field(default=None)
    
    # Open Interest
    open_interest_all: int | None = Field(default=None)
    
    # Dealer Positions
    dealer_positions_long_all: int | None = Field(default=None)
    dealer_positions_short_all: int | None = Field(default=None)
    dealer_positions_spread_all: int | None = Field(default=None)
    
    # Asset Manager Positions
    asset_mgr_positions_long_all: int | None = Field(default=None)
    asset_mgr_positions_short_all: int | None = Field(default=None)
    asset_mgr_positions_spread_all: int | None = Field(default=None)
    
    # Leveraged Money Positions
    lev_money_positions_long_all: int | None = Field(default=None)
    lev_money_positions_short_all: int | None = Field(default=None)
    lev_money_positions_spread_all: int | None = Field(default=None)
    
    # Other Reportable Positions
    other_rept_positions_long_all: int | None = Field(default=None)
    other_rept_positions_short_all: int | None = Field(default=None)
    other_rept_positions_spread_all: int | None = Field(default=None)
    
    # Total Reportable Positions
    tot_rept_positions_long_all: int | None = Field(default=None)
    tot_rept_positions_short_all: int | None = Field(default=None)
    
    # Non-Reportable Positions
    nonrept_positions_long_all: int | None = Field(default=None)
    nonrept_positions_short_all: int | None = Field(default=None)
    
    # Changes
    change_in_open_interest_all: float | None = Field(default=None)
    change_in_dealer_long_all: float | None = Field(default=None)
    change_in_dealer_short_all: float | None = Field(default=None)
    change_in_dealer_spread_all: float | None = Field(default=None)
    change_in_asset_mgr_long_all: float | None = Field(default=None)
    change_in_asset_mgr_short_all: float | None = Field(default=None)
    change_in_asset_mgr_spread_all: float | None = Field(default=None)
    change_in_lev_money_long_all: float | None = Field(default=None)
    change_in_lev_money_short_all: float | None = Field(default=None)
    change_in_lev_money_spread_all: float | None = Field(default=None)
    change_in_other_rept_long_all: float | None = Field(default=None)
    change_in_other_rept_short_all: float | None = Field(default=None)
    change_in_other_rept_spread_all: float | None = Field(default=None)
    change_in_tot_rept_long_all: float | None = Field(default=None)
    change_in_tot_rept_short_all: float | None = Field(default=None)
    change_in_nonrept_long_all: float | None = Field(default=None)
    change_in_nonrept_short_all: float | None = Field(default=None)
    
    # Percent of Open Interest
    pct_of_open_interest_all: int | None = Field(default=None)
    pct_of_oi_dealer_long_all: float | None = Field(default=None)
    pct_of_oi_dealer_short_all: float | None = Field(default=None)
    pct_of_oi_dealer_spread_all: float | None = Field(default=None)
    pct_of_oi_asset_mgr_long_all: float | None = Field(default=None)
    pct_of_oi_asset_mgr_short_all: float | None = Field(default=None)
    pct_of_oi_asset_mgr_spread_all: float | None = Field(default=None)
    pct_of_oi_lev_money_long_all: float | None = Field(default=None)
    pct_of_oi_lev_money_short_all: float | None = Field(default=None)
    pct_of_oi_lev_money_spread_all: float | None = Field(default=None)
    pct_of_oi_other_rept_long_all: float | None = Field(default=None)
    pct_of_oi_other_rept_short_all: float | None = Field(default=None)
    pct_of_oi_other_rept_spread_all: float | None = Field(default=None)
    pct_of_oi_tot_rept_long_all: float | None = Field(default=None)
    pct_of_oi_tot_rept_short_all: float | None = Field(default=None)
    pct_of_oi_nonrept_long_all: float | None = Field(default=None)
    pct_of_oi_nonrept_short_all: float | None = Field(default=None)
    
    # Traders Count
    traders_tot_all: int | None = Field(default=None)
    traders_dealer_long_all: float | None = Field(default=None)
    traders_dealer_short_all: float | None = Field(default=None)
    traders_dealer_spread_all: float | None = Field(default=None)
    traders_asset_mgr_long_all: float | None = Field(default=None)
    traders_asset_mgr_short_all: float | None = Field(default=None)
    traders_asset_mgr_spread_all: float | None = Field(default=None)
    traders_lev_money_long_all: float | None = Field(default=None)
    traders_lev_money_short_all: float | None = Field(default=None)
    traders_lev_money_spread_all: float | None = Field(default=None)
    traders_other_rept_long_all: float | None = Field(default=None)
    traders_other_rept_short_all: float | None = Field(default=None)
    traders_other_rept_spread_all: float | None = Field(default=None)
    traders_tot_rept_long_all: int | None = Field(default=None)
    traders_tot_rept_short_all: int | None = Field(default=None)
    
    # Concentration (Gross)
    conc_gross_le_4_tdr_long_all: float | None = Field(default=None)
    conc_gross_le_4_tdr_short_all: float | None = Field(default=None)
    conc_gross_le_8_tdr_long_all: float | None = Field(default=None)
    conc_gross_le_8_tdr_short_all: float | None = Field(default=None)
    
    # Concentration (Net)
    conc_net_le_4_tdr_long_all: float | None = Field(default=None)
    conc_net_le_4_tdr_short_all: float | None = Field(default=None)
    conc_net_le_8_tdr_long_all: float | None = Field(default=None)
    conc_net_le_8_tdr_short_all: float | None = Field(default=None)
    
    # Additional Fields
    contract_units: str | None = Field(default=None)
    cftc_contract_market_code_quotes: str | None = Field(default=None)
    cftc_market_code_quotes: str | None = Field(default=None)
    cftc_commodity_code_quotes: int | None = Field(default=None)
    cftc_subgroup_code: str | None = Field(default=None)
    futonly_or_combined: str | None = Field(default=None)
    
    # Metadata
    market: str | None = Field(default=None)
    
    # @property
    # def report_date_as_yyyy_mm_dd(self) -> str:
    #     """Return date as formatted string"""
    #     return self.date.strftime('%Y-%m-%d') if self.date else None
    

    
from datetime import datetime

from pydantic import BaseModel,  field_validator,ConfigDict
from typing import Any, Optional
import pandas as pd
from sqlmodel import  DateTime, SQLModel, Field, Table, UniqueConstraint



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
    report_date_as_yyyy_mm_dd: datetime | None = Field(
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
    non_rept_positions_long_all: int | None = Field(default=None)
    non_rept_positions_short_all: int | None = Field(default=None)
    
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
    change_in_non_rept_long_all: float | None = Field(default=None)
    change_in_non_rept_short_all: float | None = Field(default=None)
    
    # Percent of Open Interest
    pct_of_open_interest_all: float | None = Field(default=None)
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
    pct_of_oi_non_rept_long_all: float | None = Field(default=None)
    pct_of_oi_non_rept_short_all: float | None = Field(default=None)
    
    # Traders Count
    traders_tot_all: float | None = Field(default=None)
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
    traders_tot_rept_long_all: float | None = Field(default=None)
    traders_tot_rept_short_all: float | None = Field(default=None)
    
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
    cftc_sub_group_code: str | None = Field(default=None)
    fut_only_or_combined: str | None = Field(default=None)
    
    # Metadata
    market: str | None = Field(default=None)
    # @field_validator('report_date_as_yyyy_mm_dd', mode='before')
    # @classmethod
    # def convert_datetime_to_str(cls, v):
    #     if isinstance(v, datetime):
    #         return v.strftime('%Y-%m-%d')
    #     return v
    
COT_COLUMN_MAPPING = {
    # Date fields
    'As_of_Date_In_Form_YYMMDD': 'as_of_date_in_form_yymmdd',
    'Report_Date_as_YYYY-MM-DD': 'report_date_as_yyyy_mm_dd',
    
    # Market identifiers
    'CFTC_Contract_Market_Code': 'cftc_contract_market_code',
    'CFTC_Market_Code': 'cftc_market_code',
    'CFTC_Region_Code': 'cftc_region_code',
    'CFTC_Commodity_Code': 'cftc_commodity_code',
    'Market_and_Exchange_Names': 'market_and_exchange_names',
    
    # Open Interest
    'Open_Interest_All': 'open_interest_all',
    
    # Dealer Positions
    'Dealer_Positions_Long_All': 'dealer_positions_long_all',
    'Dealer_Positions_Short_All': 'dealer_positions_short_all',
    'Dealer_Positions_Spread_All': 'dealer_positions_spread_all',
    
    # Asset Manager Positions
    'Asset_Mgr_Positions_Long_All': 'asset_mgr_positions_long_all',
    'Asset_Mgr_Positions_Short_All': 'asset_mgr_positions_short_all',
    'Asset_Mgr_Positions_Spread_All': 'asset_mgr_positions_spread_all',
    
    # Leveraged Money Positions
    'Lev_Money_Positions_Long_All': 'lev_money_positions_long_all',
    'Lev_Money_Positions_Short_All': 'lev_money_positions_short_all',
    'Lev_Money_Positions_Spread_All': 'lev_money_positions_spread_all',
    
    # Other Reportable Positions
    'Other_Rept_Positions_Long_All': 'other_rept_positions_long_all',
    'Other_Rept_Positions_Short_All': 'other_rept_positions_short_all',
    'Other_Rept_Positions_Spread_All': 'other_rept_positions_spread_all',
    
    # Total Reportable Positions
    'Tot_Rept_Positions_Long_All': 'tot_rept_positions_long_all',
    'Tot_Rept_Positions_Short_All': 'tot_rept_positions_short_all',
    
    # Non-Reportable Positions
    'NonRept_Positions_Long_All': 'non_rept_positions_long_all',
    'NonRept_Positions_Short_All': 'non_rept_positions_short_all',
    
    # Changes
    'Change_in_Open_Interest_All': 'change_in_open_interest_all',
    'Change_in_Dealer_Long_All': 'change_in_dealer_long_all',
    'Change_in_Dealer_Short_All': 'change_in_dealer_short_all',
    'Change_in_Dealer_Spread_All': 'change_in_dealer_spread_all',
    'Change_in_Asset_Mgr_Long_All': 'change_in_asset_mgr_long_all',
    'Change_in_Asset_Mgr_Short_All': 'change_in_asset_mgr_short_all',
    'Change_in_Asset_Mgr_Spread_All': 'change_in_asset_mgr_spread_all',
    'Change_in_Lev_Money_Long_All': 'change_in_lev_money_long_all',
    'Change_in_Lev_Money_Short_All': 'change_in_lev_money_short_all',
    'Change_in_Lev_Money_Spread_All': 'change_in_lev_money_spread_all',
    'Change_in_Other_Rept_Long_All': 'change_in_other_rept_long_all',
    'Change_in_Other_Rept_Short_All': 'change_in_other_rept_short_all',
    'Change_in_Other_Rept_Spread_All': 'change_in_other_rept_spread_all',
    'Change_in_Tot_Rept_Long_All': 'change_in_tot_rept_long_all',
    'Change_in_Tot_Rept_Short_All': 'change_in_tot_rept_short_all',
    'Change_in_NonRept_Long_All': 'change_in_non_rept_long_all',
    'Change_in_NonRept_Short_All': 'change_in_non_rept_short_all',
    
    # Percent of Open Interest
    'Pct_of_Open_Interest_All': 'pct_of_open_interest_all',
    'Pct_of_OI_Dealer_Long_All': 'pct_of_oi_dealer_long_all',
    'Pct_of_OI_Dealer_Short_All': 'pct_of_oi_dealer_short_all',
    'Pct_of_OI_Dealer_Spread_All': 'pct_of_oi_dealer_spread_all',
    'Pct_of_OI_Asset_Mgr_Long_All': 'pct_of_oi_asset_mgr_long_all',
    'Pct_of_OI_Asset_Mgr_Short_All': 'pct_of_oi_asset_mgr_short_all',
    'Pct_of_OI_Asset_Mgr_Spread_All': 'pct_of_oi_asset_mgr_spread_all',
    'Pct_of_OI_Lev_Money_Long_All': 'pct_of_oi_lev_money_long_all',
    'Pct_of_OI_Lev_Money_Short_All': 'pct_of_oi_lev_money_short_all',
    'Pct_of_OI_Lev_Money_Spread_All': 'pct_of_oi_lev_money_spread_all',
    'Pct_of_OI_Other_Rept_Long_All': 'pct_of_oi_other_rept_long_all',
    'Pct_of_OI_Other_Rept_Short_All': 'pct_of_oi_other_rept_short_all',
    'Pct_of_OI_Other_Rept_Spread_All': 'pct_of_oi_other_rept_spread_all',
    'Pct_of_OI_Tot_Rept_Long_All': 'pct_of_oi_tot_rept_long_all',
    'Pct_of_OI_Tot_Rept_Short_All': 'pct_of_oi_tot_rept_short_all',
    'Pct_of_OI_NonRept_Long_All': 'pct_of_oi_non_rept_long_all',
    'Pct_of_OI_NonRept_Short_All': 'pct_of_oi_non_rept_short_all',
    
    # Traders Count
    'Traders_Tot_All': 'traders_tot_all',
    'Traders_Dealer_Long_All': 'traders_dealer_long_all',
    'Traders_Dealer_Short_All': 'traders_dealer_short_all',
    'Traders_Dealer_Spread_All': 'traders_dealer_spread_all',
    'Traders_Asset_Mgr_Long_All': 'traders_asset_mgr_long_all',
    'Traders_Asset_Mgr_Short_All': 'traders_asset_mgr_short_all',
    'Traders_Asset_Mgr_Spread_All': 'traders_asset_mgr_spread_all',
    'Traders_Lev_Money_Long_All': 'traders_lev_money_long_all',
    'Traders_Lev_Money_Short_All': 'traders_lev_money_short_all',
    'Traders_Lev_Money_Spread_All': 'traders_lev_money_spread_all',
    'Traders_Other_Rept_Long_All': 'traders_other_rept_long_all',
    'Traders_Other_Rept_Short_All': 'traders_other_rept_short_all',
    'Traders_Other_Rept_Spread_All': 'traders_other_rept_spread_all',
    'Traders_Tot_Rept_Long_All': 'traders_tot_rept_long_all',
    'Traders_Tot_Rept_Short_All': 'traders_tot_rept_short_all',
    
    # Concentration (Gross)
    'Conc_Gross_LE_4_TDR_Long_All': 'conc_gross_le_4_tdr_long_all',
    'Conc_Gross_LE_4_TDR_Short_All': 'conc_gross_le_4_tdr_short_all',
    'Conc_Gross_LE_8_TDR_Long_All': 'conc_gross_le_8_tdr_long_all',
    'Conc_Gross_LE_8_TDR_Short_All': 'conc_gross_le_8_tdr_short_all',
    
    # Concentration (Net)
    'Conc_Net_LE_4_TDR_Long_All': 'conc_net_le_4_tdr_long_all',
    'Conc_Net_LE_4_TDR_Short_All': 'conc_net_le_4_tdr_short_all',
    'Conc_Net_LE_8_TDR_Long_All': 'conc_net_le_8_tdr_long_all',
    'Conc_Net_LE_8_TDR_Short_All': 'conc_net_le_8_tdr_short_all',
    
    # Additional fields
    'Contract_Units': 'contract_units',
    'CFTC_Contract_Market_Code_Quotes': 'cftc_contract_market_code_quotes',
    'CFTC_Market_Code_Quotes': 'cftc_market_code_quotes',
    'CFTC_Commodity_Code_Quotes': 'cftc_commodity_code_quotes',
    'CFTC_SubGroup_Code': 'cftc_sub_group_code',
    'FutOnly_or_Combined': 'fut_only_or_combined',
}

filtered_country_grouping = {
    # ===== PRIMARY SINGLE-COUNTRY INSTRUMENTS =====

    "USA": [
        # USD Index
        "U.S. DOLLAR INDEX",
        "USD INDEX",
        # US Equity Indices (only those NOT excluded)
        "Russell 2000 Stock Index Future",
        "Russell 2000 Stock Index",
        "E-MINI RUSSELL 2000 INDEX",
        "RUSSELL E-MINI",
        # Russell 2000 (excluded variants removed)
        # DJIA (excluded variants removed)
        "DJIA Consolidated",  # Maps to DOW JONES INDUSTRIAL AVERAGE in your code
        # S&P 500 (excluded variants removed)
        "S&P 500 Consolidated",  # Maps to S&P 500 STOCK INDEX
        "E-MINI S&P 500 STOCK INDEX",
        "E-MINI S&P 500",
        # S&P Sector Indices (all included - none excluded)
        "E-MINI S&P CONSU STAPLES INDEX",
        "E-MINI S&P CONSUMER DISC INDEX",
        "E-MINI S&P ENERGY INDEX",
        "E-MINI S&P FINANCIAL INDEX",
        "E-MINI S&P HEALTH CARE INDEX",
        "E-MINI S&P TECHNOLOGY INDEX",
        "E-MINI S&P UTILITIES INDEX",
        "E-MINI S&P INDUSTRIAL INDEX",
        "E-MINI S&P MATERIALS INDEX",
        "DOW JONES U.S. REAL ESTATE IDX",
        "E-MINI S&P COMMUNICATION INDEX",
        # NASDAQ (excluded variants removed)
        "NASDAQ-100 Consolidated",  # Maps to NASDAQ-100 STOCK INDEX
        "NASDAQ-100 STOCK INDEX (MINI)",
        "NASDAQ MINI",
        # VIX
        "VIX FUTURES",
        # US Treasuries & Rates
        "U.S. TREASURY BONDS",
        "UST BOND",
        "ULTRA U.S. TREASURY BONDS",
        "ULTRA UST BOND",
        "ULTRA US T BOND",
        "LONG-TERM U.S. TREASURY BONDS",
        "10-YEAR U.S. TREASURY NOTES",
        "UST 10Y NOTE",
        "ULTRA 10-YEAR U.S. T-NOTES",
        "ULTRA UST 10Y",
        "5-YEAR U.S. TREASURY NOTES",
        "UST 5Y NOTE",
        "2-YEAR U.S. TREASURY NOTES",
        "UST 2Y NOTE",
        "MICRO 10 YEAR YIELD",
        "ONE-MONTH EURODOLLAR",
        "3-MONTH EURODOLLARS",
        "EURODOLLARS-3M",
        "30-DAY FEDERAL FUNDS",
        "FED FUNDS",
        "3-MONTH SOFR",
        "1-MONTH SOFR",
        "SOFR-3M",
        "SOFR-1M",
        # Cryptocurrencies (only those NOT excluded)
        "BITCOIN-USD",
        "BITCOIN",
      
        "NANO BITCOIN PERP STYLE",
        "BITCOIN CASH PERP STYLE",
        "ETHER CASH SETTLED",
        "LITECOIN CASH",
        "DOGECOIN",
        "DOGECOIN PERP STYLE",
        "POLKADOT",
        "POLKADOT PERP STYLE",
        "CHAINLINK",
        "CHAINLINK PERP STYLE",
        "AVALANCHE",
        "AVALANCHE PERP STYLE",
        "1K SHIB",
        "1K SHIB PERP",
        "STELLAR",
        "NANO STELLAR",
        "CARDONA",
        "CARDONA PERP STYLE",
        "MICRO XRP",
        "NANO XRP",
        "NANO XRP PERP STYLE",
        "XRP",
        "MICRO SOL",
        "NANO SOLANA",
        "NANO SOLANA PERP STYLE",
        "SOL",
        "HEDERA",
        "HEDERA PERP STYLE",
        "SUI PERP STYLE",
        "ONDO PERP STYLE",
        "ZCASH PERP STYLE"
    ],

    "CAN": [
        "CANADIAN DOLLAR"
    ],

    "JPN": [
        "JAPANESE YEN",
        "NIKKEI STOCK AVERAGE",
       
    ],

    "EUR": [
        "EURO FX",
       
    ],

    "GBR": [
        "BRITISH POUND STERLING",
        "BRITISH POUND"
    ],

    "AUS": [
        "AUSTRALIAN DOLLAR"
    ],

    "NZL": [
        "NEW ZEALAND DOLLAR",
        "NZ DOLLAR"
    ],

    "CHE": [
        "SWISS FRANC"
    ],

    "BRA": [
        "BRAZILIAN REAL"
    ],

    "MEX": [
        "MEXICAN PESO"
    ],

    "RUS": [
        "RUSSIAN RUBLE"
    ],

    "ZAF": [
        "SOUTH AFRICAN RAND",
        "SO AFRICAN RAND"
    ],

    "CHN": [
        "CHINESE RENMINBI-HK (CNH)",
        "USD/CHINESE RENMINBI-OFFSHORE"
    ],

    # ===== MULTI-COUNTRY INSTRUMENTS =====
    # Each entry is: [ "Instrument Name", ["Country1", "Country2", ...] ]

    "Multi-Country": [
        # ----- MAJOR CURRENCY PAIRS -----
        ["EURO FX", ["USA", "EUR"]],
        ["BRITISH POUND", ["USA", "GBR"]],
        ["BRITISH POUND STERLING", ["USA", "GBR"]],
        ["AUSTRALIAN DOLLAR", ["USA", "AUS", "CHN"]],
        ["NEW ZEALAND DOLLAR", ["USA", "NZL"]],
        ["CANADIAN DOLLAR", ["USA", "CAN"]],
        ["JAPANESE YEN", ["USA", "JPN"]],
        ["SWISS FRANC", ["USA", "CHE"]],
        ["MEXICAN PESO", ["USA", "MEX"]],
        ["BRAZILIAN REAL", ["USA", "BRA"]],
        ["RUSSIAN RUBLE", ["USA", "RUS"]],
        ["SOUTH AFRICAN RAND", ["USA", "ZAF"]],
        ["SO AFRICAN RAND", ["USA", "ZAF"]],
        ["CHINESE RENMINBI-HK (CNH)", ["USA", "CHN"]],
        ["USD/CHINESE RENMINBI-OFFSHORE", ["USA", "CHN"]],

      ]
}
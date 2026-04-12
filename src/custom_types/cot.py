from datetime import datetime

from pydantic import BaseModel, Field, field_validator
from typing import Optional
import pandas as pd

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
    # id: int  # bigint not null
    Market: str  # text not null
    # @field_validator('Change_in_Open_Interest_All', 'Change_in_Dealer_Long_All', 
    #                  'Change_in_Dealer_Short_All', 'Change_in_Dealer_Spread_All',
    #                  'Change_in_Asset_Mgr_Long_All', 'Change_in_Asset_Mgr_Short_All',
    #                  'Change_in_Asset_Mgr_Spread_All', 'Change_in_Lev_Money_Long_All',
    #                  'Change_in_Lev_Money_Short_All', 'Change_in_Lev_Money_Spread_All',
    #                  'Change_in_Other_Rept_Long_All', 'Change_in_Other_Rept_Short_All',
    #                  'Change_in_Other_Rept_Spread_All', 'Change_in_Tot_Rept_Long_All',
    #                  'Change_in_Tot_Rept_Short_All', 'Change_in_NonRept_Long_All',
    #                  'Change_in_NonRept_Short_All',
    #                  'Traders_Dealer_Long_All', 'Traders_Dealer_Short_All',
    #                  'Traders_Dealer_Spread_All', 'Traders_Asset_Mgr_Long_All',
    #                  'Traders_Asset_Mgr_Short_All', 'Traders_Asset_Mgr_Spread_All',
    #                  'Traders_Lev_Money_Long_All', 'Traders_Lev_Money_Short_All',
    #                  'Traders_Lev_Money_Spread_All', 'Traders_Other_Rept_Long_All',
    #                  'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All',
    #                  mode='before')
    # @classmethod
    # def convert_to_string(cls, v):
    #     """Convert numeric values to string, handle NaN/None"""
    #     if v is None or pd.isna(v):
    #         return None
    #     # Convert to string, removing .0 for integers if desired
    #     if isinstance(v, (int, float)):
    #         # Check if it's a whole number
    #         if isinstance(v, float) and v.is_integer():
    #             return str(int(v))
    #         return str(v)
    #     return str(v) if v is not None else None
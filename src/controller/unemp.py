import asyncio
import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import sdmx
from custom_types.cpi import UNEMPType
from model.unemp import UNEMP_Model
from typing import List


class UnempController:
    
    def __init__(self):
        self.unemp = UNEMP_Model()
       
    
    async def get_unemp(self, country=["USA","CAN","JPN","DEU","GBR","AUS","IND","CHN","KOR","BRA","FRA"]):
        try:
            last_gdp = await self.unemp.get_last_report()
            
            if last_gdp == []:
                gdp_data = await self.get_unemp_history(country)
                
                if gdp_data == None:
                    return
                
                insert = await self.unemp.insert_unemp_report(gdp_data)
                
                return insert
            
            if last_gdp:
                # Get all countries with unemployment rate in databasae
                db_country = [ele.country_code for ele in last_gdp]
                # Check if there are new countries added to the list of countries
                new_country = [ele for ele in country if ele not in set(db_country)]
                
                # Add the new countries unemployment rate data to database
                if new_country:
                   
                    gdp_data = await self.get_unemp_history(new_country)
                    logging.info(f" Add new country cpi into database with keys: {new_country}")
                
                
                least_date = min(last_gdp, key=lambda obj: obj.report_date)
                
                new_date = least_date.report_date.strftime('%Y')
                
                gdp_data = await self.get_unemp_history(country, new_date)
                
                logging.info(f"Update PPI data: {new_country}")
        except Exception as e:
            logging.error(f"Error in getting employment rate")
    
    
    async def get_unemp_history(self,countries:List[str], start_date:str ="2010-01")->List[UNEMPType] | None:
        try:
           
            IMF_DATA = sdmx.Client('IMF_DATA')
            
            country_key = "+".join(countries)
            # Dictionary key format
            key_dict = {
                'COUNTRY': country_key,
                'INDICATOR': 'LUR',
                'FREQUENCY': 'A'
            }
            
            # Fetch data
            data_msg = IMF_DATA.data(
                'WEO',
                key=key_dict,  # ✅ Pass as dictionary
                params={'startPeriod': '2010-01'},
            )
            
            unemp_df = sdmx.to_pandas(data_msg)
            
            today = pd.Timestamp.now()

            # Get date 1 year ago
            last_year = today - pd.DateOffset(years=1)
            if unemp_df.empty:
                return None
            
            unemp_model = []
            for _, idx in unemp_df.reset_index().iterrows():
                report_date = pd.to_datetime(idx["TIME_PERIOD"], format='%Y')
                
                if  report_date <= last_year:
                    unemp_model.append(
                        UNEMPType(
                            country_code=idx["COUNTRY"],
                            freq=idx["FREQUENCY"],
                            report_date=report_date,
                            index_value=idx["value"] ,
                            # forecast_value=idx["value"] if report_date > last_year else None,
                            id=None
                        )
                    )
            return unemp_model
        except Exception as e:
            logging.error(f"Error getting cpi history: {e}", exc_info=True)
            raise        
            
test = UnempController()


# test = CotModell()
loop = asyncio.get_event_loop()

if loop.is_running():
    # If loop is already running, schedule the coroutine
    val = asyncio.create_task(test.get_unemp())
    print(val)
else:
    # If no loop is running, run it synchronously
    val = asyncio.run(test.get_unemp())
    print(val)
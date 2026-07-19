from datetime import datetime
import io
import os
import sys
import asyncio
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import aiohttp
from yarl import URL
import requests
import sdmx
from sdmx.model import Key
from typing import List
import wbgapi as wb
import pandas_datareader.data as web
import imfp
import datetime
from custom_types.cpi import PPIType
from model.ppi import PPIModel

import pandas as pd


class PPIController:
    def __init__(self):
        self.ppi = PPIModel()
        
    async def get_ppi(self, country=["USA","CAN","JPN","DEU","GBR","AUS","IND","CHN","KOR","BRA","FRA"]):
        try:
            last_ppi = await self.ppi.get_last_report()
            
            
            if last_ppi == []:
                
                ppi_data = await self.get_ppi_history(country)
                
                if ppi_data == None:
                    return
                
                insert = await self.ppi.insert_ppi_report(ppi_data)
                
                return insert
            
            if last_ppi:
                # Get all countries with cpi in databasae
                db_country = [ele.country_code for ele in last_ppi]
                # Check if there are new countries added to the list of countries
                new_country = [ele for ele in country if ele not in set(db_country)]
                
                # Add the new countries cpi data to database
                if new_country:
                   
                    ppi_data = await self.get_ppi_history(new_country)
                    logging.info(f" Add new country cpi into database with keys: {new_country}")
                
                
                least_date = min(last_ppi, key=lambda obj: obj.report_date)
                
                new_date = least_date.report_date.strftime('%Y-%m')
                
                ppi_data = await self.get_ppi_history(country, new_date)
                
                logging.info(f"Update PPI data: {new_country}")
            
        except Exception as e:
            logging.error(f"Error getting ppi history: {e}", exc_info=True)
            raise
    
    async def get_ppi_history(self,countries:List[str], start_date:str ="2010-01")->List[PPIType] | None:
        try:
           
            IMF_DATA = sdmx.Client('IMF_DATA')
            
            # Dictionary key format
            key_dict = {
                'COUNTRY': countries,
                'INDICATOR': 'PPI',
                'TYPE_OF_TRANSFORMATION': 'IX',
                'FREQUENCY': 'M'
            }
            
            # Fetch data
            data_msg = IMF_DATA.data(
                'PPI',
                key=key_dict,  # ✅ Pass as dictionary
                params={'startPeriod': '2010-01'},
            )
            
            ppi_df = sdmx.to_pandas(data_msg)
            
            if ppi_df.empty:
                return None
            
            ppi_model= [PPIType(country_code=idx["COUNTRY"],freq="M",
                                report_date= pd.to_datetime(idx["TIME_PERIOD"], format='%Y-M%m'),
                                index_value=idx["value"], id=None) 
                        for _, idx in ppi_df.reset_index().iterrows()]
            
            return ppi_model
        except Exception as e:
            logging.error(f"Error getting cpi history: {e}", exc_info=True)
            raise
        
        
test = PPIController()


# test = CotModell()
loop = asyncio.get_event_loop()

if loop.is_running():
    # If loop is already running, schedule the coroutine
    val = asyncio.create_task(test.get_ppi())
    print(val)
else:
    # If no loop is running, run it synchronously
    val = asyncio.run(test.get_ppi())
    print(val)
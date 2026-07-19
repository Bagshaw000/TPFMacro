# This class handles all things CPI
from datetime import datetime
import os
import sys
import asyncio
import logging
from typing import List

import pandas as pd


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sdmx
from custom_types.cpi import CPIType
from model.cpi import CPIModel
# from src.nat import broker

class CPIController:
    
    def __init__(self):
        self.cpi = CPIModel()
    
    async def get_cpi(self, country=["USA","CAN","JPN","DEU","GBR","AUS","IND","CHN","KOR","BRA","FRA"]):
        try:
            country_key = "+".join(country)
            
            # Get the last the entry for cpi for all country
            last_cpi = await self.cpi.get_last_report()
            
            # If no CPI information is in the database
            if last_cpi == []:
                # Get all the history for countries push this to the message broker
             
                cpi_data = await self.get_cpi_history(country_key)
                
                if cpi_data == None:
                    return
                
                # Insert cpi report into database
                insert = await self.cpi.insert_cpi_report(cpi_data)
                
                return insert
            
            #If cpi has some information
            if last_cpi:
                # Get all countries with cpi in databasae
                db_country = [ele.country_code for ele in last_cpi]
                # Check if there are new countries added to the list of countries
                new_country = [ele for ele in country if ele not in set(db_country)]
                
                # Add the new countries cpi data to database
                if new_country:
                    new_country_key = "+".join(new_country)
                    cpi_data = await self.get_cpi_history(new_country_key)
                    logging.info(f" Add new country cpi into database with keys: {new_country_key}")
                
                
                
                least_date = min(last_cpi, key=lambda obj: obj.report_date)
                
                new_date = least_date.report_date.strftime('%Y-%m')
                
                cpi_data = await self.get_cpi_history(country_key, new_date)
                
                logging.info(f"Update CPI data: {new_country_key}")
    
            
            
        except Exception as e:
            logging.error(f"Error Getting CPI history: {e}")
            raise
    
    # Get CPI history
    async def get_cpi_history(self,key:str, start_date:str ="2010-01")->List[CPIType] | None:
        try:
            IMF_DATA = sdmx.Client('IMF_DATA')
            
            data_msg = IMF_DATA.data('CPI', key=f'{key}.CPI._T.IX.M', params={'startPeriod': start_date},  dsd=True)

            cpi_df = sdmx.to_pandas(data_msg)
            
            if cpi_df.empty:
                return None
            
            cpi_model= [CPIType(country_code=idx["COUNTRY"],freq="M",
                                report_date= pd.to_datetime(idx["TIME_PERIOD"], format='%Y-M%m'),
                                index_value=idx["value"], id=None) 
                        for _, idx in cpi_df.reset_index().iterrows()]
            
            
            return cpi_model
            
            
        except Exception as e:
            logging.error(f"Error getting cpi history: {e}", exc_info=True)
            raise
            
            
    # async def store_cpi_data(data)
    
# test = CPIController()


# # test = CotModell()
# loop = asyncio.get_event_loop()

# if loop.is_running():
#     # If loop is already running, schedule the coroutine
#     val = asyncio.create_task(test.get_cpi())
#     print(val)
# else:
#     # If no loop is running, run it synchronously
#     val = asyncio.run(test.get_cpi())
#     print(val)
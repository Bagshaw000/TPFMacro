from datetime import datetime
import io
import json
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
from custom_types.cpi import PPIType, countries
from model.ppi import PPIModel
from database.redis_ import RedisConnection
import pandas as pd


class PPIController:
    def __init__(self):
        self.ppi = PPIModel()
        self.redis = RedisConnection().get_async_redis()
        
    async def get_ppi(self):
        try:
            global countries
            last_ppi = await self.ppi.get_last_report()
            
            
            if last_ppi == []:
                
                ppi_data = await self.get_ppi_history(countries)
                
                if ppi_data == None:
                    return
                
                insert = await self.ppi.insert_ppi_report(ppi_data)
                
                return insert
            
            if last_ppi:
                # Get all countries with cpi in databasae
                db_country = [ele.country_code for ele in last_ppi]
                # Check if there are new countries added to the list of countries
                new_country = [ele for ele in countries if ele not in set(db_country)]
                
                # Add the new countries cpi data to database
                if new_country:
                   
                    ppi_data = await self.get_ppi_history(new_country)
                    logging.info(f" Add new country cpi into database with keys: {new_country}")
                
                
                least_date = min(last_ppi, key=lambda obj: obj.report_date)
                
                new_date = least_date.report_date.strftime('%Y-%m')
                
                ppi_data = await self.get_ppi_history(countries, new_date)
                
                await self.store_percent_change()
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
    
    async def calculate_pct_change(self) :
            try:
                # Get the last two cpi value 
                ppi_values = await self.ppi.get_percent_ppi()
                
                if not ppi_values:
                    return None
                
                df = pd.DataFrame([
                    {
                        'country_code': item.country_code,
                        'report_date': item.report_date,  # Make sure this field exists
                        'index_value': item.index_value
                    }
                    for item in ppi_values
                ])
                
     
                df['report_date'] = pd.to_datetime(df['report_date'])
                df = df.sort_values(['country_code', 'report_date'])
    
                # Calculate percentage change
                df['pct_change'] = df.groupby('country_code')['index_value'].pct_change() * 100
                df['change_points'] = df.groupby('country_code')['index_value'].diff()

                return df
            except Exception as e:
                logging.error(f"Error Calculating the percentage change:{e}")   
                raise
            
    async def store_percent_change(self):
        try:
            pct_df = await self.calculate_pct_change()
            
            if pct_df is None:
                return
            
            df_with_pct = pct_df[pct_df['pct_change'].notna()].copy()
            max_date = df_with_pct['report_date'].max()
            df_latest = df_with_pct[df_with_pct['report_date'] == max_date].copy()
            avg_df = df_latest['pct_change'].mean()

            
            pipeline = self.redis.pipeline()
            df_with_pct['report_date']= df_with_pct['report_date'].dt.strftime('%Y-%m-%d')
            result_dict = df_with_pct.set_index('country_code').to_dict('index')
            
            
            key = "ppi"
            if not result_dict:
                return None
            
            print(result_dict)
            for country, data in result_dict.items():
                pipeline.set(f"{key}:{str(country)}", json.dumps(data))
            
            pipeline.set(f"{key}:avg",value= json.dumps(avg_df))
            
            pipe_res = await pipeline.execute()
            
            
            return pipe_res
        
            
        except Exception as e:
            logging.error(f"Error storing percent change: {e}",exc_info=True)
            raise
    
# test = PPIController()


# test = CotModell()
# loop = asyncio.get_running_loop()

# if loop.is_running():
#     # If loop is already running, schedule the coroutine
#     val = asyncio.create_task(test.calculate_pct_change())
#     print(val)
# else:
    # If no loop is running, run it synchronously
# val = asyncio.run(test.store_percent_change())
# print(val)
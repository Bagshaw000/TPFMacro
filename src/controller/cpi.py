# This class handles all things CPI
from datetime import datetime
import json
import os
import sys
import asyncio
import logging
from typing import List
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sdmx
from custom_types.cpi import CPIType, countries
from model.cpi import CPIModel
import pandas as pd
from database.redis_ import RedisConnection


class CPIController:
    
    def __init__(self):
        self.cpi = CPIModel()
        self.redis = RedisConnection().get_async_redis()
    
    async def get_cpi(self):
        try:
            global countries
            country_key = "+".join(countries)
            
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
                new_country = [ele for ele in countries if ele not in set(db_country)]
                
                # Add the new countries cpi data to database
                if new_country:
                    new_country_key = "+".join(new_country)
                    cpi_data = await self.get_cpi_history(new_country_key)
                    logging.info(f" Add new country cpi into database with keys: {new_country_key}")
                
                
                
                least_date = min(last_cpi, key=lambda obj: obj.report_date)
                
                new_date = least_date.report_date.strftime('%Y-%m')
                
                cpi_data = await self.get_cpi_history(country_key, new_date)
                # update the cpi redis 
                
                await self.store_percent_change()
                
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
            
    async def calculate_pct_change(self) :
        try:
            # Get the last two cpi value 
            cpi_values = await self.cpi.get_percent_cpi()
            
            if not cpi_values:
                return None
            
            df = pd.DataFrame([
    {
        'country_code': item.country_code,
        'report_date': item.report_date,  # Make sure this field exists
        'index_value': item.index_value
    }
    for item in cpi_values
])
            
            print(df.columns.to_list())
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
           
            
            key = "cpi"
            if not result_dict:
                return None
            
            print(result_dict)
            for country, data in result_dict.items():
                pipeline.set(f"{key}:{str(country)}", json.dumps(data))
            
            pipeline.set(f"{key}:avg",value= json.dumps(avg_df))
            
            pipe_res = await pipeline.execute()
            
            
            return pipe_res
        
            
        except Exception as e:
            logging.error(f"Error sftoring percent change: {e}",exc_info=True)
            raise

    
# test = CPIController()


# # test = CotModell()
# loop = asyncio.get_event_loop()

# if loop.is_running():
#     # If loop is already running, schedule the coroutine
#     val = asyncio.create_task(test.store_percent_change())
#     print(val)
# else:
#     # If no loop is running, run it synchronously
# val = asyncio.run(test.store_percent_change())
# print(val)
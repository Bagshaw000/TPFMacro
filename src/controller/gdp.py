import asyncio
import json
import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List
from database.redis_ import RedisConnection
import pandas as pd
import sdmx

from custom_types.cpi import GDPType, countries
from model.gdp import GDPModel


class GDPController:
    def __init__(self):
        self.gdp = GDPModel()
        self.redis = RedisConnection().get_async_redis()
        
    async def get_gdp(self,country=["USA","CAN","JPN","DEU","GBR","AUS","IND","CHN","KOR","BRA","FRA"]):
        try:
            last_gdp = await self.gdp.get_last_report()
            
            if last_gdp == []:
                gdp_data = await self.get_gdp_history(country)
                
                if gdp_data == None:
                    return
                
                insert = await self.gdp.insert_gdp_report(gdp_data)
                
                return insert
            
            if last_gdp:
                # Get all countries with gdp in databasae
                db_country = [ele.country_code for ele in last_gdp]
                # Check if there are new countries added to the list of countries
                new_country = [ele for ele in country if ele not in set(db_country)]
                
                # Add the new countries gdp data to database
                if new_country:
                   
                    gdp_data = await self.get_gdp_history(new_country)
                    logging.info(f" Add new country cpi into database with keys: {new_country}")
                
                
                least_date = min(last_gdp, key=lambda obj: obj.report_date)
                
                new_date = least_date.report_date.strftime('%Y')
                
                gdp_data = await self.get_gdp_history(country, new_date)
                
                logging.info(f"Update PPI data: {new_country}")
            
        except Exception as e:
            logging.error("Error getting GDP")
            raise
        
    async def get_gdp_history(self,countries:List[str], start_date:str ="2010-01")->List[GDPType] | None:
        try:
           
            IMF_DATA = sdmx.Client('IMF_DATA')
            
            
            country_key = "+".join(countries)
            # Dictionary key format
            key_dict = {
                'COUNTRY': country_key,
                'INDICATOR': 'NGDPD',
                'FREQUENCY': 'A'
            }
            
            # Fetch data
            data_msg = IMF_DATA.data(
                'WEO',
                key=key_dict,  # ✅ Pass as dictionary
                params={'startPeriod': '2010-01'},
            )
            
            gdp_df = sdmx.to_pandas(data_msg)
            
            today = pd.Timestamp.now()

            # Get date 1 year ago
            last_year = today - pd.DateOffset(years=1)
            if gdp_df.empty:
                return None
            
            gdp_model = []
            for _, idx in gdp_df.reset_index().iterrows():
                report_date = pd.to_datetime(idx["TIME_PERIOD"], format='%Y')
                
                if  report_date <= last_year:
                    gdp_model.append(
                        GDPType(
                            country_code=idx["COUNTRY"],
                            freq=idx["FREQUENCY"],
                            report_date=report_date,
                            index_value=idx["value"] ,
                            # forecast_value=idx["value"] if report_date > last_year else None,
                            id=None
                        )
                    )
            return gdp_model
        except Exception as e:
            logging.error(f"Error getting cpi history: {e}", exc_info=True)
            raise
        
    async def calculate_pct_change(self) :
        try:
            # Get the last two cpi value 
            gdp_values = await self.gdp.get_percent_gdp()
            
            if not gdp_values:
                return None
            
            df = pd.DataFrame([
                {
                    'country_code': item.country_code,
                    'report_date': item.report_date,  # Make sure this field exists
                    'index_value': item.index_value
                }
                for item in gdp_values
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
            
            avg_df = df_with_pct['pct_change'].mean()
            
            
            
            pipeline = self.redis.pipeline()
            df_with_pct['report_date']= df_with_pct['report_date'].dt.strftime('%Y-%m-%d')
            result_dict = df_with_pct.set_index('country_code').to_dict('index')
            
            
            key = "gdp"
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
# test = GDPController()


# # test = CotModell()
# loop = asyncio.get_event_loop()

# if loop.is_running():
#     # If loop is already running, schedule the coroutine
#     val = asyncio.create_task(test.get_gdp())
#     print(val)
# else:
#     # If no loop is running, run it synchronously
# val = asyncio.run(test.store_percent_change())
# print(val)
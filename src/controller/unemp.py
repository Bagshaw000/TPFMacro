# import asyncio
# import json
# import logging
# import os
# import sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# import pandas as pd
# import sdmx
# from custom_types.cpi import UNEMPType, countries
# from model.unemp import UNEMP_Model
# from typing import List
# from database.redis_ import RedisConnection


# class UNEMPController:
    
#     def __init__(self):
#         self.unemp = UNEMP_Model()
#         self.redis = RedisConnection().get_async_redis()
       
    
#     async def get_unemp(self, country=["USA","CAN","JPN","DEU","GBR","AUS","IND","CHN","KOR","BRA","FRA"]):
#         try:
#             last_gdp = await self.unemp.get_last_report()
            
#             if last_gdp == []:
#                 gdp_data = await self.get_unemp_history(country)
                
#                 if gdp_data == None:
#                     return
                
#                 insert = await self.unemp.insert_unemp_report(gdp_data)
                
#                 return insert
            
#             if last_gdp:
#                 # Get all countries with unemployment rate in databasae
#                 db_country = [ele.country_code for ele in last_gdp]
#                 # Check if there are new countries added to the list of countries
#                 new_country = [ele for ele in country if ele not in set(db_country)]
                
#                 # Add the new countries unemployment rate data to database
#                 if new_country:
                   
#                     gdp_data = await self.get_unemp_history(new_country)
#                     logging.info(f" Add new country cpi into database with keys: {new_country}")
                
                
#                 least_date = min(last_gdp, key=lambda obj: obj.report_date)
                
#                 new_date = least_date.report_date.strftime('%Y')
                
#                 gdp_data = await self.get_unemp_history(country, new_date)
#                 await self.store_percent_change()
                
#                 logging.info(f"Update PPI data: {new_country}")
#         except Exception as e:
#             logging.error(f"Error in getting employment rate")
    
    
#     async def get_unemp_history(self,countries:List[str], start_date:str ="2010-01")->List[UNEMPType] | None:
#         try:
           
#             IMF_DATA = sdmx.Client('IMF_DATA')
#             print(IMF_DATA.series_keys)
            
#             country_key = "+".join(countries)
#             # Dictionary key format
#             key_dict = {
#                 'COUNTRY': country_key,
#                 'INDICATOR': 'U',
#                 'TYPE_OF_TRANSFORMATION': 'POP_PCH_PT', 
#                 'FREQUENCY': 'M'
#             }
            
#             # Fetch data
#             data_msg = IMF_DATA.data(
#                 'LS',
#                 key=key_dict,  # ✅ Pass as dictionary
#                 params={'startPeriod': start_date},
               
#             )
            
#             unemp_df = sdmx.to_pandas(data_msg)
            
#             print(unemp_df.tail(10))
#             # today = pd.Timestamp.now()
#             # # Get date 1 year ago
#             # last_year = today - pd.DateOffset(years=1)
#             if unemp_df.empty:
#                 return None
            
#             unemp_model = []
#             for _, idx in unemp_df.reset_index().iterrows():
#                 report_date = pd.to_datetime(idx["TIME_PERIOD"], format='%Y-M%m')
                
                
#                 unemp_model.append(
#                     UNEMPType(
#                         country_code=idx["COUNTRY"],
#                         freq=idx["FREQUENCY"],
#                         report_date=report_date,
#                         index_value=idx["value"] ,
#                         # forecast_value=idx["value"] if report_date > last_year else None,
#                         id=None
#                     )
#                 )
#             return unemp_model
#         except Exception as e:
#             logging.error(f"Error getting unemp history: {e}", exc_info=True)
#             raise   
        
        
#     async def calculate_pct_change(self) :
#         try:
#             # Get the last two cpi value 
#             unemp_values = await self.unemp.get_percent_unemp()
            
#             if not unemp_values:
#                 return None
            
#             df = pd.DataFrame([
#                 {
#                     'country_code': item.country_code,
#                     'report_date': item.report_date,  # Make sure this field exists
#                     'index_value': item.index_value
#                 }
#                 for item in unemp_values
#             ])
            
    
#             df['report_date'] = pd.to_datetime(df['report_date'])
#             df = df.sort_values(['country_code', 'report_date'])

#             # Calculate percentage change
#             df['pct_change'] = df.groupby('country_code')['index_value'].pct_change() * 100
#             df['change_points'] = df.groupby('country_code')['index_value'].diff()

#             return df
#         except Exception as e:
#             logging.error(f"Error Calculating the percentage change:{e}")   
#             raise
                    
#     async def store_percent_change(self):
#         try:
#             pct_df = await self.calculate_pct_change()
            
#             if pct_df is None:
#                 return
            
#             df_with_pct = pct_df[pct_df['pct_change'].notna()].copy()
#             max_date = df_with_pct['report_date'].max()
#             df_latest = df_with_pct[df_with_pct['report_date'] == max_date].copy()
#             avg_df = df_latest['pct_change'].mean()
            
            
#             pipeline = self.redis.pipeline()
#             df_with_pct['report_date']= df_with_pct['report_date'].dt.strftime('%Y-%m-%d')
#             result_dict = df_with_pct.set_index('country_code').to_dict('index')
            
            
#             key = "unemp"
#             if not result_dict:
#                 return None
            
#             print(result_dict)
#             for country, data in result_dict.items():
#                 pipeline.set(f"{key}:{str(country)}", json.dumps(data))
            
#             pipeline.set(f"{key}:avg",value= json.dumps(avg_df))
            
#             pipe_res = await pipeline.execute()
            
            
#             return pipe_res
        
            
#         except Exception as e:
#             logging.error(f"Error storing Unemployment percent change: {e}",exc_info=True)
#             raise     
            
# test = UNEMPController()


# # test = CotModell()
# loop = asyncio.get_event_loop()

# if loop.is_running():
    
#     # If loop is already running, schedule the coroutine
#     val = asyncio.create_task(test.get_unemp_history(["USA"]))
#     # print(val)
# else:
#     # If no loop is running, run it synchronously
#     val = asyncio.run(test.get_unemp_history(["USA"]))
#     # print(val)
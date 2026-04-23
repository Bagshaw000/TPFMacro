import asyncio
from datetime import datetime
import json
import logging
import math
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.redis_ import RedisConnection
from model import CotModel
import pandas as pd


class COTController:
    
    def __init__(self):
        self.redis = RedisConnection().get_redis()
        self.cot = CotModel() 
        
    #Get Cot data 
    async def get_cot_data(self):
        try:
            # Check if data is stored in redis
            data_obj = dict()
            check_cot =   self.redis.keys("cot_ttf*")
            
            # # self.redis.hdel('cot_ttf', *check_cot)
            # self.redis.delete(*check_cot)

            # # Or clear all fields but keep the hash
            # # self.redis.hdel('cot_ttf', *self.redis.hkeys('cot_ttf*'))
            # return
            with open('data/instr.json', 'r') as f:
                data = json.load(f)
                
            for category, items in data.items():
                data_obj[category] = items
            
            data_keys = list(data_obj.keys())

            # Check if we have existing records
       
            if not check_cot:
                
                # print("Empty") 
                # print(await self.cot.get_latest_cot_data())
                data = await self.cot.get_cot_data_size()
                data_list = await self.batch_get_data(data.count)
                # print((data))
                insert_data = await self.insert_cot_redis(data_list)
                
                if not insert_data[0]:
                    return {}
            
          
                
            currency_data,indices_data,financial_data,crypto_data =await asyncio.gather( self.convert_redis_dataframe(data_obj[data_keys[0]],data_keys[0]),self.convert_redis_dataframe(data_obj[data_keys[1]],data_keys[1]),self.convert_redis_dataframe(data_obj[data_keys[2]],data_keys[2]),self.convert_redis_dataframe(data_obj[data_keys[3]],data_keys[3]))
                
            cur_pct,ind_pct,fin_pct,crypt_pct = await asyncio.gather(self.calculate_all_change(currency_data),self.calculate_all_change(indices_data),self.calculate_all_change(financial_data),self.calculate_all_change(crypto_data)) 
        
            await self.interpret_pct_change(cur_pct,"Currency")
            
            data = {
                "Currency":cur_pct,
                "Indicies":ind_pct,
                "Financial":fin_pct,
                "Crypto":crypt_pct
            }  
            
            return data
            # if check_cot:
       
            #     # print("Not empty")
            #     # Filter by symbol and return the last 52 weeks
            #     # crypto_data = self.redis.keys("cot_ttf:Crypto:*")  # or "cot_ttf/Crypto"
            #     # currency_data = self.redis.hgetall("cot_ttf:Currency")
            #     # financial_data = self.redis.hgetall("cot_ttf:Financial")
            #     # indices_data = self.redis.hgetall("cot_ttf:Indices")
                
            #     currency_data,indices_data,financial_data,crypto_data =await asyncio.gather( self.convert_redis_dataframe(data_obj[data_keys[0]],data_keys[0]),self.convert_redis_dataframe(data_obj[data_keys[1]],data_keys[1]),self.convert_redis_dataframe(data_obj[data_keys[2]],data_keys[2]),self.convert_redis_dataframe(data_obj[data_keys[3]],data_keys[3]))
                
               
            #     cur_pct,ind_pct,fin_pct,crypt_pct = await asyncio.gather(self.calculate_all_change(currency_data),self.calculate_all_change(indices_data),self.calculate_all_change(financial_data),self.calculate_all_change(crypto_data)) 

            #     # await self.interpret_pct_change(cur_pct,"Currency")
            #     # Pass the data to LLM
            
        except Exception as e:
            logging.error(f"Error getting data : {e}", exc_info=True)
    
    # This function interpretes the pct change for all instruments   
    async def interpret_pct_change(self, data:dict,asset_cls:str):
        try:
            
            # Check if the recent LLM intepretation exists
       
            
            for instrument, instrument_data in data.items():
                *rest, last = instrument_data.keys()
         
                date_series = pd.to_datetime(rest).max().strftime('%Y-%m-%d')
               
                
                # Search if the recent explanation has been set
                instrument_key = f"cot_expl:{asset_cls}:{instrument}:{date_series}"
                check_exp = self.redis.hgetall(instrument_key)
                # print(check_exp)
                
                if check_exp:
                    # Just append explanation to the  dict object
                    continue
                
                # Loop through pct change and ask LLM to explain append that to the Object
                
            # Loop through the data and select 2 input for each field
            
        except Exception as e:
            logging.error("Error interpreting cot data")     
    
    # This function calculates the pct change for different intervals
    async def calculate_all_change(self, data:dict):
        try:
            fields = ['Large_Spec_Net', 'Commercial_Net', 'Dealer_Net', 
                  'Asset_Mgr_Net', 'Lev_Money_Net', 'Other_Rept_Net']
            
            result = dict()
            max_weeks = 52
            
            for instrument, instrument_data in data.items():
                # If no data is found skip
                if not instrument_data:
                    continue
                
                # Transpose the data to make the date the index and sort based on the date
                df = pd.DataFrame(instrument_data).T
                df.index = pd.to_datetime(df.index)
                df = df.sort_index()
                
                
                for field in fields:
                    if field in df.columns:
                        df[field] = pd.to_numeric(df[field], errors="coerce")
                        
                df = df.ffill()
                # Create date range  for all date
               
            #    Limit to the last 52 weeks
            
                if len(df) > max_weeks:
                    df = df.tail(max_weeks)
                
                
                # Calculate shifts for different periods
                periods = {
                    '1_month': 4,    # ~4 weeks
                    '3_month': 12,   # ~12 weeks
                    '6_month': 24,   # ~24 weeks
                    '1_year': 52     # ~52 weeks
                }
                
    
                
                
                instrument_results = {}
                # Loop through all the selected fields in the dataframe
                for field in fields:
                    if field not in df.columns:
                        continue
                    
                    field_results = {}
                    
                    # Loop through all the periods
                    for period_name, weeks in periods.items():
                        
                        # Get data in the that time interval
                        shifted = df[field].shift(weeks)
                        
                        mask = (shifted.notna()) & (shifted != 0)
                        
                         # Calculate percentage changes
                        pct_changes = ((df[field] - shifted) / abs(shifted)) * 100
                        pct_changes = pct_changes[mask].round(2)
                        
                        # Filter to original dates only
                        period_data = {}
                        for date, value in pct_changes.items():
                            if date in df.index:  # Only original data points
                                period_data[str(date.date())] = value                        

                        # Limit the amount of data by the last 52 weeks
                        if period_data:
                            if period_name == "1_month":
                                sliced_dict = list(period_data.items())
                                
                                field_results[period_name] = dict(sliced_dict[-52:])
                            
                            if period_name == "3_month":
                                sliced_dict = list(period_data.items())
                                
                                field_results[period_name] = dict(sliced_dict[-18:])
                                
                            if period_name == "6_month":
                                sliced_dict = list(period_data.items())
                                
                                field_results[period_name] = dict(sliced_dict[-9:])
                                
                            if period_name == "1_year":
                                sliced_dict = list(period_data.items())
                                
                                field_results[period_name] = dict(sliced_dict[-5:])
                            
                           
                        
            
                    if field_results:
                        instrument_results[field] = field_results
                
                result[instrument] = instrument_results
                data[instrument]["pct_change"] = result[instrument]
         
            return data

        except Exception as e:
            logging.error(f"Error getting data : {e}", exc_info=True)
    
    
    # async def clean_data(self, )
    async def convert_redis_dataframe(self,asset_list:list,asset_cls:str)  :
        try:
            # print(asset_cls)
            # print(asset_list)
            df = dict()
            for asset in asset_list:
                temp_dict = {}
                # df[asset] = {}
                data = self.redis.keys(f"cot_ttf:{asset_cls}:{asset}:*")
                # print(data)
                
                for key in data:
                    date_key = key.split(":")
                    hash_data = self.redis.hgetall(key)
                    
                    if hash_data:
                         temp_dict[date_key[-1]] = hash_data
                    
                if temp_dict:  # Filters out assets with no data
                    df[asset] = temp_dict   
                    
                    
            return df
        except Exception as e:
            logging.error(f"Converting redis to dataframe", exc_info=True)     

    #Get all cot data in batches  
    async def batch_get_data(self, count:int):
        try:
            data_list = []
            if count> 0:
                
                loop_len = math.ceil(count/1000)
                
                for i in range(loop_len):
                   
                    start_index = i * 1000
                    end_index = min(start_index + 1000 - 1, count - 1)
                    data = await self.cot.get_latest_cot_data(start_index,end_index)
                    data_list.extend(data.data[:])
            
            
            return data_list
        except Exception as e:
            logging.error(f"Error batch processing : {e}")

    # This insert the cot_ttf into the redis
    async def insert_cot_redis(self, data:list):
        try:
            # pass
            pipe = self.redis.pipeline()
            # print(len(data))
            df = pd.DataFrame(data)
            
            # Example calculations
            df['Dealer_Net'] = df['Dealer_Positions_Long_All'] - df['Dealer_Positions_Short_All']
            # Positive = Dealers are net long (could be bearish signal - hedging client selling)
            # Negative = Dealers are net short (could be bullish signal - hedging client buying)
            
            df['Asset_Mgr_Net'] = df['Asset_Mgr_Positions_Long_All'] - df['Asset_Mgr_Positions_Short_All']
            # Positive = Institutions bullish (trend-following)
            # Negative = Institutions bearish (trend-following)
            
            df['Lev_Money_Net'] = df['Lev_Money_Positions_Long_All'] - df['Lev_Money_Positions_Short_All']
            # Positive = Hedge funds/CTAs bullish (often crowded - contrarian signal at extremes)
            # Negative = Hedge funds/CTAs bearish (often crowded - contrarian signal at extremes)
                        
            df['Other_Rept_Net'] = df['Other_Rept_Positions_Long_All'] - df['Other_Rept_Positions_Short_All']
            
            
            df['Commercial_Net'] = df['Dealer_Net'] + df['Other_Rept_Net']
            # Positive = Commercials net long (smart money bullish)
            # Negative = Commercials net short (smart money bearish)

            df['Large_Spec_Net'] = df['Asset_Mgr_Net'] + df['Lev_Money_Net']
            # Positive = Speculators bullish (sentiment indicator)
            # Negative = Speculators bearish (sentiment indicator)
            
            new_df = df[["Market_and_Exchange_Names","Market","Report_Date_as_YYYY_MM_DD","Dealer_Net", 'Dealer_Positions_Long_All','Dealer_Positions_Short_All','Asset_Mgr_Net','Asset_Mgr_Positions_Long_All', 'Asset_Mgr_Positions_Short_All','Commercial_Net','Lev_Money_Positions_Long_All','Lev_Money_Positions_Short_All','Large_Spec_Net','Other_Rept_Net']]
            
          
            for index, row in new_df.iterrows():
                row_date = datetime.fromisoformat(row["Report_Date_as_YYYY_MM_DD"])
                pipe.hset(f"cot_ttf:{row["Market"]}:{row["Market_and_Exchange_Names"]}:{row_date.date()}", mapping=row.to_dict())
            
            exec = pipe.execute()  
            
            return exec  
            
            
        except Exception as e:
            logging.error(f"Error inserting into redis data : {e}",exc_info=True)
            



# import pandas as pd

# # Assuming df is your COT DataFrame

# # DEALER/INTERMEDIARY (Sell Side)
# df['Dealer_Net'] = df['Dealer_Positions_Long_All'] - df['Dealer_Positions_Short_All']

# # ASSET MANAGER/INSTITUTIONAL (Buy Side - Long Term)
# df['Asset_Mgr_Net'] = df['Asset_Mgr_Positions_Long_All'] - df['Asset_Mgr_Positions_Short_All']

# # LEVERAGED FUNDS (Buy Side - Speculative)
# df['Lev_Money_Net'] = df['Lev_Money_Positions_Long_All'] - df['Lev_Money_Positions_Short_All']

# # OTHER REPORTABLES (Buy Side - Commercial Hedgers)
# df['Other_Rept_Net'] = df['Other_Rept_Positions_Long_All'] - df['Other_Rept_Positions_Short_All']

# # TOTAL REPORTABLE (All categories combined)
# df['Tot_Rept_Net'] = df['Tot_Rept_Positions_Long_All'] - df['Tot_Rept_Positions_Short_All']

# # NON-REPORTABLE (Small/Retail Traders)
# df['NonRept_Net'] = df['NonRept_Positions_Long_All'] - df['NonRept_Positions_Short_All']
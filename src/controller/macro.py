import asyncio
from collections import defaultdict
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.redis_ import RedisConnection
from custom_types.cpi import countries
import logging

macro_list = ["cpi","gdp", "ppi", "unemp"]
class MacroController:
    
    def __init__(self):
        self.redis = RedisConnection().get_async_redis()
        
    async def get_global_avg(self):
        try:
            global macro_list
            
            # macro_list.remove("gdp")
         
            avg_dict = defaultdict()
            pipeline = self.redis.pipeline()
            
            # get the global avg
            for ele in macro_list:
                pipeline.get(f"{str(ele)}:avg")
            
            result= await pipeline.execute()
            
           
            for value, macro in zip(result, macro_list):
                avg_dict[macro] = round(float(value),4) or None
                
            return avg_dict
                      
            
        except Exception as e:
            logging.error(f"Error getting global average", exc_info=True)
            raise
    
    async def get_global_stats(self)   :
        try:
            global countries, macro_list
            pipeline = self.redis.pipeline()
            
            data= defaultdict(dict)
            keys = []

            for country in countries:
               
                # pipeline.get(senti_key)
                for macro in macro_list:
                    key = f"{macro}:{country}"
                    keys.append(key)
                    pipeline.get(key)  # Queue all get commands
            
            sentiment_keys = [f"sentiment_news:{country}" for country in countries]
            for senti_key in sentiment_keys:
                pipeline.get(senti_key)
            # Execute all gets at once
            results = await pipeline.execute()
            
            macro_results = results[:len(keys)]
            sentiment_results = results[len(keys):]

            for key, tmp_data in zip(keys, macro_results):
                # Parse key to get country and macro
                macro, country = key.split(':')
                senti_key = f"sentiment_news:{country}"
                senti_score = await self.redis.get(senti_key)
                
                if tmp_data:
                    json_data = json.loads(tmp_data)
                    data[country][macro] = json_data.get("pct_change")
                  
                else:
                    data[country][macro] = None  
            
            for country, senti_score in zip(countries, sentiment_results):
                if senti_score:
                    data[country]["new_sentiment"] = float(senti_score)
                else:
                    data[country]["new_sentiment"] = None
            
            return data
                    
            
        except Exception as e:
            logging.error(f"Error getting global stats: {e}", exc_info=True)
            raise 
        
    async def get_country_stats(self, country:str):
        try:
            data = defaultdict()
            keys = []
            pipeline = self.redis.pipeline()
            for macro in macro_list:
                key = f"{macro}:{country}"
                keys.append(key)
                pipeline.get(key)
            
            sentiment_keys = [f"sentiment_news:{country}" for country in countries]
            for senti_key in sentiment_keys:
                pipeline.get(senti_key)
            # Execute all gets at once
            results = await pipeline.execute()
            
            macro_results = results[:len(keys)]
            sentiment_results = results[len(keys):]

            for key, tmp_data in zip(keys, macro_results):
                # Parse key to get country and macro
                macro, country = key.split(':')
                senti_key = f"sentiment_news:{country}"
                senti_score = await self.redis.get(senti_key)
                
                if tmp_data:
                    json_data = json.loads(tmp_data)
                    data[macro] = json_data.get("pct_change")
                    data[f"{macro}_date"] = json_data.get("report_date")
                    
                else:
                    data[macro] = None  
            
            for country, senti_score in zip(countries, sentiment_results):
                if senti_score:
                    data["new_sentiment"] = float(senti_score)
                else:
                    data["new_sentiment"] = None
            
            return data
        except Exception as e:
            logging.error(f"Error getting countries macro data")
            raise

# test = MacroController()
# loop = asyncio.get_event_loop()

# if loop.is_running():
#     val = asyncio.create_task(test.get_country_stats("USA"))

#     print(val)  
    
# else:
#     val = asyncio.run(test.get_country_stats("USA"))
#     print(val) 
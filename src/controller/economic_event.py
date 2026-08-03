import asyncio
from collections import defaultdict
import json
import logging
from typing import List
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import sys 
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_types.cpi import EconomicEventType
from database.redis_ import RedisConnection
from model.market_overview import MarketOverview

country=["US","CA","JP","DE","UK","AU","IN","CN","KR","BR","FR"]

class EconomicEventController:
    
    def __init__(self):
        self.redis = RedisConnection().get_async_redis()
        self.mko = MarketOverview()
    
    #  This function get ecnomic events from yfinance and filters them  by countries
    async def get_economic_event(self,country=["US","CA","JP","DE","UK","AU","IN","CN","KR","BR","FR"]):
        try:
            
            calendar = yf.Calendars()
            
            ev_df:pd.DataFrame = calendar.get_economic_events_calendar(limit=100)
            
            # Filter from all the event for relevant countries
            country_df = ev_df[ev_df["Region"].isin(country)]
            
            if country_df.empty:
                return None
            
            ev_model = [EconomicEventType(event= idx['Event'],country_code=idx['Region'], event_time=idx["Event Time"], last_value= idx["Last"]) for _, idx in country_df.reset_index().iterrows()]
            
            return ev_model
        except Exception as e:
            logging.error(f"Error getting economic events: {e}", exc_info=True)
    
    #   This function store economic events
    async def store_economic_event(self):
        try:
            # Get events 
            event = await self.get_economic_event()
            
            if event == None:
                return 
            
            pipeline = self.redis.pipeline()
            
            # Get expiration time for all event 
            ex_time = [self.mko.calculate_ttl(e.event_time.strftime('%Y-%m-%d %H:%M:%S%z')) for e in event  if isinstance(e.event_time, datetime)]
            ttls = await asyncio.gather(*ex_time)
            
            # Zip the event expiration time 
            for e, ttl in zip(event, ttls):
                
                e.expiration = ttl
                
                
            # Store event in according to country
            for e in event:
                if isinstance(e.event_time, datetime):
                    e.event_time = e.event_time.isoformat()
                   
                key = f"news:{e.country_code}:{e.event}"
                pipeline.hset(name= key, mapping=e.model_dump())
               
                    
                expiration = e.expiration or 172800
                pipeline.expire(name= key,time=expiration)
                    
            # Execute  redis pipeline
            event_set = await pipeline.execute()
            
            
        except Exception as e:
            logging.error(f"Error storing economic event: {e}", exc_info=True)  
            raise
            
    async def get_all_events(self):
        try:
            # key = "news:*"
            # events = await self.redis.hgetall(name=key)
            # print(events)
            keys = []
            cursor = 0
            global country
            
            while True:
                cursor, batch = await self.redis.scan(cursor, match="news:*",count=100)
                keys.append(batch)
               
                if cursor == 0:
                    break
          
            
            flattened_key:List[str] = sum(keys,[])
            # print(flattened_key)
            events = defaultdict(dict)
            
            for key in flattened_key:
                split_key = key.split(":")
                
                # Get all the redis data fro all keys
                data = await self.redis.hgetall(key)
                
                # Ensure the time data is in datetime format for sorting
                if isinstance(data["event_time"], str):
                    data["event_time"] = datetime.strptime(data["event_time"], '%Y-%m-%dT%H:%M:%S%z')
                
                # Filter for selected countries
                if split_key[1] in country:
                    events[split_key[1]][data["event_time"] ] = data
            
            # Sort the news event for each country
            sorted_events = {
                country:{
                    time_obj.isoformat():  {**data, "event_time": time_obj.isoformat()} 
                    for time_obj, data in sorted(time_dict.items())
                }
                for country, time_dict in events.items()
            }  
              

            return sorted_events
    
        except Exception as e:
            logging.error(f"Error getting all events: {e}", exc_info=True)  
            raise
        
    async def get_event_country(self, country:str):
        try:
            keys = []
            cursor = 0
            print("Test")
            print(country)
            
            while True:
                cursor, batch = await self.redis.scan(cursor, match=f"news:{country}:*",count=100)
                keys.append(batch)
                
                if cursor == 0:
                    break
        
            flattened_key:List[str] = sum(keys,[])
           
            events = defaultdict(dict)
            
            for key in flattened_key:
                split_key = key.split(":")
                
                # Get all the redis data fro all keys
                data = await self.redis.hgetall(key)
                
                # Ensure the time data is in datetime format for sorting
                if isinstance(data["event_time"], str):
                    data["event_time"] = datetime.strptime(data["event_time"], '%Y-%m-%dT%H:%M:%S%z')
                
                # Filter for selected countries
              
                events[split_key[1]][data["event_time"] ] = data
            
            # Sort the news event for each country
            sorted_events = {
                country:{
                    time_obj.isoformat():  {**data, "event_time": time_obj.isoformat()} 
                    for time_obj, data in sorted(time_dict.items())
                }
                for country, time_dict in events.items()
            }  
                
            
            return sorted_events
            
        except Exception as e:
            logging.error(f"Error getting all events for {country} : {e}", exc_info=True)  
            raise
            
# test = EconomicEventController()


# loop = asyncio.get_event_loop()

# if loop.is_running():
#     # If loop is already running, schedule the coroutine
#     val = asyncio.create_task(test.get_event_country("US"))
#     print(val)
# else:
#     # If no loop is running, run it synchronously
#     val = asyncio.run(test.get_event_country("US"))
#     print(val)
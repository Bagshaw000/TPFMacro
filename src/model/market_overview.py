import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
import logging
import re
import sys
import os
import numpy as np
import requests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dateutil.relativedelta import relativedelta
import yfinance as yf
import pandas as pd
from database.redis_ import RedisConnection
from config.config import get_doppler_env


sem = asyncio.Semaphore(10)
class MarketOverview:
    
    def __init__(self):
        self.redis = RedisConnection().get_redis()
        self.aioredis = RedisConnection().get_async_redis()
        self.secret= get_doppler_env()
        
        
    async def get_currency(self):
        try:
            
            all_pairs = (
                # Majors
                'EURUSD=X', 'USDJPY=X', 'GBPUSD=X', 'USDCHF=X', 
                'AUDUSD=X', 'USDCAD=X', 'NZDUSD=X',
                
                # Euro crosses
                'EURGBP=X', 'EURJPY=X', 'EURCHF=X', 'EURAUD=X', 
                'EURCAD=X', 'EURNZD=X', 'EURSEK=X', 'EURNOK=X',
                
                # Yen crosses  
                'GBPJPY=X', 'AUDJPY=X', 'CADJPY=X', 'CHFJPY=X', 'NZDJPY=X',
                
                # Pound crosses
                'GBPCHF=X', 'GBPAUD=X', 'GBPCAD=X', 'GBPNZD=X',
                
                # Swiss crosses
                'CHFAUD=X', 'CHFCAD=X',
                
                # AUD & NZD crosses
                'AUDNZD=X', 'AUDCAD=X', 'AUDCHF=X',
                'NZDCAD=X', 'NZDCHF=X',
                
                # Other USD pairs
                'USDSEK=X', 'USDNOK=X', 'USDMXN=X', 'USDSGD=X',
                'USDTRY=X', 'USDZAR=X', 'USDPLN=X', 'USDHUF=X'
            )
            tickers_string = " ".join(all_pairs)  
            # symbol_df =pd.DataFrame(yf.Tickers(tickers_string).history(period="ytd",repair=True,))
            
            # filter_df = symbol_df
            
            pipeline = self.redis.pipeline()
            pattern = f"overview:currency:"
            currency_keys= [f"{pattern}{symbol.replace("=X",'')}" for symbol in all_pairs]
            # print(currency_keys)
            
           
            # Check if the keys exist
            for key in currency_keys:
                pipeline.exists(key)
            
            # Map all key exist status
            key_exists= dict(zip(currency_keys,[bool(res)for res in pipeline.execute()]))
            
            
            false_keys = [key for key, val in key_exists.items() if not val]
            true_keys = [key for key, val in key_exists.items() if  val]
            
            task = [self.get_currency_ytd(key.split(":")[-1], pattern) for key in false_keys]
            
            true_task =  [self.get_currency_last_entry(key.split(":")[-1], pattern) for key in true_keys]
            # print(true_keys)
            curr = await asyncio.gather(*task)
            curr_tru = await asyncio.gather(*true_task)
            # print(key_exists)
        except Exception as e:
            logging.error(f"Error in currency pipeline {e}", exc_info=True)
    
    async def get_currency_ytd(self, ticker:str,pattern:str):
        try:
            async with sem:
                
                pipeline = self.redis.pipeline()
                format_ticker = f'{ticker}=X'
                one_year_ago = datetime.now() - relativedelta(years=1)
                start = one_year_ago.strftime('%Y-%m-%d')
                
            
                
                symbol_df = pd.DataFrame(yf.Ticker(format_ticker).history(interval='1d',repair=True, start=start))
                
                redis_key = f"{pattern}{ticker}"
                
                dates = symbol_df.index.strftime('%Y-%m-%d')
                data_dicts = [{'open': row['Open'],
                                'high': row['High'], 
                                'low': row['Low'],
                                'close': row['Close'],
                                'volume': row['Volume']}
                              for _, row in symbol_df.iterrows()
                              ]
                
                for dates, data_dicts in zip(dates, data_dicts)   :
                    pipeline.hset(redis_key, dates, json.dumps(data_dicts))
                pipeline.execute()

                await asyncio.sleep(1)
            
        except Exception as e:
            logging.error("Error get currency year to date", exc_info=True)
            
            
    async def get_currency_last_entry(self,ticker:str, pattern:str):
        try:
            async with sem:
                # self.redis.delete("overview:currency:*")
                # keys_to_delete = list(self.redis.scan_iter("overview:currency:*"))
                
                # if keys_to_delete:
                #   self.redis.unlink(*keys_to_delete)
                
                format_ticker = f'{ticker}=X'
                keys = [ field async for field, value in  self.aioredis.hscan_iter(f"overview:currency:{ticker}")]
                sorted_keys = sorted(keys)
                print(sorted_keys[-1])
                # Get the most recent days
                # session = requests.Session()
                # session.headers.update({
                #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                #     'Accept-Language': 'en-US,en;q=0.5',
                # })
                
                
             
                symbol_df = pd.DataFrame(yf.Ticker(format_ticker).history(interval='1d',repair=True, start=str(sorted_keys[-1])))
                
            
                if len(symbol_df)>0:
                    pipeline = self.redis.pipeline()
                    redis_key = f"{pattern}{ticker}"
                
                    dates = symbol_df.index.strftime('%Y-%m-%d')
                    data_dicts = [{'open': row['Open'],
                                    'high': row['High'], 
                                    'low': row['Low'],
                                    'close': row['Close'],
                                    'volume': row['Volume']}
                                for _, row in symbol_df.iterrows()
                                ]
                    
                    for dates, data_dicts in zip(dates, data_dicts):
                        pipeline.hset(redis_key, dates, json.dumps(data_dicts))
                    pipeline.execute()
                await asyncio.sleep(1)
                
        except Exception as e:
            logging.error("Error implementing last currency entry", exc_info=True)
    
    
    async def get_symbol_data(self, ticker, category):
        try:
            ticker_key = f"overview:{category}:{ticker}"
            
            
            async with self.aioredis as redis:
                get_data = await redis.hgetall(ticker_key)
            
            if not get_data:
              return {}
            
          
            sorted_data ={k:get_data[k] for k in sorted(get_data)}
            
            return sorted_data
            
        except Exception as e:
            logging.error("Error getting Symbols data", exc_info=True)
            return {}
            
            
    async def get_featured_pairs(self):
        try:
            featured_pairs = {
                "EURUSD":"currency",
                "GBPUSD":"currency",
                "USDCAD": "currency"
                
            }
            
            pipeline = self.redis.pipeline()
            
            for pair,category in featured_pairs.items():
                key = f"overview:{category}:{pair}"
                pipeline.hgetall(key)
            
            data = pipeline.execute()
            paired_results = dict(zip(featured_pairs.keys(), data))
            
            return paired_results
        except Exception as e:
            logging.error("Error getting featured data", exc_info=True)
            
            
    async def get_equity_data(self):
        try:
            pass
            # Check if any equity is tored
            
            
        except Exception as e:
            logging.error("Error getting equity data", exc_info=True)
            
    async def get_economic_event(self, countries: str = "US, DE, GB, EU, HU, PL, CA, AU, NZ, JP, CH, SE, TR, NO, ZA, SG, MX"):
        try:
            # self.redis.delete("news:US:*")
            # keys_to_delete = list(self.redis.scan_iter("news:US:*"))
                
            # if keys_to_delete:
            #     self.redis.unlink(*keys_to_delete)
            # return
            start = datetime.strftime(datetime.now(), '%Y-%m-%d')
            prev_day = datetime.now() + relativedelta(days=7)
            
            end = prev_day.strftime('%Y-%m-%d')
            

            querystring = {"from":start,"to":end,"countries":countries}

            headers = {
                "x-rapidapi-key":self.secret.news_key,
                "x-rapidapi-host": "ultimate-economic-calendar.p.rapidapi.com",
                "Content-Type": "application/json"
            }

            response = requests.get(self.secret.news_api, headers=headers, params=querystring)
            
            # Parse request into Json
            news_data = response.json()
            
            if news_data.get("status") != "ok":
                logging.error("Error fetching news api 1", exc_info=True)
                return
            
            countries_list = countries.replace(" ",'').split(",")
            
            await self.store_event_parrallel(news_data.get('result', []), countries_list)
           
            
            return news_data
        except Exception as e:
            logging.error('Error getting Economice event',exc_info=True)
            
            
    def sanitize_redis_key(self,key: str) -> str:
        """Remove problematic characters from Redis keys"""
        # Replace spaces with underscores
        sanitized = key.replace(' ', '_')
        # Replace colons with something else (colons create hierarchy)
        sanitized = sanitized.replace(':', '_')
        # Remove other problematic characters
        sanitized = re.sub(r'[^\w\-_\.]', '', sanitized)
        return sanitized       
    
    
    async def store_event(self, country:str, events: list[dict] ):
        try:
            # 
            if not events:
                logging.info("No events to store")
                return
          
            pipeline = self.redis.pipeline()
            for event in events:
               
                event_id = event.get("id")
                if event_id:
                    ttl = event.get("expiration", 172800)
                    # expiry_time = datetime.now().timestamp() + ttl
                    safe_id= self.sanitize_redis_key(event_id)
                    key = f"news:{country}:{safe_id}"
                    # pipeline.zadd(key, {json.dumps(event): expiry_time})
                    pipeline.setex(key, event.get("expiration", 172800), json.dumps(event))
                    
            pipeline.execute()
        except Exception as e:
            logging.error("Error Storing single event",exc_info=True)
            
    async def store_event_parrallel(self, events: list[dict], countries:list)  :
        try:
            if not events:
                return
            
            ttl_tasks = [self.calculate_ttl(e.get("date")) for e in events]
            ttls = await asyncio.gather(*ttl_tasks)
            
            # Add expirations to events
            for event, ttl in zip(events, ttls):
                event["expiration"] = ttl
            
            country_groups = {}
            for event in events:
                country = event.get("country")
                if country:
                    if country not in country_groups:  # Initialize if not exists
                        country_groups[country] = []
                    country_groups[country].append(event)

                
            store_tasks =[
                self.store_event(country, country_event) for  country, country_event in country_groups.items()
            ]
            
            await asyncio.gather(*store_tasks)
        except Exception as e:
            logging.error("Error storing event by country")
            
    async def calculate_ttl(self, date_str: str) -> int:
        """Calculate TTL in seconds"""
        try:
            
            if not date_str:
                return 172800
            
            event_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            now = datetime.now().astimezone()
            
            
            if event_date > now:
                # Future event: keep until 2 days after
                seconds_until = (event_date - now).total_seconds()
                return int(seconds_until + 172800)  # 2 days after event
            else:
               
                return 3600 
        except Exception as e:
            logging.error(f"Error calculating TTL for {date_str}: {e}", exc_info=True)
            return 172800  # Default 2 days
        
        
    async def get_news_events(self, country):
        try:
            key = f"news:{country}:*"
           
            # keys = list(self.redis.scan_iter(match=key))
            keys = [k async for k in   self.aioredis.scan_iter(match=key)]
            
            print(keys)
            if not keys:
                return {}
            
            async with self.aioredis.pipeline(transaction=False) as pipeline:
                for k in keys:
                    pipeline.get(k)
                
                # 3. FIX: Await the pipeline execution
                results = await pipeline.execute()
        
            
         
            result_dict = {
                    k:  (v)
                    for k, v in zip(keys, results)
                }
            
        
           
            return result_dict
        except Exception as e:
            logging.error(f"Error in get news event",exc_info=True)

    async def symbol_snapshot(self,ticker:str, category:str):
        try:
            # Add a semaphore
            #Ticker pattern
            ticker_key = f"overview:{category}:{ticker}"
         
            # Get all ticker data
            async with self.aioredis as redis:
                ticker_data = await redis.hgetall(ticker_key)
            
            # Limit the keys to last year worth of data
            sorted_keys = set(sorted(ticker_data)[-252:])
       
            
            filter_ticker_data = {k: json.loads(v) for k,v in ticker_data.items() if k in sorted_keys}

            
            # Convert dict data to dataframe
            df = pd.DataFrame.from_dict(filter_ticker_data, orient='index')
            df.index = pd.to_datetime(df.index)
            df = df.sort_index(ascending=True)
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df['open'] = pd.to_numeric(df['open'], errors='coerce')
            df['close'] = df['close'].ffill()
            
            
            
            max_high = df['high'].max()
            max_low = df['high'].min()
            last_day_high = df.iloc[-1]['high']
            last_day_low = df.iloc[-1]['low']
            last_day_open = df.iloc[-1]['open']
            last_day_close = df.iloc[-1]['close']
            df['returns'] = df['close'].pct_change()
            last_day_pct_change = df.iloc[-1]['returns']
            rolling_30_deviation = df['returns'].rolling(window=30, min_periods=1).std() * 100
         
            # Return snapshot data
            data = {
                'max_high':round(max_high , 4),
                "max_low": round(max_low,4 ),
                'day_high': round(last_day_high,4),
                'day_low':round(last_day_low, 4),
                'day_open':round(last_day_open, 4),
                'day_close':round(last_day_close, 4),
                'vol_30':round(rolling_30_deviation.iloc[-1], 4),
                'pct_change': round(last_day_pct_change, 4)
            }
            
            return data
        except Exception as e:
            logging.error(f"Error getting symbol snapshot {e}", exc_info=True)

    
    async def symbol_correlation(self, ticker:str,category:str):
        try:
            
            # Empty list of records
            records = []

            # Get all keys for that category
            pattern = f"overview:{category}:*"
            keys = [k async for k in   self.aioredis.scan_iter(match=pattern)]
            
            if not keys:
                return {"target": ticker, "most_negative": [], "most_positive": []}
      
          
            # Get all data for the respective keys
            async with self.aioredis.pipeline() as pipeline:
               
                [ pipeline.hgetall(k) for k in keys ]
                task = await pipeline.execute()
            
            # Map all the data to their respective keys
            for key, data_dict in zip(keys,task):
                symbol = key.split(":")[-1]
                
                record={"symbol":symbol, **data_dict}
                
                records.append(record)
            
            # Create a helper function to offload taskd to thread
            def process_records(raw_records):
                df = pd.DataFrame(raw_records)
                df.set_index("symbol", inplace=True)
                new_df = df.T
                
                # function to extract close value from the row data
                def get_close_price(cell):
                    try:
                        if pd.isna(cell):
                            return np.nan
                        if isinstance(cell, str):
                            return json.loads(cell).get("close", np.nan)
                        if isinstance(cell, dict):
                            return cell.get("close", np.nan)
                        return np.nan
                    except Exception:
                        return np.nan
                    
                # This map all cells   
                prices_df = new_df.map(get_close_price)
                
                # Fills nan data
                final_prices_df = prices_df.ffill().bfill()
                
                # Sort in Ascending order
                final_prices_df=final_prices_df.sort_index(ascending=True)
               
                # Correlation matrix for the last 90 days
                corr_matrix = final_prices_df.tail(90).corr()
                ticker_corr = corr_matrix[ticker]
                
                # Drop ticker self correlation
                ticker_corr = ticker_corr.drop(ticker)
                
                # Get max and min correlation
                max_corr_idx = ticker_corr.nlargest(2)
                min_corr_idx = ticker_corr.nsmallest(2)
                
                print(min_corr_idx)
                
                # Get the value of the correlate valuse
                pos_clean = [{"symbol": idx, "value": round(max_corr_idx[idx], 4)} for idx in max_corr_idx.index]
                
                
                neg_clean = [{"symbol": idx, "value": round(min_corr_idx[idx], 4)} for idx in min_corr_idx.index]
                
                
                return {
                "target": ticker,
                "most_negative": neg_clean,
                "most_postive": pos_clean
                }

          
            
            results = await asyncio.to_thread(process_records,records)
            return results
        except Exception as e:
            logging.error(f"Error in calculating {ticker} in {category} calculation", exc_info=True)

    
    async def symbol_technical_signals(self, ticker:str,category:str):
        try:
            #Ticker pattern
            ticker_key = f"overview:{category}:{ticker}"
         
            # Get all ticker data
            async with self.aioredis as redis:
                ticker_data = await redis.hgetall(ticker_key)
            
            # Limit the keys to last year worth of data
            sorted_keys = set(sorted(ticker_data)[-90:])
       
            
            filter_ticker_data = {k: json.loads(v) for k,v in ticker_data.items() if k in sorted_keys}
            
            df = pd.DataFrame.from_dict(filter_ticker_data, orient='index')
            df.index = pd.to_datetime(df.index)
            df = df.sort_index(ascending=True)
            
            def calc_rsi(prices:pd.Series, period:int= 14):
                try:
                    
                    if len(prices)<= period:
                        return {}
                    
                    delta = prices.diff()
                    
                    # 2. Separate gains and losses, forcing negative values to 0
                    gain = delta.clip(lower=0)
                    loss = -delta.clip(upper=0)
                    
                    # 3. Calculate the exponential moving average using Wilder's alpha (1 / period)
                    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
                    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
                    
                    # 4. Calculate Relative Strength (RS)
                    rs = avg_gain / avg_loss
                    
                    # 5. Calculate RSI and handle division by zero (when loss is 0)
                    rsi = 100 - (100 / (1 + rs))
                    state = "Neutral"
                    if rsi.iloc[-1] > 75:
                        state= "Overbought"
                    if rsi.iloc[-1] < 25:
                        state= "Oversold"
                    
                    result = {
                        "rsi":state,
                        "value": rsi.iloc[-1]
                    }   
                        
                    return result
            
                    
                except Exception as e:
                    logging.error(f"Error calculating RSI {e}", exc_info=True)
                    return {}
                
            def atr(df:pd.DataFrame):
                try:
                    if df.empty: return {}
                    high_low = df['high'] - df['low']
                    high_prev_close = (df['high']-df['close'].shift(1)).abs()
                    low_prev_close = (df['low']-df['close'].shift(1)).abs()
                    
                    df['tr'] = pd.concat([high_low, high_prev_close, low_prev_close], axis=1).max(axis=1)
                    
                    
                    df['tr_mean'] = df['tr'].rolling(window=14).mean()
                    df['tr_std'] = df['tr'].rolling(window=14).std()
                    
                    df['z_score'] = (df['tr'] - df['tr_mean'])/df['tr_std']
                    
                    z_score = df.iloc[-1]['z_score']
                    atr = df.iloc[-1]['tr_mean']
                    
                    state = 'Neutral'
                    
                    if z_score > 1.5 : state = "High"
                    
                    if z_score <-1.0 :state = "Low"
                    
                    data ={
                        "atr":atr,
                        "state":state
                    }
                    
                    return data
                except Exception as e:
                    logging.error(f"Error calculating ATR {e}", exc_info=True)
                    return {}
            
            def ema_20(df:pd.DataFrame):
                try:
                    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
                    
                    last_price = df.iloc[-1]['close']
                    
                    state = "Bull"
                    if df.iloc[-1]['ema_20'] > last_price:
                        state = "Bear"
                        
                    data = {
                        'ema_20':df.iloc[-1]['ema_20'],
                        'state': state
                    }
                    return data
                except Exception as e:
                    logging.error(f"Error calculating EMA20 {e}", exc_info=True)
                    
          
            # Use multithreading
                
            with ThreadPoolExecutor(max_workers=5) as executor:
                loop = asyncio.get_running_loop()
                
                task1 = loop.run_in_executor(executor,atr, df[-90:])
                task2 = loop.run_in_executor(executor,calc_rsi, df.tail(90)['close'])
                task3 = loop.run_in_executor(executor,ema_20, df[-90:])
                
                result_corr, result_vol,result_ema= await asyncio.gather(task1, task2, task3)
            
            return  result_corr, result_vol, result_ema
        except Exception as e:
            logging.error(f"Error calculating technical signal {e}", exc_info=True)



# test = MarketOverview() 
      
# asyncio.run(test.get_currency_last_entry('AUDNZD',"overview:currency:"))        

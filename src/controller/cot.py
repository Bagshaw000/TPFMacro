"""CFTC Commitment of Traders (COT) data pipeline: fetches weekly COT
reports, derives net-position metrics per trader category (dealers, asset
managers, leveraged money, other reportables), caches them in Redis, and
computes percentage change over several trailing periods (1/3/6/12 months)
so the frontend can show positioning trends per instrument.

Uses BOTH a sync (`self.redis`) and an async (`self.aioredis`) Redis client
from the same RedisConnection - most methods here use the sync client even
though they're declared `async def`, which means those Redis calls block
the event loop for their duration rather than yielding to it. Only
`setup_redis` and `insert_cot_redis` consistently use the async client.
"""

import asyncio
from datetime import datetime, timedelta
import json
import logging
import math
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_types.cot import CotData
from database.redis_ import RedisConnection
from model.cot import CotModell
import pandas as pd
import numpy as np

class COTController:

    def __init__(self):
        # Sync client - used for quick lookups (keys/scan/hgetall) in most
        # methods below, even inside `async def` functions (see module
        # docstring: this blocks the event loop while it runs).
        self.redis = RedisConnection().get_redis()
        # Async client - used where the code actually awaits Redis calls
        # (insert_cot_redis, setup_redis).
        self.aioredis = RedisConnection().get_async_redis()
        self.cot = CotModell()

    #Get Cot data
    async def get_cot_data(self):
        """Return COT percentage-change data for every configured
        instrument, grouped by asset class (Currency, Indicies, Financial,
        Crypto).

        If Redis has no cached COT data yet (`check_cot` empty), first
        fetches and caches the last year of reports via
        `self.cot.get_all_last_year_cot()` + `insert_cot_redis`. Either way,
        reads back from Redis, converts to per-asset dicts, and computes
        the pct-change breakdown for each asset class.
        """
        try:
            # Check if data is stored in redis
            data_obj = dict()
            check_cot =   self.redis.keys("cot_ttf*")

            # self.redis.delete(*check_cot)

            # return
            # # Or clear all fields but keep the hash
            # # self.redis.hdel('cot_ttf', *self.redis.hkeys('cot_ttf*'))
            # return
            with open('data/instr.json', 'r') as f:
                data = json.load(f)

            for category, items in data.items():
                data_obj[category] = items

            data_keys = list(data_obj.keys())

            # Check if we have existing records not in redis
            if not check_cot:

                data_list = await self.cot.get_all_last_year_cot()


                # If batch data is not returned then stop operations
                if not data_list:
                    logging.info("data list is empty")
                    return

                # Update redis records
                insert_data = await self.insert_cot_redis(data_list)


                if not insert_data:
                    logging.info("Failed to update redis cot data")
                    return

            # If data is our redis convert data to dataframe for further processing
            # NOTE: data_keys[0..3] assumes data/instr.json always has
            # exactly these 4 categories in this order (Currency, Indicies,
            # Financial, Crypto per the `data` dict below) - if instr.json
            # ever gains/loses/reorders a category this indexing silently
            # breaks or maps the wrong category to the wrong asset class.
            currency_data,indices_data,financial_data,crypto_data =await asyncio.gather( self.new_covert_redis_dataframe(data_obj[data_keys[0]],data_keys[0]),self.new_covert_redis_dataframe(data_obj[data_keys[1]],data_keys[1]),self.new_covert_redis_dataframe(data_obj[data_keys[2]],data_keys[2]),self.new_covert_redis_dataframe(data_obj[data_keys[3]],data_keys[3]))

            # Ensure all asset class converts properly
            if not currency_data or not indices_data or not financial_data or not crypto_data:
                logging.info("Error in coverting to redis")
                return

            # Calculate the percentage change for all asset classes
            cur_pct,ind_pct,fin_pct,crypt_pct = await asyncio.gather(self.new_calculate_all_change(currency_data),self.new_calculate_all_change(indices_data),self.new_calculate_all_change(financial_data),self.new_calculate_all_change(crypto_data))


            data = {
                "Currency":cur_pct,
                "Indicies":ind_pct,
                "Financial":fin_pct,
                "Crypto":crypt_pct
            }

            return data


        except Exception as e:
            logging.error(f"Error getting data : {e}", exc_info=True)
            raise

    # !Incomplete
    # This function interpretes the pct change for all instruments
    async def interpret_pct_change(self, data:dict,asset_cls:str):
        """Unfinished: intended to have an LLM generate a human-readable
        explanation of each instrument's pct-change data, cached under
        "cot_expl:{asset_cls}:{instrument}:{date}" so it's only computed
        once per date. Currently only the cache-check/skip logic exists -
        the actual LLM call and result-append step are not implemented
        (see the trailing comments below the loop).
        """
        try:

            # Check if the recent LLM intepretation exists


            for instrument, instrument_data in data.items():
                *rest, last = instrument_data.keys()

                date_series = pd.to_datetime(rest).max().strftime('%Y-%m-%d')


                # Search if the recent explanation has been set
                instrument_key = f"cot_expl:{asset_cls}:{instrument}:{date_series}"
                check_exp = self.redis.hgetall(instrument_key)


                if check_exp:
                    # Just append explanation to the  dict object
                    continue

                # Loop through pct change and ask LLM to explain append that to the Object

            # Loop through the data and select 2 input for each field

        except Exception as e:
            logging.error("Error interpreting cot data")


    # Calculate the percentage change for a given period
    async def new_calculate_all_change(self, data:dict):
        """For every instrument in `data`, compute pct change of each net-
        position field (large_spec_net, commercial_net, dealer_net,
        asset_mgr_Net, lev_money_net, other_rept_net) over 1/3/6/12-month
        trailing windows (expressed in weekly COT reports: 4/12/24/52
        weeks), using the most recent non-null value forward-filled across
        gaps. Mutates and returns `data` with a `pct_change` key added to
        each instrument that had at least one computable field.
        """
        try:
            # These are the fields that are relevant to the task
            fields = ['large_spec_net', 'commercial_net', 'dealer_net',
                  'asset_mgr_Net', 'lev_money_net', 'other_rept_net']

            # Periods to call calculate pct change
            periods = {'1_month': 4, '3_month': 12, '6_month': 24, '1_year': 52}


            # Loop through all the instrument data
            for instrument, instrument_data in data.items():

                if not instrument_data:
                    continue

                # Sort the entry by the dates
                dates = np.array(sorted(instrument_data.keys()))

                # Check if there is enough data
                if len(dates) < 4:
                    continue

                instrument_results = dict()

                # Loop through all fields and
                for field in fields:
                    # Extract and forward fill values
                    values=list()
                    last_val = None

                    # Loop through all dates for the instrument
                    for d in dates:
                        # Get the value for the field for the coressponding date
                        val = instrument_data[d].get(field)

                        # Check if the value got is valid before converting to float
                        if val is not None and val !='':
                            try:
                                last_val= float(val)
                            except:
                                pass

                        # Append the value to an array
                        values.append(last_val)

                    # convert to np aray and automatically cast all values to a float
                    values = np.array(values,dtype=np.float64)

                    # Check if any value are nan and return
                    valid_mask = ~np.isnan(values)

                    # if in all values are nan is breaks
                    if not valid_mask.any():
                        continue

                    # Get most recent valid value where valid mask is True(This index is likely the most recent value)
                    last_idx = np.max(np.where(valid_mask)[0])
                    current_val = values[last_idx]
                    current_date = dates[last_idx]


                    field_results= dict()

                    # Loop through all the periods
                    for period_name, weeks in periods.items():
                        # For the number of weeks in the period calculate the target index
                        target_idx= last_idx - weeks

                        # Check that the target does not exceed 0 and there is value in that index
                        if target_idx >= 0 and valid_mask[target_idx]:

                            prev_val = values[target_idx]

                            # Check the previous value is not 0
                            if prev_val !=0:
                                # Calculate pct change for that period and round to d.p
                                pct = round(((current_val - prev_val) / abs(prev_val))*100, 2)

                                # Store the pct change for tha period with its dat
                                field_results[period_name] = {current_date:pct}

                    # field result not empty
                    if field_results:
                        # Store all period change to the particular field
                        instrument_results[field] = field_results
                #   Store the  pct result for all field to the instrument
                if instrument_results:
                    data[instrument]['pct_change'] = instrument_results

            return data
        except Exception as e:
            logging.error(f"Error getting data : {e}", exc_info=True)

    # Convert redis to dataframe
    async def new_covert_redis_dataframe(self, asset_list:list, asset_cls:str):
        """For every asset in `asset_list`, scan Redis for that asset's
        weekly "cot_ttf:{asset_cls}:{asset}:*" hashes, keep only the most
        recent 53 (~1 year of weekly COT reports), and batch-read them into
        `{date: hash_data}` dicts. Returns `{asset: {date: hash_data}}` for
        every asset that had at least one stored entry.
        """
        try:
            # Get all patterns
            patterns = [f"cot_ttf:{asset_cls}:{asset}:*" for asset in asset_list]

            async def get_asset_data(asset,pattern):
                # Collect All keys for the asset
                # NOTE: uses the sync client's scan_iter/pipeline (not
                # self.aioredis) despite being declared async - see module
                # docstring.

                all_keys = list()
                cursor = 0
                for key in self.redis.scan_iter(match=pattern,count=1000):
                    all_keys.append(key)


                if not all_keys:
                    return None

                # Sort all keys in revers order to get the most recent entries
                all_keys.sort(reverse=True)

                # Get the most recent 53 entries (COT reports are weekly,
                # so 53 entries covers roughly the trailing year - matches
                # the longest period (1_year: 52 weeks) new_calculate_all_change needs)
                recent_keys = all_keys[:53]

                pipeline= self.redis.pipeline()

                # Batch process all the data from with recent keys
                for key in recent_keys:
                    pipeline.hgetall(key)

                results = pipeline.execute()


                temp_dict = dict()
                for key, hash_data in zip(recent_keys, results):

                    # Check if the data for the corresponding key
                    if hash_data:
                        date_key = key.split(":")
                        temp_dict[date_key[-1]] = hash_data

                # Return the asset and asset data if any exist else return None
                return asset,temp_dict if temp_dict else None

            # Get assest data parrellelly by batch processing data
            tasks = [get_asset_data(asset,pattern) for asset,pattern in zip(asset_list, patterns)]
            results = await asyncio.gather(*tasks)


            # Map all assets to its corresponding data
            df = dict()
            for result in results:
                if result is not None:
                    asset, data = result
                    df[asset] = data
            return df

        except Exception as e:
            logging.error(f"Converting redis to dataframe", exc_info=True)


    # This insert the cot_ttf into the redis
    async def insert_cot_redis(self, data:list[CotData]):
        """Convert fetched CotData records into net-position metrics per
        trader category, then cache each row in Redis as a hash keyed
        "cot_ttf:{market}:{market_and_exchange_names}:{report_date}", with
        a TTL of ~60 weeks from that report's date (get_ttl_until_60_weeks).

        NOTE: `batch_size`/`batch` (below) are computed as if this loops
        over the dataframe in chunks, but the inner loop actually iterates
        `new_df.iterrows()` (the WHOLE dataframe) every time through the
        outer `for i in range(0, len(new_df), batch_size)` loop, not
        `batch.iterrows()` - so `batch` is unused and every row gets
        processed/written len(new_df)/batch_size times over, not once.
        """
        try:
            # pass
            batch_size = 1000


            df = pd.DataFrame.from_records([r.__dict__ for r in data])

            # Remove 'id' and 'market' if you don't need them
            df = df.drop(columns=['_sa_instance_state','id'], errors='ignore')

            # Reverse to chronological order (oldest first)

            df["report_date_as_yyyy_mm_dd"]= df["report_date_as_yyyy_mm_dd"]


            # Example calculations
            df['dealer_net'] = df['dealer_positions_long_all'] - df['dealer_positions_short_all']
            # Positive = Dealers are net long (could be bearish signal - hedging client selling)
            # Negative = Dealers are net short (could be bullish signal - hedging client buying)

            df['asset_mgr_net'] = df['asset_mgr_positions_long_all'] - df['asset_mgr_positions_short_all']
            # Positive = Institutions bullish (trend-following)
            # Negative = Institutions bearish (trend-following)

            df['lev_money_net'] = df['lev_money_positions_long_all'] - df['lev_money_positions_short_all']
            # Positive = Hedge funds/CTAs bullish (often crowded - contrarian signal at extremes)
            # Negative = Hedge funds/CTAs bearish (often crowded - contrarian signal at extremes)

            df['other_rept_net'] = df['other_rept_positions_long_all'] - df['other_rept_positions_short_all']


            df['commercial_net'] = df['dealer_net'] + df['other_rept_net']
            # Positive = Commercials net long (smart money bullish)
            # Negative = Commercials net short (smart money bearish)

            df['large_spec_net'] = df['asset_mgr_net'] + df['lev_money_net']
            # Positive = Speculators bullish (sentiment indicator)
            # Negative = Speculators bearish (sentiment indicator)

            new_df = df[["market_and_exchange_names","market","report_date_as_yyyy_mm_dd","dealer_net", 'dealer_positions_long_all','dealer_positions_short_all','asset_mgr_net','asset_mgr_positions_long_all', 'asset_mgr_positions_short_all','commercial_net','lev_money_positions_long_all','lev_money_positions_short_all','large_spec_net','other_rept_net',"open_interest_all"]]

            # Batch execute the insert functionality
            for i in range(0, len(new_df), batch_size):
                batch = new_df.iloc[i:i+batch_size]
                pipe = self.aioredis.pipeline()
                for index, row in new_df.iterrows():
                    # Get temporary data
                    tmp_data:dict = row.to_dict()
                    tmp_data["report_date_as_yyyy_mm_dd"]= datetime.strftime(row["report_date_as_yyyy_mm_dd"], "%Y-%m-%d")

                    # Get the expiration date
                    expiration_date = await self.get_ttl_until_60_weeks(datetime.strftime(row["report_date_as_yyyy_mm_dd"], "%Y-%m-%d"))
                    row_date = datetime.fromisoformat(datetime.strftime(row["report_date_as_yyyy_mm_dd"], "%Y-%m-%d"))

                    key = f"cot_ttf:{row["market"]}:{row["market_and_exchange_names"]}:{row_date.date()}"
                    pipe.hset(key, mapping=tmp_data)
                    pipe.expire(key,time= expiration_date)


                exec = await pipe.execute()

            return exec


        except Exception as e:
            logging.error(f"Error inserting into redis data : {e}",exc_info=True)
            raise

    # Get the expiration for 60 weeks in seconds
    async def get_ttl_until_60_weeks(self,reference_date_str, format="%Y-%m-%d"):
        """
        Returns the number of seconds from the current moment until
        the date that is 60 weeks after the provided reference_date.
        """
        # Parse the reference date
        ref_date = datetime.strptime(reference_date_str, format)

        # Calculate the future expiration date (60 weeks later)
        expire_date = ref_date + timedelta(weeks=60)

        # Get the current time
        now = datetime.now()

        # Calculate the TTL in seconds
        ttl_seconds = int((expire_date - now).total_seconds())

        # Return 0 if the expiration date is in the past
        return max(0, ttl_seconds)


    # Unimplemented stub - always a no-op.
    async def cot_asset_position(self, asset_name:str, asset_cls:str):
        try:
            # check if asset data exist in redis
            pass


        except Exception as e:
            logging.error(f"Error calculating asset position")
            raise

    # Incomplete: `all_keys` is declared but scan_iter's results are never
    # appended to it (unlike the equivalent loop in
    # new_covert_redis_dataframe.get_asset_data), so `keys` is unused and
    # `print(all_keys)` always prints an empty list. Nothing is returned.
    async def get_asset_year(self, asset:str, asset_cls:str ):
        try:
                    # check if asset data exist in redis
            pipeline = self.aioredis.pipeline()
            key = f"cot_ttf:{asset_cls}:{asset}:*"
            all_keys = list()
            cursor = 0
            keys = self.redis.scan_iter(match=key,count=1000)

            print(all_keys)


        except Exception as e:
            logging.error(f"Error calculating asset position")
            raise

    # This setup redis to ensure data is coherent for database and redis
    async def setup_redis(self):
        """Idempotent Redis warm-up: if "cot_status" isn't already set to 1,
        either populate Redis from scratch (no cot_ttf* keys yet) or clear
        and repopulate it (stale keys present), then mark "cot_status" = 1
        so subsequent calls skip straight to the "already updated" log line.
        """
        try:
            cot_status =  await  self.aioredis.get("cot_status")

            # Check if the status updated
            if cot_status != 1:
                check_cot =   self.redis.keys("cot_ttf*")
                print(check_cot)

                if check_cot ==[]:
                    data_list = await self.cot.get_all_last_year_cot()

                    # If batch data is not returned then stop operations
                    if not data_list:
                        logging.info("data list is empty")
                        return

                    # Insert redis records
                    await self.insert_cot_redis(data_list)

                else:
                    clear_cache = await self.aioredis.delete(*check_cot)

                    print(clear_cache)
                    # Ensure the all data in cot_ttf is deleted
                    if clear_cache != 0:

                        data_list = await self.cot.get_all_last_year_cot()

                        # If batch data is not returned then stop operations
                        if not data_list:
                            logging.info("data list is empty")
                            return

                        # Insert redis records
                        await self.insert_cot_redis(data_list)

                await  self.aioredis.set("cot_status",1)
                logging.info(f"Redis has been updated")

            logging.info(f"Redis is already updated")

        except Exception as e:
            logging.error(f"Error setting up redis: {e}", exc_info=True)
            raise


# test = COTController()
# val = asyncio.run(test.setup_redis())

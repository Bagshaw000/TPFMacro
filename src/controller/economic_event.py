"""Economic-calendar events sourced from yfinance, cached in Redis.

Unlike controller/lse_.py (which persists to Postgres via the LSE API),
this controller only ever reads from yfinance and writes to Redis - there's
no database table backing it. Data flow:

1. get_economic_event() - pulls yfinance's economic events calendar, keeps
   only the countries we care about.
2. store_economic_event() - computes a per-event TTL (via
   MarketOverview.calculate_ttl) and writes each event as its own Redis
   hash, keyed "news:{country_code}:{event}", set to expire at that TTL.
3. get_all_events() / get_event_country() - scan Redis for stored "news:*"
   keys, read each hash back, and return them grouped by country and
   sorted by event_time.
"""

import asyncio
from collections import defaultdict
import json
import logging
from typing import Awaitable, List, cast
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_types.cpi import EconomicEventType
from database.redis_ import RedisConnection
from model.market_overview import MarketOverview

# Countries this controller tracks, in yfinance's own 2-letter region codes
# (not the 3-letter codes used elsewhere in this codebase, e.g. lse_.py).
country=["US","CA","JP","DE","UK","AU","IN","CN","KR","BR","FR"]

class EconomicEventController:

    def __init__(self):
        self.redis = RedisConnection().get_async_redis()
        self.mko = MarketOverview()

    #  This function get ecnomic events from yfinance and filters them  by countries
    async def get_economic_event(self,country=["US","CA","JP","DE","UK","AU","IN","CN","KR","BR","FR"]):
        """Pull yfinance's economic events calendar and keep only the rows
        for the requested countries, mapped into EconomicEventType.

        Returns None if nothing matched (checked by callers before use).
        """
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
        """Fetch the current calendar and cache each event in Redis as a
        hash, with a per-event TTL so stale/past events expire on their own
        instead of needing an explicit cleanup pass.
        """
        try:
            # Get events
            event = await self.get_economic_event()

            if event == None:
                return

            pipeline = self.redis.pipeline()

            # Get expiration time for all event
            # calculate_ttl is async, so this builds a list of coroutines
            # and awaits them all together via gather rather than one at a
            # time - only events with a real datetime event_time get a TTL
            # computed; the rest are skipped by the isinstance guard.
            ex_time = [self.mko.calculate_ttl(e.event_time.strftime('%Y-%m-%d %H:%M:%S%z')) for e in event  if isinstance(e.event_time, datetime)]
            ttls = await asyncio.gather(*ex_time)

            # Zip the event expiration time
            # NOTE: this zips `event` (every event) against `ttls` (only the
            # subset that had a datetime event_time) positionally - only
            # correct as long as every event in `event` actually has a
            # datetime event_time, since zip stops at the shorter sequence
            # and would otherwise silently pair the wrong ttl with the
            # wrong event.
            for e, ttl in zip(event, ttls):

                e.expiration = ttl


            # Store event in according to country
            for e in event:
                if isinstance(e.event_time, datetime):
                    # Redis hash fields must be strings - stringify the
                    # datetime before model_dump() below serializes it.
                    e.event_time = e.event_time.isoformat()

                key = f"news:{e.country_code}:{e.event}"
                pipeline.hset(name= key, mapping=e.model_dump())

                # Fall back to 2 days (in seconds) if calculate_ttl didn't
                # produce one for this event (e.g. event_time wasn't a
                # datetime, so it was skipped above and expiration is None).
                expiration = e.expiration or 172800
                pipeline.expire(name= key,time=expiration)

            # Execute  redis pipeline
            event_set = await pipeline.execute()


        except Exception as e:
            logging.error(f"Error storing economic event: {e}", exc_info=True)
            raise

    async def get_all_events(self):
        """Read back every cached "news:*" event from Redis, grouped by
        country and sorted chronologically by event_time.

        Uses SCAN (via the cursor loop below) instead of KEYS to walk the
        keyspace without blocking Redis, same reasoning as lse_.py's use of
        scan_iter - the manual cursor loop here does the same thing at a
        lower level.
        """
        try:
            # key = "news:*"
            # events = await self.redis.hgetall(name=key)
            # print(events)
            keys = []
            cursor = 0
            global country

            while True:
                cursor, batch = await self.redis.scan(cursor, match="news:*",count=100)
                # batch is a list of matching keys from this scan iteration;
                # appended as a whole list (not extended) so `keys` ends up
                # as a list-of-lists, flattened below via sum(keys, []).
                keys.append(batch)

                if cursor == 0:
                    # SCAN's cursor comes back to 0 once the full keyspace
                    # has been walked - that's the loop's exit condition,
                    # not an error state.
                    break


            flattened_key:List[str] = sum(keys,[])
            # print(flattened_key)
            events = defaultdict(dict)

            for key in flattened_key:
                split_key = key.split(":")

                # Get all the redis data fro all keys
                # hgetall's stub return type (Awaitable[dict] | dict) is
                # shared between redis-py's sync and async clients; cast
                # narrows it since self.redis is always the async client
                # here, so this call always actually returns an awaitable.
                data = await cast(Awaitable[dict], self.redis.hgetall(key))

                # Ensure the time data is in datetime format for sorting
                if isinstance(data["event_time"], str):
                    data["event_time"] = datetime.strptime(data["event_time"], '%Y-%m-%dT%H:%M:%S%z')

                # Filter for selected countries
                if split_key[1] in country:
                    # split_key[1] is the country_code segment of
                    # "news:{country_code}:{event}"; keyed by the parsed
                    # datetime here so the dict-comprehension sort below
                    # can sort chronologically before re-stringifying it.
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
        """Same as get_all_events(), but scoped to a single country by
        scanning only "news:{country}:*" instead of every "news:*" key -
        the country filter that get_all_events applies in Python is
        unnecessary here since the SCAN pattern itself already narrows to
        one country.
        """
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
                # hgetall's stub return type (Awaitable[dict] | dict) is
                # shared between redis-py's sync and async clients; cast
                # narrows it since self.redis is always the async client
                # here, so this call always actually returns an awaitable.
                data = await cast(Awaitable[dict], self.redis.hgetall(key))

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

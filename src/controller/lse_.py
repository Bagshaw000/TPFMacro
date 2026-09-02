"""LSE (London Stock Exchange data feed) economic-calendar sync pipeline.

Overall data flow, top to bottom:

1. get_event_cal() - entry point. For each tracked event type ("cpi", "ppi",
   "unemp", "retail", "inflation"), asks the DB for the most recent stored
   report per country (LSEModel.get_last_report). Event types are split into
   two groups: ones with existing rows (cal_tasks - only need a catch-up
   fetch) and ones with none yet (empty_cal_tasks - need a full historical
   backfill).
2. calendar_history() - for each event type, calls the external LSE API
   (self.lse_client.economic_calendar) to pull raw calendar rows, either
   from 2022-01-01 (first-time backfill) or from the day after each
   country's last known report_date (catch-up).
3. process_event() - filters the raw API rows down to just the "plain
   monthly release" for that event type (see MONTHLY_EVENT_PATTERNS below),
   maps each row into the correct SQLModel table class, and converts the
   raw actual/forecast strings into percentages.
4. LSEModel.insert_event() (in model/lse.py) - upserts the built records
   into Postgres.
5. insert_redis() - caches each country's latest reading in Redis
   (`{table}:{country_code}`) plus a cross-country average of only the
   countries whose latest reading is for the most recent month
   (`{table}:avg`).
"""

import asyncio
from datetime import datetime, timedelta
import json
import os
import sys
from typing import List
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import get_doppler_env
from custom_types.cpi import ECON_INDICATOR_TYPES, _EconIndicatorWithForecast, CPIType, INFType, LSEResponseType, PPIType, RETAILType, UNEMPType, countries, country_mapping, get_country_code, get_key, months


def _period_from_hint(period_hint: str, release_date: datetime) -> str:
    """Reference month a reading describes, as "YYYY-MM".

    `period_hint` is a bare month token ("DEC"); the year is inferred from
    `release_date` (the publication date). A release is always published in or
    after its reference month, so if the hint month is *after* the release
    month it must belong to the previous year (e.g. published 2026-01-05 with
    hint "DEC" -> 2025-12).
    """
    hint_month = months.index(period_hint.upper()) + 1   # 1..12
    year = release_date.year
    if hint_month > release_date.month:
        year -= 1
    return f"{year:04d}-{hint_month:02d}"
from database.redis_ import RedisConnection
from model.lse import LSEModel
from lse import LSE
import logging
logger = logging.getLogger(__name__)


def _parse_percentage(value: str | None, previous: str | None) -> float | None:
    """Parse an LSE calendar figure as a percentage.

    If `value` is already a percentage (trailing '%'), parse it directly.
    Otherwise it's a raw level, so derive the percentage change from
    `previous` instead of returning the raw value.
    """
    if value is None:
        return None
    value = value.strip()
    if value.endswith("%"):
        try:
            return float(value[:-1])
        except ValueError:
            return None

    try:
        current = float(value)
    except ValueError:
        return None

    if previous is None:
        return None
    previous = previous.strip()
    try:
        prev = float(previous[:-1]) if previous.endswith("%") else float(previous)
    except ValueError:
        return None
    if prev == 0:
        return None

    return (current - prev) / prev * 100

# Canonical event/table names this pipeline tracks. These match both
# ECON_INDICATOR_TYPES' keys (custom_types/cpi.py) and each table's actual
# name in the database - every event string flowing through this file
# (get_last_report's `table`, calendar_history's `event`, process_event's
# `event`) is expected to be one of these, not the LSE API's own short
# codes ("inf", "ret") which only exist at the API-call boundary.
events_ = ["cpi", "retail", "unemp", "ppi", "inflation"]

# Exact token sequence (excluding the trailing month, e.g. "DEC") that
# element_data.event.split(" ") must match precisely for a row to count as
# that event's plain monthly release. Strict equality on purpose: variants
# like "Harmonised Inflation Rate MoM" are a different methodology (HICP)
# from the country's own headline "Inflation Rate MoM" and must NOT be
# treated as the same indicator, so a loose "contains these tokens" check
# is wrong here - the prefix has to be absent, not just present.
# Verified against real LSE payloads. unemp has no MoM/YoY suffix at all
# (e.g. "Unemployment Rate OCT" is already the monthly reading).
# TODO: gdp isn't verified yet - add its real event-name pattern here
# before relying on data for that table.
MONTHLY_EVENT_PATTERNS: dict[str, list[str]] = {
    "inflation": ["Inflation", "Rate", "MoM"],
    "unemp": ["Unemployment", "Rate"],
    "cpi": ["CPI", "MoM"],
    "ppi": ["PPI", "MoM"],
    "retail": ["Retail", "Sales", "MoM"],
}
class LSEController:

    global country_mapping, countries, months, events_


    def __init__(self):
            # Shared async Redis client (connection-pooled, see database/redis_.py)
            self.redis = RedisConnection().get_async_redis()
            # DB-facing layer: reads existing rows, upserts new ones (model/lse.py)
            self.lse = LSEModel()
            # Wrapper around the external LSE economic-calendar API
            self.lse_client = LSE(api_key=get_doppler_env().lse_key)

    async def get_event_cal(self):
        """Entry point: sync every tracked event type from the LSE API into
        Postgres, then refresh each event type's Redis cache.

        Runs two independent batches concurrently within each branch:
        - cal_tasks: event types that already have at least one stored row
          per country - only need a catch-up fetch (recent releases only).
        - empty_cal_tasks: event types with nothing stored yet - need a full
          historical backfill (calendar_history's start='2022-01-01' path).
        Both branches follow the same fetch -> DB upsert -> Redis cache
        pipeline; they're just two different starting states.
        """
        try:
            global events_

            # For every tracked event type, ask the DB for the most recent
            # report already on file per country (see the `_recent` SQL
            # views this hits, one per table, inside get_last_report).
            tasks = [asyncio.create_task(self.lse.get_last_report(i)) for i in events_]

            get_recent = await asyncio.gather(*tasks)

            # get_last_report returns (table_name, rows). Split into event
            # types that have existing data (cal_tasks) vs none yet
            # (empty_cal_tasks), since each needs a different fetch strategy
            # in calendar_history (catch-up vs full backfill).
            cal_tasks = [ task for task in get_recent if task[1]]
            empty_cal_tasks = [ task for task in get_recent if not task[1]]

            if cal_tasks:
                # Catch-up path: fetch only new releases since each
                # country's last known report_date, then persist them.
                event_tasks =[asyncio.create_task(self.calendar_history(
                    event=element[0].lower(),
                    recent_event=element[1]
                    )
                    )
                for element in cal_tasks]
                get_tasks = await asyncio.gather(*event_tasks)
              
                insert_db_tasks = [
                                    asyncio.create_task(self.lse.insert_event(element)
                                        )
                                    for element in get_tasks
                                ]

                insert_db = await asyncio.gather(*insert_db_tasks)

                # Refresh Redis's per-country + average cache with whatever
                # actually got upserted (insert_db holds the deduped records
                # LSEModel.insert_event confirmed were sent to Postgres).
                insert_redis_task = [
                                    asyncio.create_task(self.insert_redis(element)
                                        )
                                    for element in insert_db
                                ]

                insert_redis = await asyncio.gather(*insert_redis_task)

            if empty_cal_tasks:
                # Backfill path: no rows exist yet for this event type, so
                # calendar_history pulls the full history from 2022-01-01
                # for every country instead of just a catch-up window.
                event_tasks = [
                    asyncio.create_task(self.calendar_history(
                        event=element[0].lower(),
                        recent_event=element[1]
                        )
                        )
                    for element in empty_cal_tasks
                ]

                get_tasks = await asyncio.gather(*event_tasks)

                insert_db_tasks = [
                                    asyncio.create_task(self.lse.insert_event(element)
                                        )
                                    for element in get_tasks
                                ]

                insert_db = await asyncio.gather(*insert_db_tasks)

                insert_redis_task = [
                                    asyncio.create_task(self.insert_redis(element)
                                        )
                                    for element in insert_db
                                ]

                insert_redis = await asyncio.gather(*insert_redis_task)


        except Exception as e:
            logger.error(f"Error getting event calendar: {e}", exc_info=True)
            raise e

    async def calendar_history(self, event:str,  recent_event: List[_EconIndicatorWithForecast] | None = None):
        """Fetch raw calendar rows from the LSE API for one event type, then
        hand them to process_event() to filter/parse/build DB-ready records.

        `event` is always the canonical table name (e.g. "inflation",
        "retail") - both branches below translate it to the LSE API's own
        short event code (`api_event`) only at the point of calling
        self.lse_client.economic_calendar; everything else in this method,
        and everything downstream, keeps using the canonical `event`.

        Two branches depending on whether we already have data:
        - recent_event == [] (or omitted): no prior data for this event
          type - fetch every country's full history from 2022-01-01.
        - recent_event has entries: fetch only what's new since each
          country's last known report_date (report_date + 1 day), avoiding
          re-fetching (and re-processing) an already-stored release.
        """
        try:


            if recent_event is None:
                recent_event = []
            # Get the last
            # Get today's date
        

            today = datetime.now().date()

            # event is the canonical table/DB name (e.g. "inflation", "retail");
            # the LSE API expects its own short event codes, so translate only
            # for the outbound client call.
            event_db = {"inf":"inflation", "ret":"retail"}
            api_event = get_key(event, event_db) or event


                            # Get date 7 days from today
            date_7_days_from_now = today + timedelta(days=7)
            date_yyyy_mm_dd = date_7_days_from_now.strftime("%Y-%m-%d")

            if  recent_event == []:
                # ✅ More readable - Separate lines
                # First-time backfill: no stored data for this event type
                # yet, so pull every tracked country's full history.
                tasks = [

                        self.lse_client.economic_calendar(
                            get_country_code(element),
                            event=api_event,
                            start='2022-01-01',  # Or whatever start date you want
                            end=date_yyyy_mm_dd,  # 7 days from today
                            order='desc'
                        )

                    for element in countries
                ]

                event_proc = await self.process_event(event, tasks)

                return  event_proc


            # Catch-up: for each country's last known record, only fetch
            # releases published after it (start = last report_date + 1
            # day). This avoids re-pulling (and re-inserting) a release
            # that's already stored.
            tasks = [

                    self.lse_client.economic_calendar(
                        get_country_code(element.country_code),
                        event=api_event,
                        start=(element.report_date.date() + timedelta(days=1)).strftime("%Y-%m-%d"),  # Or whatever start date you want
                        end=date_yyyy_mm_dd,  # 7 days from today
                        order='desc'
                    )

                for element in recent_event
            ]
            # get_recent = await asyncio.gather(*tasks)

            event_proc = await self.process_event(event, tasks)

            return  event_proc




        except Exception as e:
            logger.error(f"Error getting event calendar history: {e}", exc_info=True)
            raise e

    async def process_event(self, event:str, event_list: List[List]) -> List[_EconIndicatorWithForecast]:
        """Filter raw LSE calendar rows down to one event type's plain
        monthly release, and build them into DB-ready model instances.

        `event_list` is a list of per-country row-lists (one inner list per
        `economic_calendar()` call in calendar_history). For each raw row:
          1. Skip anything that isn't in `events_` at all (shouldn't happen
             given how this is always called, but guards against a bad
             event string reaching here).
          2. Skip rows that don't match this event type's exact monthly
             pattern (MONTHLY_EVENT_PATTERNS) - filters out YoY/Core/
             Harmonised variants and non-monthly releases.
          3. Skip rows whose region_code isn't in country_mapping (no way
             to know the 3-letter country_code to store).
          4. Skip rows where the parsed index_value comes out None (not yet
             released, or unparseable) - index_value is NOT NULL in the DB.
          5. Build a concrete model instance (e.g. UNEMPType, CPIType) via
             ECON_INDICATOR_TYPES[event] and append it to the result list.
        """
        try:
            global events_, months
         

            if event not in events_:
                logger.warning(f"Skipping unrecognized event type: {event}")
                return []

            model_type = ECON_INDICATOR_TYPES[event]
            expected_pattern = MONTHLY_EVENT_PATTERNS.get(event)
            data_list = []
            for country in event_list:
             
                for element in country:
                    element_data = LSEResponseType(**element)
                    event_type = element_data.event.split(" ")
                    period_hint = str(element_data.period_hint).upper() if element_data.period_hint else None
                    # Some sources embed the period in the event name
                    # (e.g. "Unemployment Rate OCT"), others don't and
                    # carry it only in period_hint (e.g. "Unemployment
                    # Rate" with period_hint="FEB") - only strip the
                    # trailing token when it actually duplicates
                    # period_hint, don't assume it's always there.
                    tokens = event_type[:-1] if event_type and event_type[-1].upper() == period_hint else event_type

                    if (
                        expected_pattern is not None
                        and tokens == expected_pattern
                        and period_hint in months
                    ):
                        
                        country_code_ = country_mapping.get(element_data.region_code)
                        if country_code_ is None:
                            logger.warning(f"Skipping unmapped region code: {element_data.region_code}")
                            continue
                        forecast = _parse_percentage(element_data.actual, element_data.previous)
                        if forecast is None:
                            # Not yet released (future calendar entry with no
                            # `actual` printed) or unparseable - index_value
                            # is NOT NULL in the DB, so there's nothing to insert.
                            continue
                        lse_forecast = _parse_percentage(element_data.forecast, element_data.previous)
                        release_date = datetime.strptime(element_data.date, "%Y-%m-%d")
                        data = model_type(country_code=country_code_,
                                           freq="M",
                                           report_date=release_date,
                                           # Reference month, e.g. "2025-12" - the
                                           # upsert key. period_hint is guaranteed
                                           # to be in `months` by the filter above.
                                           period=_period_from_hint(period_hint, release_date),
                                           index_value=forecast, lse_forecast=lse_forecast)

                        data_list.append(data)
          
            return data_list
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            raise

    async def insert_redis(self, data: List[_EconIndicatorWithForecast]):
        """Refresh this event type's Redis cache from `data` plus whatever
        is already stored, and return the cross-country average.

        Two separate things get cached:
        - `{table}:{country_code}` - each country's single most recent
          record from `data` (deduped via most_recent_per_country, keeping
          the newest report_date per country).
        - `{table}:avg` - the average index_value across only the countries
          whose *currently stored* latest record (re-read from Redis via
          scan_iter/mget, not just this batch's `data`) falls in the most
          recent year-month seen. This deliberately re-reads the full
          per-country state rather than just averaging `data`, since a
          given run of get_event_cal may only have fetched new releases for
          a handful of countries - the average should reflect everyone's
          latest known reading, and "up to date" excludes any country still
          stuck on an older month so a stale country can't drag the average
          down (or skew it) alongside genuinely current ones.
        """
        try:
            if not data:
                return None

            # Each record's own concrete class already knows its table name
            # (e.g. "unemp", "cpi") - use that directly as the redis key
            # prefix instead of re-deriving it from a lookup table.
            key = data[0].__class__.__tablename__

            # Sort so that, per country, the newest report_date comes first
            # (used below by setdefault to keep only the latest per country).
            sorted_list = sorted(data, key=lambda d: (d.country_code, d.report_date), reverse=True)

            most_recent_per_country: dict[str, _EconIndicatorWithForecast] = {}
            for record in sorted_list:
                # setdefault only inserts on the first time a country_code is
                # seen; since sorted_list is newest-first per country, that
                # first occurrence is always the most recent record.
                most_recent_per_country.setdefault(record.country_code, record)



            most_recent_list = list(most_recent_per_country.values())

            # Cache each country's latest record as its own key.
            pipeline = self.redis.pipeline()
            for recent in most_recent_list:
                pipeline.set(f"{key}:{recent.country_code}", json.dumps(recent.model_dump(mode="json")))

            await pipeline.execute()

            # Re-read the FULL current per-country state for this table
            # (not just what we just wrote) so the average below reflects
            # every country ever cached, not only the ones touched by this
            # particular run's `data`. Exclude the "{key}:avg" key itself.
            country_keys = [
                redis_key
                async for redis_key in self.redis.scan_iter(f"{key}:*")
                if redis_key != f"{key}:avg"
            ]

            raw_values = await self.redis.mget(country_keys) if country_keys else []
            records = [json.loads(v) for v in raw_values if v is not None]

            # "Up to date" = the country's latest stored reading is for the
            # same reference month as the most recent one across all countries.
            # Prefer the `period` column (the true reference month); fall back
            # to slicing the ISO report_date ("YYYY-MM-DDTHH:MM:SS"[:7]) for any
            # record cached before the period column existed.
            periods = [
                (record, record.get("period") or (record["report_date"][:7] if record.get("report_date") else None))
                for record in records
            ]
            periods = [(record, period) for record, period in periods if period]
            if periods:
                latest_period = max(period for _, period in periods)
                up_to_date_records = [record for record, period in periods if period == latest_period]
            else:
                up_to_date_records = []

            index_values = [r["index_value"] for r in up_to_date_records if r.get("index_value") is not None]
            avg_value = sum(index_values) / len(index_values) if index_values else None

            await self.redis.set(f"{key}:avg", json.dumps(avg_value))

            return avg_value

        except Exception as e:
            logger.error(f"Error inserting into redis: {e}", exc_info=True)
            raise


# Guarded so importing this module (e.g. from another controller) doesn't
# trigger a live run against the real LSE API / DB / Redis as a side effect.
# if __name__ == "__main__":
#     test = LSEController()
#     asyncio.run(test.get_event_cal())

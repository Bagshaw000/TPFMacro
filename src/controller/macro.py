"""Cross-indicator "macro" aggregation reads, mostly backed by Redis, plus
an economic-cycle calculator built on top of it.

Every other controller (lse_.py, cpi.py, ppi.py, unemp.py, gdp.py,
economic_event.py's sentiment side, etc.) is the one that *writes* into
Redis - per-country latest readings at `{macro}:{country_code}` and the
cross-country average at `{macro}:avg` (see lse_.py's insert_redis), plus
news sentiment at `sentiment_news:{country}`. This controller mostly just
reads those already-cached keys back out, in a few different shapes:

- get_global_avg()   -> {macro: avg_value} across all tracked countries
- get_global_stats() -> {country: {macro: value, ..., new_sentiment}} for
                         every tracked country
- get_country_stats()-> {macro: value, f"{macro}_date": ..., new_sentiment}
                         for one specific country

The one place this file *does* talk to Postgres is the trailing-stats
pipeline that feeds the economic-cycle calculator below:

- refresh_factor_stats() -> recomputes each factor's trailing (mu, sigma)
                             per country from Postgres (via LSEModel) and
                             caches it in Redis at `{macro}:stats:{country}`
                             (meant to run on a periodic cron, see
                             worker.py, not on every request)
- get_factor_stats()     -> reads that cached (mu, sigma) baseline back for
                             one country
- get_economic_cycle()   -> classifies one country's growth/inflation
                             quadrant and trend, using get_country_stats()
                             (latest readings) and get_factor_stats()
                             (baseline) as inputs - see the module-level
                             _z_score/_compute_composite/_classify_*
                             helpers below for the actual math
- get_global_cycle()     -> the same classification for every tracked
                             country, batched into two Redis pipelines
                             total instead of calling get_economic_cycle()
                             (and its several round trips) once per country
"""

import asyncio
from collections import defaultdict
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.redis_ import RedisConnection
from custom_types.cpi import countries
from model.lse import LSEModel
from .lse_ import events_
import logging



# NOTE: unused - superseded by `events_` (imported from lse_.py) everywhere
# below. Left over from before the two lists were unified; safe to delete.
macro_list = ["cpi","gdp", "ppi", "unemp"]

# TTL on cached trailing (mu, sigma) stats - a safety margin over the
# weekly refresh cron (worker.py's refresh_factor_stats job) so a stale
# cache expires on its own if that job ever stops running, instead of
# serving out-of-date stats forever.
STATS_TTL = 9 * 24 * 3600

# Economic-cycle calculator config: each tracked factor's weight within its
# category (growth/inflation weights each sum to 1.0), and its orientation
# - +1 if a higher raw reading means stronger growth / hotter inflation,
# -1 if a higher reading means the opposite (e.g. rising unemployment is
# *weaker* growth, so its z-score gets flipped before combining).
FACTOR_CONFIG = {
    "retail":    {"weight": 0.5,  "orientation": 1,  "category": "growth"},
    "unemp":     {"weight": 0.5,  "orientation": -1, "category": "growth"},
    "cpi":       {"weight": 1/3,  "orientation": 1,  "category": "inflation"},
    "ppi":       {"weight": 1/3,  "orientation": 1,  "category": "inflation"},
    "inflation": {"weight": 1/3,  "orientation": 1,  "category": "inflation"},
}


def _z_score(x: float, mu: float, sigma: float) -> float:
    """Standardize a raw reading against its own trailing history:
    z = (x - mu) / sigma. sigma == 0 (e.g. a brand-new/flat series) yields
    a neutral 0.0 instead of dividing by zero."""
    if sigma == 0:
        return 0.0
    return (x - mu) / sigma


def _compute_composite(raw_values: dict, stats: dict) -> tuple[float, float, dict]:
    """Standardize each factor with data available, orient its sign, then
    combine by category into a growth composite and an inflation composite.

    A factor missing either its latest raw value or its cached (mu, sigma)
    baseline is skipped entirely rather than treated as neutral/zero - the
    composites are a sum of whatever weighted z-scores are actually
    available, not padded out to a fixed factor count.
    """
    z_scores: dict[str, float] = {}
    growth_sum = 0.0
    inflation_sum = 0.0

    for factor, cfg in FACTOR_CONFIG.items():
        value = raw_values.get(factor)
        factor_stats = stats.get(factor)

        if value is None or factor_stats is None:
            continue

        z = _z_score(value, factor_stats["mu"], factor_stats["sigma"])
        z *= cfg["orientation"]
        z_scores[factor] = z

        weighted = z * cfg["weight"]
        if cfg["category"] == "growth":
            growth_sum += weighted
        else:
            inflation_sum += weighted

    return growth_sum, inflation_sum, z_scores


def _classify_quadrant(growth: float, inflation: float) -> str:
    """Growth/inflation quadrant regime, from the sign of each composite."""
    if growth >= 0 and inflation < 0:
        return "Reflation"
    if growth >= 0 and inflation >= 0:
        return "Overheat"
    if growth < 0 and inflation >= 0:
        return "Stagflation"
    return "Recession"


def _classify_ring(composite_now: float, composite_prev: float) -> str:
    """Single-composite four-phase business-cycle read: level (above/below
    baseline 0) crossed with direction (rising/falling vs. the previous
    reading)."""
    rising = composite_now > composite_prev
    above_baseline = composite_now >= 0

    if above_baseline and rising:
        return "Expansion"
    if above_baseline and not rising:
        return "Slowdown"
    if not above_baseline and not rising:
        return "Contraction"
    return "Recovery"


class MacroController:
    # NOTE: `global events_` at class-body scope (and again inside each
    # method below) is a no-op here - `global` only matters when a function
    # *assigns* to the name; every use of `events_` in this file only reads
    # it, so none of these declarations actually change anything. Harmless,
    # but dead.
    global events_


    def __init__(self):
        self.redis = RedisConnection().get_async_redis()
        self.model = LSEModel()

    async def refresh_factor_stats(self, years: int = 2):
        """Recompute every tracked factor's trailing (mu, sigma) per
        country from Postgres and cache it in Redis, so the economic-cycle
        z-score calculation doesn't have to hit Postgres on every read.

        Meant to run on a periodic cron (see worker.py) rather than on
        every request, since trailing stats barely move between individual
        releases.
        """
        try:
            global events_
            pipeline = self.redis.pipeline()

            for macro in events_:
                country_stats = await self.model.get_trailing_stats(macro, years=years)
                for country, stats in country_stats.items():
                    pipeline.set(f"{macro}:stats:{country}", json.dumps(stats), ex=STATS_TTL)

            await pipeline.execute()

        except Exception as e:
            logging.error(f"Error refreshing factor stats: {e}", exc_info=True)
            raise

    async def get_factor_stats(self, country: str) -> dict:
        """Read back the cached trailing (mu, sigma) for every tracked
        factor for one country - the historical-stats input to the
        economic-cycle z-score calculation. A factor with no cached stats
        yet (cache not warmed, or no history for that country) comes back
        as None so callers can decide how to handle a missing baseline.
        """
        try:
            global events_
            pipeline = self.redis.pipeline()
            for macro in events_:
                pipeline.get(f"{macro}:stats:{country}")
            results = await pipeline.execute()

            return {
                macro: json.loads(value) if value else None
                for macro, value in zip(events_, results)
            }
        except Exception as e:
            logging.error(f"Error getting factor stats for {country}: {e}", exc_info=True)
            raise

    # async def get_economic_cycle(self, country: str) -> dict:
    #     """Classify `country`'s current position in the growth/inflation
    #     economic cycle: z-score each tracked factor's latest reading
    #     (get_country_stats) against its cached trailing baseline
    #     (get_factor_stats), combine into growth/inflation composites, then
    #     classify by quadrant and - if a prior reading is cached - by trend.

    #     Returns None for `ring_phase` on the first call for a country
    #     (there's no previous composite yet to compare direction against);
    #     every later call has one, since each run caches its own composite
    #     for the next call to read.
    #     """
    #     try:
    #         # Two independent inputs: the country's latest readings for
    #         # every tracked indicator, and the cached trailing (mu, sigma)
    #         # baseline each of those readings gets standardized against.
    #         raw = await self.get_country_stats(country)
    #         stats = await self.get_factor_stats(country)

    #         # `raw` also carries `{macro}_date` and `new_sentiment` keys
    #         # (see get_country_stats) that aren't factors - keep only the
    #         # ones FACTOR_CONFIG actually knows about, and drop any that
    #         # came back None (no cached reading for that indicator yet).
    #         raw_values = {
    #             factor: raw.get(factor)
    #             for factor in FACTOR_CONFIG
    #             if raw.get(factor) is not None
    #         }

    #         # z-score each available factor against its baseline, orient
    #         # it, and combine by category - see _compute_composite above.
    #         growth, inflation, z_scores = _compute_composite(raw_values, stats)

    #         if not z_scores:
    #             # No factor had both a raw value and a cached baseline -
    #             # nothing to classify (e.g. stats cache not warmed yet).
    #             return {
    #                 "growth": None,
    #                 "inflation": None,
    #                 "composite": None,
    #                 "quadrant": None,
    #                 "ring_phase": None,
    #                 "z_scores": {},
    #             }

    #         # Quadrant only needs this call's own growth/inflation - no
    #         # history required.
    #         quadrant = _classify_quadrant(growth, inflation)
    #         composite_now = (growth + inflation) / 2

    #         # The trend read (ring_phase) needs a *previous* composite to
    #         # compare direction against - read whatever the last call for
    #         # this country left cached, before this call overwrites it.
    #         composite_key = f"cycle:{country}:composite"
    #         composite_prev = await self.redis.get(composite_key)
    #         ring_phase = (
    #             _classify_ring(composite_now, float(composite_prev))
    #             if composite_prev is not None
    #             else None  # first call ever for this country - nothing to compare against yet
    #         )
    #         # Persist this call's composite so the *next* call has a
    #         # previous value to read back above.
    #         await self.redis.set(composite_key, composite_now)

    #         return {
    #             "growth": growth,
    #             "inflation": inflation,
    #             "composite": composite_now,
    #             "quadrant": quadrant,
    #             "ring_phase": ring_phase,
    #             "z_scores": z_scores,
    #         }

    #     except Exception as e:
    #         logging.error(f"Error getting economic cycle for {country}: {e}", exc_info=True)
    #         raise

    async def get_global_cycle(self) -> dict:
        """Growth/inflation/trend classification for every tracked country,
        e.g. {"USA": {"growth": ..., "quadrant": "Overheat",
        "values": {"cpi": 2.1, "retail": -0.8, ...}, "new_sentiment": 0.42,
        ...}, ...}.

        Unlike calling get_economic_cycle() once per country, this batches
        every input read (raw values + trailing stats + previous
        composites, across every country) into a single Redis pipeline,
        and every composite write into a second single pipeline - O(1)
        round trips instead of O(len(countries)).
        """
        try:
            global countries, events_
            read_pipeline = self.redis.pipeline()

            # Queue every input this computation needs, for every country,
            # in one batch: raw latest readings, cached trailing stats,
            # each country's previously stored composite, and its news
            # sentiment - in a fixed, known order so the flat `results`
            # list can be sliced back apart per country below.
            for country in countries:
                for macro in events_:
                    read_pipeline.get(f"{macro}:{country}")
                    read_pipeline.get(f"{macro}:stats:{country}")
                read_pipeline.get(f"cycle:{country}:composite")
                read_pipeline.get(f"sentiment_news:{country}")

            results = await read_pipeline.execute()

            n = len(events_)
            # Every country consumed a fixed-size slice of the flat
            # `results` list, in the exact order queued above: n (raw,
            # stats) pairs (2n entries), then composite_prev, then
            # sentiment - i.e. [raw_1, stats_1, raw_2, stats_2, ..., raw_n,
            # stats_n, composite_prev, sentiment], repeated once per
            # country.
            stride = 2 * n + 2

            write_pipeline = self.redis.pipeline()
            cycles: dict[str, dict] = {}

            for i, country in enumerate(countries):
                offset = i * stride
                # raw/stats are interleaved within their 2n-entry span, so
                # a stride-2 slice un-interleaves them: every even index is
                # a raw value, every odd index is that same macro's stats.
                # For this country's window of `results` (n=5 shown):
                #
                #   index:   0     1      2     3      4     5      6     7      8     9      10               11
                #   value: raw_1 stats_1 raw_2 stats_2 raw_3 stats_3 raw_4 stats_4 raw_5 stats_5 composite_prev sentiment
                #          \___________________________ 2*n entries ___________________________/
                #
                #   results[offset      : offset+2*n : 2] -> indices 0,2,4,6,8   -> raw_1..raw_5
                #   results[offset+1    : offset+2*n : 2] -> indices 1,3,5,7,9   -> stats_1..stats_5
                #   results[offset+2*n]                   -> index 10           -> composite_prev
                #   results[offset+2*n+1]                 -> index 11           -> sentiment
                raw_slice = results[offset : offset + 2 * n : 2]
                stats_slice = results[offset + 1 : offset + 2 * n : 2]
                composite_prev_raw = results[offset + 2 * n]
                sentiment_raw = results[offset + 2 * n + 1]
                new_sentiment = float(sentiment_raw) if sentiment_raw else None

                # Pair each slice back up with the macro names in `events_`
                # (same order they were queued in), dropping any macro this
                # country has no cached reading/baseline for.
                raw_values = {
                    macro: json.loads(v).get("index_value")
                    for macro, v in zip(events_, raw_slice)
                    if v
                }
                stats = {
                    macro: json.loads(v)
                    for macro, v in zip(events_, stats_slice)
                    if v
                }

                # Same z-score/composite/classification logic as
                # get_economic_cycle, just run once per country here
                # instead of via a second Redis round trip per country.
                growth, inflation, z_scores = _compute_composite(raw_values, stats)

                if not z_scores:
                    # No factor had both a raw value and a cached baseline
                    # for this country - still return what raw data and
                    # sentiment exist, just with the cycle fields empty.
                    cycles[country] = {
                        "growth": None,
                        "inflation": None,
                        "composite": None,
                        "quadrant": None,
                        "ring_phase": None,
                        "z_scores": {},
                        "values": raw_values,
                        "new_sentiment": new_sentiment,
                    }
                    continue

                quadrant = _classify_quadrant(growth, inflation)
                composite_now = (growth + inflation) / 2
                # Trend vs. whatever composite the *previous* call for this
                # country left cached (fetched above, before this call's
                # write below overwrites it) - None on a country's first
                # ever call, same as get_economic_cycle.
                ring_phase = (
                    _classify_ring(composite_now, float(composite_prev_raw))
                    if composite_prev_raw is not None
                    else None
                )

                # Queue this country's new composite for the batched write
                # below, instead of writing it immediately - keeps every
                # country's write in the same single round trip.
                write_pipeline.set(f"cycle:{country}:composite", composite_now)

                cycles[country] = {
                    "growth": growth,
                    "inflation": inflation,
                    "composite": composite_now,
                    "quadrant": quadrant,
                    "ring_phase": ring_phase,
                    "z_scores": z_scores,
                    "values": raw_values,
                    "new_sentiment": new_sentiment,
                }

            # One round trip persists every country's new composite at
            # once, so the next call to get_global_cycle() (or
            # get_economic_cycle() for an individual country) has each
            # country's previous value to compare trend against.
            await write_pipeline.execute()

            return cycles

        except Exception as e:
            logging.error(f"Error getting global economic cycle: {e}", exc_info=True)
            raise

    async def get_global_avg(self):
        """Return the cross-country average for every tracked macro
        indicator, e.g. {"cpi": 2.31, "ppi": 1.05, ...}.

        Reads the `{macro}:avg` key each indicator controller maintains
        (see lse_.py's insert_redis, which recomputes it on every sync).
        """
        try:
            global events_

            # macro_list.remove("gdp")

            avg_dict = defaultdict()
            pipeline = self.redis.pipeline()

            # get the global avg
            # Queue one GET per tracked event/indicator type onto the
            # pipeline so all averages are fetched in a single round trip.
            for ele in events_:
                pipeline.get(f"{str(ele)}:avg")

            result= await pipeline.execute()


            # BUG: `round(float(value), 4) or None` - if the rounded average
            # is exactly 0.0, `0.0 or None` evaluates to None (0.0 is
            # falsy), silently turning a genuine zero average into a
            # missing value. Also, if `value` is None (no `{macro}:avg` key
            # cached yet for this indicator), `float(None)` raises
            # TypeError, uncaught here (it would propagate up as a raw
            # exception via the `except` block below instead of yielding
            # a clean per-key None like the other methods in this file do).
            for value, macro in zip(result, events_):
                avg_dict[macro] = round(float(value),4) or None

            return avg_dict


        except Exception as e:
            logging.error(f"Error getting global average", exc_info=True)
            raise

    async def get_global_stats(self)   :
        """Return every tracked macro indicator's latest value, plus news
        sentiment, for every tracked country:
        {"USA": {"cpi": 2.1, "ppi": 0.9, ..., "new_sentiment": 0.42}, ...}
        """
        try:
            global countries, events_
            pipeline = self.redis.pipeline()

            data= defaultdict(dict)
            keys = []

            # Queue one GET per (country, macro) pair - e.g. "cpi:USA",
            # "ppi:USA", ... - onto the pipeline. `keys` tracks the exact
            # order queued so the flat `results` list can be zipped back
            # against them below.
            for country in countries:

                # pipeline.get(senti_key)
                for macro in events_:
                    key = f"{macro}:{country}"
                    keys.append(key)
                    pipeline.get(key)  # Queue all get commands

            # Also queue one GET per country's cached sentiment score, in
            # the same pipeline batch (after all the macro keys).
            sentiment_keys = [f"sentiment_news:{country}" for country in countries]
            for senti_key in sentiment_keys:
                pipeline.get(senti_key)
            # Execute all gets at once
            results = await pipeline.execute()

            # `results` is one flat list in queue order: all macro GETs
            # first, then all sentiment GETs - split it back into the two
            # groups using the length of `keys` as the boundary.
            macro_results = results[:len(keys)]
            sentiment_results = results[len(keys):]

            for key, tmp_data in zip(keys, macro_results):
                # Parse key to get country and macro
                # NOTE: this re-derives `country` from the key instead of
                # reusing the outer loop's `country` (which isn't in scope
                # here anyway, this loop is separate) - harmless since the
                # key was built from that same country above, but fragile
                # if a country code ever contained ":".
                macro, country = key.split(':')
                senti_key = f"sentiment_news:{country}"
                # NOTE: dead work - this issues one extra, non-pipelined
                # GET per (country, macro) pair (i.e. len(countries) *
                # len(events_) redundant round trips total), and the result
                # (`senti_score`) is never used - `data[...]["new_sentiment"]`
                # is set from the already-pipelined `sentiment_results`
                # in the separate loop below instead. Safe to delete.
                senti_score = await self.redis.get(senti_key)

                if tmp_data:
                    json_data = json.loads(tmp_data)
                    data[country][macro] = json_data.get("index_value")

                else:
                    data[country][macro] = None

            # Attach each country's sentiment score (from the pipelined
            # `sentiment_results`, positionally paired with `countries`
            # since both were built by iterating `countries` in the same
            # order above).
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
        """Return every tracked macro indicator's latest value (plus its
        report date) and news sentiment for a single `country`:
        {"cpi": 2.1, "cpi_date": "2026-07-01", ..., "new_sentiment": 0.42}
        """
        try:
            global events_
            data = defaultdict()
            keys = []
            pipeline = self.redis.pipeline()
            # Queue one GET per tracked macro indicator for this country
            # only (unlike get_global_stats, which does this for every
            # country).
            for macro in events_:
                key = f"{macro}:{country}"
                keys.append(key)
                pipeline.get(key)

            # BUG: builds a sentiment key for *every* country in `countries`
            # (reusing the get_global_stats pattern) even though this
            # method only wants the single requested `country`'s sentiment.
            # Combined with the loop below, this makes the returned
            # `new_sentiment` reflect whichever country happens to be last
            # in `countries`, not the requested one - see note below.
            sentiment_keys = [f"sentiment_news:{country}" for country in countries]
            for senti_key in sentiment_keys:
                pipeline.get(senti_key)
            # Execute all gets at once
            results = await pipeline.execute()

            macro_results = results[:len(keys)]
            sentiment_results = results[len(keys):]

            for key, tmp_data in zip(keys, macro_results):
                # Parse key to get country and macro
                # NOTE: this reassigns the `country` parameter itself
                # (shadowing the argument passed into this method) with
                # whatever country the key happens to encode - harmless
                # here since every key was built from the same passed-in
                # `country` above, but means `country` no longer reliably
                # refers to the caller's argument for the rest of this
                # block.
                macro, country = key.split(':')
                senti_key = f"sentiment_news:{country}"
                # NOTE: same dead, non-pipelined, unused-result GET as in
                # get_global_stats above - safe to delete.
                senti_score = await self.redis.get(senti_key)

                if tmp_data:
                    json_data = json.loads(tmp_data)
                    data[macro] = json_data.get("index_value")
                    data[f"{macro}_date"] = json_data.get("report_date")

                else:
                    data[macro] = None

            # BUG: iterates over *all* `countries`, not just the requested
            # one, and unconditionally overwrites `data["new_sentiment"]`
            # on every iteration (since `data` here is flat, not nested by
            # country like get_global_stats). The end result is whatever
            # sentiment score belongs to the *last* entry in `countries`
            # (e.g. always "FRA" given the current custom_types.cpi.countries
            # order) - not the sentiment for the `country` argument this
            # method was actually called with. This loop should instead
            # look up just `sentiment_results[countries.index(country)]`
            # (or better, queue only the one sentiment key needed above).
            for country, senti_score in zip(countries, sentiment_results):
                if senti_score:
                    data["new_sentiment"] = float(senti_score)
                else:
                    data["new_sentiment"] = None

            return data
        except Exception as e:
            logging.error(f"Error getting countries macro data")
            raise

# if __name__ == "__main__":
#     test = MacroController()
#     print(asyncio.run(test.get_global_cycle()))
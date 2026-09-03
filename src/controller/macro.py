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

get_cross_section_panel() feeds a different consumer entirely: the
"Orthogonal View" frontend's buildPanel() swap point, which wants 60
monthly YoY% series per country (cpi, ppi, retailNom) plus a monthly
unemployment rate series (unemp) - see that function's docstring for how
this bridges TPFMacro's MoM-only LSE data into the YoY shape it expects.
"""

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.redis_ import RedisConnection
from custom_types.cpi import countries, country_mapping, get_country_code
from model.lse import LSEModel
from controller.lse_ import events_
import logging
logger = logging.getLogger(__name__)



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


# Cross-section panel config (feeds get_cross_section_panel(), the
# Orthogonal View frontend's buildPanel() swap point - see that method's
# docstring). TPFMacro's own 3-letter codes mapped to the frontend's
# 2-letter-ish codes; the frontend has no Eurozone aggregate, so only the
# countries this app actually tracks per-country (custom_types.cpi's
# `countries`) are included.
#
# Derived from custom_types.cpi's country_mapping via get_country_code()
# rather than hardcoded, so this list of codes can't drift out of sync
# with the rest of the app's own country mapping.
#
# One override is required: country_mapping has both 'GB' and 'UK' mapped
# to 'GBR', and get_country_code() returns whichever key comes first in
# the dict ('GB' - custom_types.cpi orders it that way deliberately, to
# prefer ISO 3166-1 alpha-2 for TPFMacro's own reverse lookups elsewhere).
# But the Orthogonal View source this panel feeds uses "UK" specifically
# (its XS_META/ECON keys are US/EU/CN/JP/UK/IN/BR/DE) - keeping that exact
# contract intact matters more here than TPFMacro's own internal
# convention, so GBR is force-mapped to "UK" rather than letting the
# generic reverse-lookup silently hand back "GB" instead.
_XS_CODE_OVERRIDES: dict[str, str] = {"GBR": "UK"}

XS_COUNTRY_CODES: dict[str, str] = {
    tpf_code: _XS_CODE_OVERRIDES.get(tpf_code) or get_country_code(tpf_code, country_mapping)
    for tpf_code in countries
}

# How many monthly YoY observations get_cross_section_panel() returns per
# series - matches buildPanel()'s "60 observations" requirement.
PANEL_MONTHS = 30

# If a country's most recent synced reading (across cpi/ppi/retail/unemp)
# is older than this, get_cross_section_panel() logs a warning - a panel
# can have exactly PANEL_MONTHS points (so the completeness gate passes)
# while still being built from a stalled sync, and without checking the
# actual report_date there'd be no way to tell.
STALE_DATA_DAYS = 45


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


def _mom_to_yoy(mom_series: list[float]) -> list[float]:
    """Chain a month-over-month % change series into an approximate
    year-over-year % change series, since TPFMacro only tracks MoM releases
    (see lse_.py's MONTHLY_EVENT_PATTERNS) but the cross-section panel
    needs YoY.

    `mom_series` is oldest -> newest. Each output point compounds the
    trailing 12 MoM readings ending at that month: (1+m1/100)*...*(1+m12/100)
    - 1, as a %. Needs at least 12 MoM points to produce even one YoY
    point, so the result is 11 points shorter than the input (empty if
    there are fewer than 12).
    """
    if len(mom_series) < 12:
        return []

    yoy: list[float] = []
    for i in range(11, len(mom_series)):
        factor = 1.0
        for mom in mom_series[i - 11 : i + 1]:
            factor *= 1 + mom / 100
        yoy.append((factor - 1) * 100)
    return yoy


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
            logger.error(f"Error refreshing factor stats: {e}", exc_info=True)
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
            logger.error(f"Error getting factor stats for {country}: {e}", exc_info=True)
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
    #         logger.error(f"Error getting economic cycle for {country}: {e}", exc_info=True)
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
            logger.error(f"Error getting global economic cycle: {e}", exc_info=True)
            raise

    async def get_cross_section_panel(self) -> dict:
        """Build the panel shape the "Orthogonal View" frontend's
        buildPanel() swap point requires:
        {"US": {"cpi": [...60 YoY%...], "ppi": [...], "retailNom": [...],
        "unemp": [...60 rate%...]}, ...}, oldest -> newest.

        Two bridges from what TPFMacro actually stores to what that shape
        wants:

        - MoM -> YoY: LSE only publishes month-over-month releases for
          cpi/ppi/retail/inflation (see lse_.py's MONTHLY_EVENT_PATTERNS),
          not year-over-year, so cpi/ppi/retailNom are chained from MoM via
          _mom_to_yoy() rather than read directly. unemp is already a rate
          level, so it's used as-is, unchained.
        - cpi vs inflation: some countries' calendars report headline
          prices as "CPI MoM", others as "Inflation Rate MoM" - those land
          in TPFMacro's separate `cpi` and `inflation` tables respectively.
          Both represent the same real-world headline figure, so a
          country's `cpi` field here prefers the `cpi` table and falls back
          to `inflation` only if that country has no `cpi` history.

        A country is omitted entirely if any of the four series doesn't
        have PANEL_MONTHS points yet (e.g. too little sync history) -
        better than handing the frontend a partially-populated series it
        has no way to flag as incomplete.
        """
        try:
            # +11 so chaining 12-point MoM windows still yields PANEL_MONTHS
            # YoY points at the end (each YoY point consumes 12 MoM points).
            mom_months = PANEL_MONTHS + 11
            cpi_mom, inflation_mom, ppi_mom, retail_mom, unemp_levels = await asyncio.gather(
                self.model.get_monthly_series("cpi", months=mom_months),
                self.model.get_monthly_series("inflation", months=mom_months),
                self.model.get_monthly_series("ppi", months=mom_months),
                self.model.get_monthly_series("retail", months=mom_months),
                self.model.get_monthly_series("unemp", months=PANEL_MONTHS),
            )

            panel: dict[str, dict] = {}

            for tpf_code, xs_code in XS_COUNTRY_CODES.items():
                # (report_date, index_value) pairs, oldest -> newest.
                cpi_points = cpi_mom.get(tpf_code) or inflation_mom.get(tpf_code) or []
                ppi_points = ppi_mom.get(tpf_code) or []
                retail_points = retail_mom.get(tpf_code) or []
                unemp_points = (unemp_levels.get(tpf_code) or [])[-PANEL_MONTHS:]

                cpi_series = [value for _, value in cpi_points]
                ppi_series = [value for _, value in ppi_points]
                retail_series = [value for _, value in retail_points]
                unemp_series = [value for _, value in unemp_points]

                cpi_yoy = _mom_to_yoy(cpi_series)[-PANEL_MONTHS:]
                ppi_yoy = _mom_to_yoy(ppi_series)[-PANEL_MONTHS:]
                retail_yoy = _mom_to_yoy(retail_series)[-PANEL_MONTHS:]

                if not (
                    len(cpi_yoy) == PANEL_MONTHS
                    and len(ppi_yoy) == PANEL_MONTHS
                    and len(retail_yoy) == PANEL_MONTHS
                    and len(unemp_series) == PANEL_MONTHS
                ):
                    continue  # not enough synced history yet for this country

                # Now that get_monthly_series() hands back real dates
                # instead of bare values, a country's most recent reading
                # can actually be checked for staleness - a panel with
                # exactly PANEL_MONTHS points can still be built from data
                # that stopped syncing months ago, and length alone can't
                # tell the two apart.
                latest_date = max(
                    points[-1][0]
                    for points in (cpi_points, ppi_points, retail_points, unemp_points)
                    if points
                )
                if datetime.now() - latest_date > timedelta(days=STALE_DATA_DAYS):
                    logger.warning(
                        f"Cross-section panel for {xs_code}: most recent reading is from "
                        f"{latest_date.date()}, older than {STALE_DATA_DAYS} days - sync may be stalled."
                    )

                panel[xs_code] = {
                    "cpi": cpi_yoy,
                    "ppi": ppi_yoy,
                    "retailNom": retail_yoy,
                    "unemp": unemp_series,
                }

            return panel

        except Exception as e:
            logger.error(f"Error building cross-section panel: {e}", exc_info=True)
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

            avg_dict: dict = {}
            pipeline = self.redis.pipeline()

            # get the global avg
            # Queue one GET per tracked event/indicator type onto the
            # pipeline so all averages are fetched in a single round trip.
            for ele in events_:
                pipeline.get(f"{str(ele)}:avg")

            result = await pipeline.execute()

            # `result` has one slot per indicator: a JSON string ("2.31"),
            # the literal "null" (insert_redis stores json.dumps(None) when no
            # country was up to date), or None (the `{macro}:avg` key has never
            # been written). Parse each independently and map the "no value"
            # cases to a clean None rather than letting float() raise.
            for value, macro in zip(result, events_):
                parsed = json.loads(value) if value is not None else None
                avg_dict[macro] = round(float(parsed), 4) if parsed is not None else None

            return avg_dict


        except Exception as e:
            logger.error(f"Error getting global average", exc_info=True)
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
            logger.error(f"Error getting global stats: {e}", exc_info=True)
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
            logger.error(f"Error getting countries macro data")
            raise
        
    async def get_country_stats_timeseries(self, country: str, months: int = 14) -> dict:
        """Last `months` monthly readings of every tracked macro indicator for
        one country: {macro: [[report_date_iso, value], ...]}. An indicator
        with no data for this country maps to [].

        report_date comes back from Postgres as a datetime.date, which
        starlette's JSONResponse can't serialise - so dates are stringified
        (ISO) here before returning.

        Multiple releases can land in the same calendar month (a flash estimate
        then the final, or a revision). Those are collapsed to the single most
        recent reading for that month, so the series has at most one point per
        month. Because get_monthly_series trims to `months` raw rows *before*
        this dedupe, a month with a duplicate leaves the returned series one
        point shorter than `months`.
        """

        def _one_per_month(points: list[tuple]) -> list[tuple]:
            # points arrive oldest -> newest; keying a dict by "YYYY-MM" and
            # letting later rows overwrite keeps the last reading per month,
            # and (since months only advance) preserves chronological order.
            by_month: dict[str, tuple] = {}
            for d, value in points:
                month = d.strftime("%Y-%m") if hasattr(d, "strftime") else str(d)[:7]
                by_month[month] = (d, value)
            return list(by_month.values())

        try:
            code = country.upper()
            # events_ calls are independent -> issue them together.
            series = await asyncio.gather(
                *(self.model.get_monthly_series(macro, months) for macro in events_)
            )
            return {
                macro: [
                    [d.isoformat() if hasattr(d, "isoformat") else d, value]
                    for d, value in _one_per_month(by_country.get(code, []))
                ]
                for macro, by_country in zip(events_, series)
            }
        except Exception as e:
            logger.error(f"Error getting all the country timeseries {e}", exc_info=True)
            raise
# if __name__ == "__main__":
#     test = MacroController()
#     print(asyncio.run(test.get_country_stats_timeseries("USA")))
"""Cross-section macro analysis - the "Orthogonal View" frontend's scoring
pipeline, run against real TPFMacro data.

Two pipelines share this module, both keyed on TPFMacro's 3-letter country
codes and both pure arithmetic once load_data() has pulled the raw monthly
releases from Postgres (via LSEModel):

  - Phillips-curve / quadrant snapshot: load_data -> derived_economic_standard
    (target/NAIRU deviations) -> normalized_factors (cross-section percentile
    and per-country z-score) -> calculate_composite (price/demand/composite
    axes) -> assign_quadrants (median split) -> store_quadrants (Redis cache).
    update_quandrant() runs the whole chain end to end.
  - performance_trend(): quarterly percentile-over-time comparison, ranking
    raw quarterly readings directly against peers rather than deviations.

Every helper lives as a method on CrossSectionController (even the ones with
no I/O and no use of `self`) so the whole pipeline is reachable through one
object.
"""

import json
import logging
import os
import sys

# This module is run both as part of the package (imported as
# controller.cross_section) and directly as a script for smoke-testing (see
# the __main__ block at the bottom). Adding the project's src/ directory to
# sys.path makes the bare `database.*` / `controller.*` / `model.*` imports
# below resolve in the script case too.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.redis_ import RedisConnection   # async Redis client factory
from controller.macro import MacroController  # sibling controller; only its ctor is used here so far
from model.lse import LSEModel                # Postgres access layer for the LSE economic-release tables
import asyncio
import pandas as pd
import numpy as np
# Inflation targets are fixed policy constants (central-bank-announced),
# not something to derive from data - unlike NAIRU below. Keyed by
# TPFMacro's own 3-letter codes, so this maps directly off full_df['country']
# with no translation step needed. CN has no official target; 2.0 is an
# assumption (matches the original spec's note).
XS_INFLATION_TARGET: dict[str, float] = {
    "USA": 2.0, "CAN": 2.0, "JPN": 2.0, "DEU": 2.0, "GBR": 2.0,
    "AUS": 2.0, "IND": 4.0, "CHN": 2.0, "KOR": 2.0, "BRA": 3.0, "FRA": 2.0,
}

# Composite indicator config: goodness direction (-1 = lower is healthier)
# and weight. Weights must sum to 1.0 - the fifth ("policy") slot from the
# original spec isn't lit up here since TPFMacro has no policy-rate series
# yet; add it here (and give it a weight, rebalancing the rest) once it does.
IND = {
    'cpiDev': {'sign': -1, 'weight': 0.275},   # Lower is better
    'ppi': {'sign': -1, 'weight': 0.225},      # Lower is better
    'unempGap': {'sign': -1, 'weight': 0.30}, # Lower is better
    'retMom': {'sign': +1, 'weight': 0.20}
    }

# TTL on the cached cross-section quadrant snapshot. It's recomputed from the
# full panel whenever the pipeline runs, so this is only a staleness guard for
# a stalled refresh - a bit over a month, matching the monthly release cadence.
QUADRANT_TTL = 40 * 24 * 3600

# Redis key prefix for the per-country quadrant snapshot (assign_quadrants
# output). One JSON blob per country at f"{QUADRANT_KEY_PREFIX}:{code}", plus
# a f"{QUADRANT_KEY_PREFIX}:_meta" blob with the cross-section medians.
QUADRANT_KEY_PREFIX = "cross_section:quadrant"


class CrossSectionController:
    """Orchestrates the full cross-section pipeline against real data: the
    Phillips-curve/quadrant snapshot (update_quandrant, which chains
    load_data -> derived_economic_standard -> normalized_factors ->
    calculate_composite -> assign_quadrants -> store_quadrants) and the
    quarterly performance-trend comparison (performance_trend).
    """

    def __init__(self):
        # MacroController is constructed but currently unused - kept as the
        # intended hook for reading the pre-built cross-section panel from
        # macro.py once that path is wired up.
        self.macro = MacroController()
        # LSEModel.get_monthly_series() is the only data source this module
        # actually uses today (see load_data()).
        self.model = LSEModel()
        # Shared async Redis connection (pool-backed, decode_responses=True, so
        # every value read back is already a str).
        self.redis = RedisConnection().get_async_redis()


    async def load_data(self):
        """Pull the five raw monthly release histories from Postgres and reshape
        them into one long-form DataFrame plus a small per-country stats dict.

        Returns:
            (df_by_country, country_stats)
            - df_by_country: columns [date, country, cpi, inflation, ppi,
              retail, unemp] with a plain RangeIndex, one row per
              (month, country). cpi/inflation/ppi/retail are month-over-month
              percentage changes; unemp is a rate level. Missing
              (month, country) combinations are present as NaN rows because
              stack(future_stack=True) keeps the full grid.
            - country_stats: {code: {n_months, first_date, last_date, nairu}}
              where nairu is that country's median unemployment (the
              data-derived NAIRU proxy used later by derived_economic_standard).

        Note: the `inflation` column is loaded here but nothing downstream
        consumes it yet - it exists so a later step can coalesce it with `cpi`
        (some countries file their MoM CPI release under the `inflation` table).
        """
        try:
            # One round trip per table, all issued concurrently. Each call
            # returns {country_code: [(report_date, value), ...]} oldest->newest.
            cpi_hist, inflation_hist, ppi_hist, retail_hist, unemp_hist = await asyncio.gather(
            self.model.get_monthly_series("cpi"),
            self.model.get_monthly_series("inflation" ),
            self.model.get_monthly_series("ppi"),
            self.model.get_monthly_series("retail" ),
            self.model.get_monthly_series("unemp"),
        )
            # Turn each {country: [(date, value)]} mapping into a wide frame
            # (index = month-start Timestamp, one column per country).
            cpi_df,inflation_df,ppi_df,retail_df,unemp_df= await asyncio.gather(
                self._series_to_df(cpi_hist),
                                  self._series_to_df(inflation_hist),
                                  self._series_to_df(ppi_hist),
                                  self._series_to_df(retail_hist),
                                  self._series_to_df(unemp_hist))

            # Stack the five wide frames side by side. Result has a two-level
            # column index: level 0 = indicator name, level 1 = country code.
            combined = pd.concat(
                    {"cpi": cpi_df, "inflation": inflation_df, "ppi": ppi_df, "retail": retail_df, "unemp": unemp_df},
                    axis=1,
                ).sort_index()

            df_by_country = (combined.stack(level=1, future_stack=True)          # moves country from columns into the index
            .rename_axis(index=["date", "country"])                              # name the resulting (date, country) MultiIndex
            .reset_index()                                                       # flatten it back to plain columns + RangeIndex
            .rename(columns={                                                    # no-op today; explicit so renames are one edit away
                "date": "date", "country": "country",
                "cpi": "cpi", "ppi": "ppi", "retail": "retail", "unemp": "unemp",
            }))

            # Per-country coverage summary + NAIRU proxy (median unemployment
            # over that country's whole available history).
            country_stats = {}
            for code, df in df_by_country.groupby('country'):
                country_stats[code] = {
                    'n_months': len(df),
                    'first_date': df['date'].min(),
                    'last_date': df['date'].max(),
                    'nairu': df['unemp'].median()
                }

            return df_by_country, country_stats
        except Exception as e:
            logging.error(f"Error loading data into a dataframe: {e}", exc_info=True)
            raise
    


    async def _series_to_df(self, series: dict[str, list[tuple]]) -> pd.DataFrame:
        """Convert one indicator's {country: [(report_date, value), ...]} mapping
        into a wide DataFrame: index = month-start Timestamp, one column per
        country. Countries with no points are dropped; an all-empty input
        yields an empty DataFrame (so the caller's concat still works).
        """
        cols = {}
        for country, points in series.items():
            if not points:
                continue
            dates, values = zip(*points)
            # Snap every release date to the first of its month so readings
            # from different countries line up on a common monthly index.
            idx = pd.to_datetime(dates).to_period("M").to_timestamp()
            s = pd.Series(values, index=idx, name=country)
            s = s[~s.index.duplicated(keep="last")]   # same-month readings -> keep the latest (final over flash)
            cols[country] = s
        return pd.concat(cols, axis=1) if cols else pd.DataFrame()
    
    
    async def derived_economic_standard(self, full_df: pd.DataFrame):
        """Turn the raw long-form frame from load_data() into the four
        "standardised" macro factors the scoring steps work on.

        Input columns used: country, date, cpi, ppi, retail, unemp.
        Output columns (same row grain as the input):
          - cpiDev   : headline inflation minus that country's target
                       (positive = running hot). Uses XS_INFLATION_TARGET.
          - ppi      : producer-price MoM %, passed through unchanged.
          - unempGap : unemployment rate minus that country's NAIRU proxy
                       (median of its own history). Positive = slack.
          - retReal  : real retail growth = nominal retail MoM % minus CPI
                       MoM % (a rough inflation deflation).
          - retMom   : retail momentum = 3-month mean of retReal now, minus
                       the 3-month mean ending 3 months earlier. Positive =
                       retail is accelerating. Forced to 0 for each country's
                       first 6 months, where the two windows aren't both full.
        """
        try:
            # NAIRU proxy: each country's own median unemployment. Recomputed
            # here (rather than taken from load_data's country_stats) so this
            # method can be called with any subset of the panel.
            nairu_by_country = full_df.groupby('country')['unemp'].median()

            # Start from an empty frame sharing full_df's row index, then add
            # one derived column at a time. Every operation below is vectorised
            # across all (month, country) rows at once.
            derived = pd.DataFrame(index=full_df.index)
            derived['country'] = full_df['country']
            derived['date'] = full_df['date']

            # 1. CPI deviation: headline MoM inflation minus the per-country
            #    target (2% for most; see XS_INFLATION_TARGET for exceptions).
            target_by_country = full_df['country'].map(XS_INFLATION_TARGET)
            derived['cpiDev'] = full_df['cpi'] - target_by_country

            # 2. PPI passes straight through - no target/baseline to subtract.
            derived['ppi'] = full_df['ppi']

            # 3. Unemployment gap: rate minus the country's NAIRU proxy.
            derived['unempGap'] = full_df['unemp'] - full_df['country'].map(nairu_by_country)

            # 4. Real retail growth: nominal retail MoM % less CPI MoM %.
            derived['retReal'] = full_df['retail'] - full_df['cpi']

            # 5. Retail momentum (acceleration): difference between the current
            #    3-month average of retReal and the 3-month average three
            #    months back. transform() keeps the result aligned to the
            #    original rows, computed independently per country.
            derived['retMom'] = derived.groupby('country')['retReal'].transform(
                lambda x: x.rolling(3).mean() - x.shift(3).rolling(3).mean()
            )

            # The first 6 rows per country can't have both 3-month windows
            # fully populated, so pin their momentum to 0 rather than leave a
            # partially-computed value. cumcount() numbers rows 0,1,2,... within
            # each country group.
            first_6_mask = derived.groupby('country').cumcount() < 6
            derived.loc[first_6_mask, 'retMom'] = 0.0

            return derived

        except Exception as e:
            logging.error(f"Error implementing economic derivation: {e}", exc_info=True)
            raise
        
    async def normalized_factors(self, df: pd.DataFrame):
        """Score every country 0-100 on each of the four IND factors, two
        different ways.

        Input: the frame from derived_economic_standard() (columns date,
        country, cpiDev, ppi, unempGap, retReal, retMom).

        Returns (scores_df, z_scores_df), both indexed by country code with
        columns score_cpiDev / score_ppi / score_unempGap / score_retMom:
          - scores_df   : CROSS-SECTIONAL percentile of the most recent
                          month's reading - how a country ranks against its
                          peers right now. 100 = healthiest of the group.
          - z_scores_df : TIME-SERIES z-score of the country's latest reading
                          against its own history, mapped onto a 50-centred
                          band (50 + 15*z, sign-adjusted) and clamped to
                          [2, 98] - how unusual this reading is for that
                          country, regardless of peers.

        The IND `sign` (-1 for "lower is healthier") is applied in both so a
        higher score always means "healthier".
        """
        try:
            # Work on a (date, country) MultiIndex so we can slice a single
            # month's cross-section with .xs(...).
            df = df.set_index(['date', 'country'])
            latest_date = df.index.get_level_values('date').max()

            # Both outputs are one row per country.
            scores_df = pd.DataFrame(index=df.index.get_level_values('country').unique())
            z_scores_df = pd.DataFrame(index=df.index.get_level_values('country').unique())

            for indicator, config in IND.items():
                sign = config['sign']
                # ---- cross-sectional percentile (peers, latest month) ----
                latest_values = df.xs(latest_date, level='date')[indicator]
                signed_values = latest_values * sign   # flip so bigger = healthier

                # rank(pct=True) returns k/n in (0, 1]; *100 -> (0, 100].
                percentile = signed_values.rank(pct=True) * 100

                # Rescale so the spacing spans n-1 gaps rather than n. With
                # n countries the raw best rank is 100 (n/n); this stretches
                # the distribution slightly so the ranks aren't compressed
                # toward the top for small n. Skipped when n <= 1.
                n = len(signed_values)
                if n > 1:
                    percentile = percentile * n / (n - 1)

                scores_df[f'score_{indicator}'] = percentile.round()

                # ---- per-country time-series z-score ----
                def z_score_country(country_data):
                    # country_data is that one country's full history (all
                    # months) as a sub-frame; assumes it's in date order.
                    history = country_data[indicator].dropna()
                    if history.empty:
                        return float('nan')
                    mu = history.mean()
                    sigma = history.std(ddof=0)         # population stdev
                    current = history.iloc[-1]          # latest reading
                    z = (current - mu) / sigma if sigma > 0 else 0
                    # Centre at 50, ~15 points per standard deviation, sign so
                    # "healthier" is higher, then clamp to a 2..98 band.
                    raw = 50 + 15 * z * config['sign']
                    return max(2, min(98, round(raw)))

                z_scores_df[f'score_{indicator}'] = df.groupby('country').apply(
                lambda x: z_score_country(x)
            )
            return scores_df,z_scores_df

        except  Exception as e:
            logging.error(f"Error Normalizing derived factors: {e}", exc_info=True)
            raise
        
    async def calculate_composite(self, scores_df: pd.DataFrame):
        """Blend the four per-factor scores into the axes the quadrant view
        needs. The pipeline (update_quandrant) passes the z-score frame here,
        but the maths works on either frame from normalized_factors().

        Output columns, one row per country:
          - price     : price-pressure axis, 0.55*cpiDev + 0.45*ppi
          - demand    : demand-strength axis, 0.60*unempGap + 0.40*retMom
          - composite : all four scores combined with their IND weights
                        (0.275/0.225/0.30/0.20, summing to 1.0)
          - contrib_* : each factor's weighted piece of `composite`
                        (contrib_cpi + contrib_ppi + contrib_unemp +
                        contrib_ret == composite), kept separate so a stacked
                        bar chart can show each factor's contribution.

        Because these are plain sums, a NaN in any input score propagates to
        NaN in price / demand / composite for that country.
        """
        try:
            result = pd.DataFrame(index=scores_df.index)

            # 1. Price-pressure axis: inflation deviation and producer prices.
            #    The input scores are already sign-adjusted by normalized_factors.
            result['price'] = (0.55 * scores_df['score_cpiDev'] +
                            0.45 * scores_df['score_ppi']).round()

            # 2. Demand axis: labour-market slack and retail momentum.
            result['demand'] = (0.60 * scores_df['score_unempGap'] +
                            0.40 * scores_df['score_retMom']).round()

            # 3. Single headline number: weighted sum of all four factor scores.
            result['composite'] = (
                scores_df['score_cpiDev'] * IND['cpiDev']['weight'] +
                scores_df['score_ppi'] * IND['ppi']['weight'] +
                scores_df['score_unempGap'] * IND['unempGap']['weight'] +
                scores_df['score_retMom'] * IND['retMom']['weight']
            ).round()

            # 4. The individual weighted terms behind `composite` (unrounded),
            #    for stacked-bar rendering on the frontend.
            result['contrib_cpi'] = scores_df['score_cpiDev'] * IND['cpiDev']['weight']
            result['contrib_ppi'] = scores_df['score_ppi'] * IND['ppi']['weight']
            result['contrib_unemp'] = scores_df['score_unempGap'] * IND['unempGap']['weight']
            result['contrib_ret'] = scores_df['score_retMom'] * IND['retMom']['weight']

            return result

        except Exception as e:
            logging.error(f"Error calculating composite score", exc_info=True)
            raise
        
    async def assign_quadrants(self, composite_df: pd.DataFrame) -> pd.DataFrame:
        """Label each country by which quadrant of the price/demand plane it
        sits in, splitting on the cross-section median of each axis.

            demand high + price high  -> Overheating
            demand low  + price high  -> Weak
            demand high + price low   -> Goldilocks
            demand low  + price low   -> Stagflation

        Mutates `composite_df` in place (adds `quadrant`, `price_median`,
        `demand_median`) and returns it.

        Caveat: a country with NaN price or demand (an indicator was missing
        upstream) compares False on every ">" test, so it lands in the
        low/low bucket ("Stagflation") rather than the `default` of
        "Unclassified". Guard on notna() before trusting a label for a country
        known to have gaps.
        """
        try:
            # Split lines: the median price and median demand across all
            # countries in this cross-section.
            price_median = composite_df['price'].median()
            demand_median = composite_df['demand'].median()

            # Boolean column masks, evaluated for every country at once.
            price_high = composite_df['price'] > price_median
            demand_high = composite_df['demand'] > demand_median

            # np.select walks the conditions in order and picks the first that
            # is True for each row; the four cases are mutually exclusive and
            # exhaustive for non-NaN rows.
            conditions = [
                (price_high & demand_high),      # Overheating
                (price_high & ~demand_high),     # Weak
                (~price_high & demand_high),     # Goldilocks
                (~price_high & ~demand_high)     # Stagflation
            ]

            choices = ['Overheating', 'Weak', 'Goldilocks', 'Stagflation']

            composite_df['quadrant'] = np.select(conditions, choices, default='Unclassified')
            # Store the split lines on every row so a single-country read still
            # carries the context needed to place it on the plane.
            composite_df['price_median'] = price_median
            composite_df['demand_median'] = demand_median

            return composite_df
        except Exception as e:
            logging.error(f"Error assigning quadrant: {e}", exc_info=True)
            raise

    async def store_quadrants(self, composite_df: pd.DataFrame, ttl: int = QUADRANT_TTL) -> None:
        """Cache the assign_quadrants() output in Redis, one JSON object per
        country at f"{QUADRANT_KEY_PREFIX}:{code}" (price/demand/composite/
        contrib_*/quadrant/price_median/demand_median), plus a "_meta" object
        with the cross-section medians and an update timestamp.

        Uses DataFrame.to_json so NaN cells - a country missing an indicator,
        via the composite NaN propagation - serialise as JSON null rather than
        a bare NaN token the frontend can't parse. Writes go through one
        pipeline so the whole snapshot lands in a single round trip.
        """
        try:
            # to_json(orient="index") -> {code: {column: value, ...}, ...} with
            # NaN rendered as JSON null. Round-tripping through json.loads gives
            # a plain dict so we can re-serialise each country separately.
            per_country = json.loads(composite_df.to_json(orient="index"))

            # median() skips NaN; the result is still NaN if a whole column is
            # empty, so coerce that to None for valid JSON.
            medians = composite_df[["price", "demand"]].median()
            meta = {
                "price_median": None if pd.isna(medians["price"]) else float(medians["price"]),
                "demand_median": None if pd.isna(medians["demand"]) else float(medians["demand"]),
                "countries": [str(c) for c in composite_df.index],
                "updated": pd.Timestamp.now(tz="UTC").isoformat(),
            }

            # Queue every write on one pipeline: N per-country keys + the
            # _meta key, all with the same TTL, sent in a single round trip.
            pipeline = self.redis.pipeline()
            for code, row in per_country.items():
                pipeline.set(f"{QUADRANT_KEY_PREFIX}:{code}", json.dumps(row), ex=ttl)
            pipeline.set(f"{QUADRANT_KEY_PREFIX}:_meta", json.dumps(meta), ex=ttl)
            await pipeline.execute()
        except Exception as e:
            logging.error(f"Error storing quadrants to redis: {e}", exc_info=True)
            raise

    def performance_trend(self, full_df: pd.DataFrame, quarters: int = 8) -> dict:
        """Quarterly percentile-over-time comparison: for each of the last
        `quarters` quarters, rank every country against its peers on each
        indicator, then blend the ranks into one composite per country per
        quarter.

        Returns {'quarters': [...], 'per_indicator': {name: DataFrame},
        'composite': DataFrame} - the per_indicator / composite frames are
        indexed by country, one column per quarter.

        NOTE: this method expects `full_df` to have a quarterly-resamplable
        DatetimeIndex and a (date, country) level structure (so `.resample`,
        `quarterly.index.get_level_values('country')` and
        `quarterly.xs(q, level='date')` all work). The long-form frame that
        load_data() returns has a plain RangeIndex with date/country as
        columns, so it must be reshaped (set a DatetimeIndex, group by country)
        before being passed here - calling this directly on load_data()[0]
        raises "Only valid with DatetimeIndex...".
        """
        try:
            # 1. Collapse each quarter to a single number per indicator.
            #    cpi/ppi/retail: percentage change from the quarter's first
            #    reading to its last. unemp: the quarter's last reading.
            quarterly = full_df.resample('Q').agg({
                'cpi': lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100,  # Quarter change
                'ppi': lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100,
                'retail': lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100,
                'unemp': 'last'
            })

            # 2. Keep only the most recent `quarters` rows.
            last_n_quarters = quarterly.tail(quarters)

            # 3. For every indicator, build a country x quarter frame of
            #    cross-sectional percentile ranks (sign-adjusted so higher =
            #    healthier, same convention as IND).
            per_indicator = {}
            composite = pd.DataFrame(index=quarterly.index.get_level_values('country').unique())

            for indicator, config in IND.items():
                quarter_ranks = []
                for q in last_n_quarters.index:
                    # All countries' values for this one quarter.
                    values = quarterly.xs(q, level='date')[indicator]
                    signed_values = values * config['sign']
                    ranks = signed_values.rank(pct=True) * 100
                    quarter_ranks.append(ranks)

                # One column per quarter, labelled by the quarter timestamp.
                ranks_df = pd.concat(quarter_ranks, axis=1)
                ranks_df.columns = last_n_quarters.index
                per_indicator[indicator] = ranks_df

            # 4. Weighted blend of the four indicator ranks, quarter by quarter.
            for i, q in enumerate(last_n_quarters.index):
                composite[q] = (
                    per_indicator['cpi'].iloc[:, i] * 0.30 +
                    per_indicator['ppi'].iloc[:, i] * 0.225 +
                    per_indicator['unemp'].iloc[:, i] * 0.30+
                    per_indicator['retail'].iloc[:, i] * 0.20
                ).round()

            return {
                'quarters': list(last_n_quarters.index),
                'per_indicator': per_indicator,
                'composite': composite
            }
        except Exception as e:
            logging.error(f"Error in performing trend: {e}", exc_info=True)
            raise
        
    async def update_quandrant(self):
        """Run the whole quadrant pipeline once and cache the result. Intended
        to be triggered on a schedule (e.g. after each monthly release batch).
        Read the cached output with get_cross_section().
        """
        try:
            data = await self.load_data()                                   # (df_by_country, country_stats)
            eco_data = await self.derived_economic_standard(data[0])        # standardised factors
            normalized_data = await self.normalized_factors(eco_data)       # (percentile_scores, z_scores)
            composite = await self.calculate_composite(normalized_data[1])  # pipeline uses the z-score frame
            quandrant = await self.assign_quadrants(composite)              # + quadrant / median columns
            await self.store_quadrants(quandrant)                           # write per-country blobs to Redis
        except Exception as e:
            logging.error(f"Error updating the quandrant:{e}", exc_info=True)
            raise
        
    async def get_cross_section(self) -> dict:
        """Read the cached quadrant snapshot back out of Redis - the inverse of
        store_quadrants(). Returns
        {"meta": {...}, "countries": {code: {price, demand, composite,
        contrib_*, quadrant, price_median, demand_median}}} with whatever
        per-country blobs are still live. An empty "countries" dict means the
        cache has expired or update_quandrant() has never run.
        """
        try:
            # decode_responses=True on the pool means these come back as str
            # (or None when the key has expired / never existed).
            meta_raw = await self.redis.get(f"{QUADRANT_KEY_PREFIX}:_meta")
            meta = json.loads(meta_raw) if meta_raw else {}

            # Prefer the country list from _meta; if that blob is gone, scan
            # for the per-country keys directly (skipping _meta itself).
            codes = meta.get("countries")
            if not codes:
                prefix = f"{QUADRANT_KEY_PREFIX}:"
                codes = [
                    key[len(prefix):]
                    async for key in self.redis.scan_iter(match=f"{prefix}*")
                    if not key.endswith(":_meta")
                ]

            # Nothing cached at all - hand back an empty result rather than error.
            if not codes:
                return {"meta": meta, "countries": {}}

            # One GET per country, batched into a single round trip.
            pipeline = self.redis.pipeline()
            for code in codes:
                pipeline.get(f"{QUADRANT_KEY_PREFIX}:{code}")
            rows = await pipeline.execute()

            # Drop any country whose own key expired between the _meta read and
            # this pipeline (row is None).
            countries = {
                code: json.loads(row)
                for code, row in zip(codes, rows)
                if row is not None
            }
            return {"meta": meta, "countries": countries}
        except Exception as e:
            logging.error(f"Error getting cross section: {e}", exc_info=True)
            raise
    
# Manual smoke-test harness, kept commented out so importing this module has
# no side effects. Uncomment the stage(s) you want to exercise:
#   - the full write path builds the snapshot and stores it, then
#   - get_cross_section() reads it straight back from Redis.
# Requires a reachable Postgres and Redis (set REDIS_HOST for a local run).
# if __name__ == "__main__":
#     test = CrossSectionController()
#     # val = asyncio.run(test.load_data())
#     # val2 = asyncio.run(test.derived_economic_standard(val[0]))
#     # val3 = asyncio.run(test.normalized_factors(val2))
#     # val4 = asyncio.run(test.calculate_composite(val3[1]))

#     # quadrants = asyncio.run(test.assign_quadrants(val4))
#     # print(quadrants)
#     # asyncio.run(test.store_quadrants(quadrants))
#     print(asyncio.run(test.get_cross_section()))
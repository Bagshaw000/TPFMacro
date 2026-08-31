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
from controller.llm import LLMController
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

# Redis key + TTL for the LLM-written narrative of the current snapshot
# (store_cross_section_breakdown output). One JSON blob: the summary text plus
# when it was generated and which snapshot it describes.
BREAKDOWN_KEY = "cross_section:breakdown"
BREAKDOWN_TTL = 40 * 24 * 3600


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
        self.llm = LLMController()


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

                # Run z_score_country once per country (its full multi-month
                # sub-frame) and collect the results into a Series indexed by
                # country - the same index as scores_df, so the two frames
                # align column-for-column.
                z_scores_df[f'score_{indicator}'] = df.groupby('country').apply(
                lambda x: z_score_country(x)
            )
            # scores_df: cross-sectional percentile (vs peers this month).
            # z_scores_df: per-country z-score (vs that country's own history).
            return scores_df,z_scores_df

        except  Exception as e:
            logging.error(f"Error Normalizing derived factors: {e}", exc_info=True)
            raise
        
    async def calculate_composite(self, scores_df: pd.DataFrame):
        """Blend the four per-factor scores from normalized_factors() into the
        axes the quadrant view plots on.

        Input: one of the two frames normalized_factors() returns (columns
        score_cpiDev / score_ppi / score_unempGap / score_retMom, indexed by
        country). update_quandrant() passes the z-score frame; the arithmetic
        works on either.

        Every input score is already sign-adjusted so higher = healthier, so:
          - price   = 0.55*score_cpiDev + 0.45*score_ppi
                      -> HIGH price = inflation CONTAINED (at/below target, soft
                      producer prices); LOW price = inflation running hot.
          - demand  = 0.60*score_unempGap + 0.40*score_retMom
                      -> HIGH demand = tight labour market + accelerating real
                      retail; LOW demand = slack + stalling retail.
          - composite = all four scores weighted by IND['*']['weight']
                        (0.275/0.225/0.30/0.20, summing to 1.0) - one headline
                        health number.
          - contrib_* = each factor's weighted term of `composite`, kept
                        separately so a stacked bar can show the four pieces
                        (they add up to `composite`).

        NaN propagation: these are plain sums, so if any one input score is NaN
        for a country (an indicator was missing upstream), that country's
        price / demand / composite all come out NaN.
        """
        try:
            # One row per country, same index as the incoming scores frame.
            result = pd.DataFrame(index=scores_df.index)

            # 1. Price axis: CPI-vs-target score (55%) + producer-price score
            #    (45%). Rounded for a clean 0-100 display value.
            result['price'] = (0.55 * scores_df['score_cpiDev'] +
                            0.45 * scores_df['score_ppi']).round()

            # 2. Demand axis: unemployment-gap score (60%) + retail-momentum
            #    score (40%).
            result['demand'] = (0.60 * scores_df['score_unempGap'] +
                            0.40 * scores_df['score_retMom']).round()

            # 3. Composite: all four factor scores, each times its IND weight.
            result['composite'] = (
                scores_df['score_cpiDev'] * IND['cpiDev']['weight'] +
                scores_df['score_ppi'] * IND['ppi']['weight'] +
                scores_df['score_unempGap'] * IND['unempGap']['weight'] +
                scores_df['score_retMom'] * IND['retMom']['weight']
            ).round()

            # 4. The individual weighted terms behind `composite` (left
            #    unrounded so they still sum exactly to it) - for stacked-bar
            #    rendering on the frontend.
            result['contrib_cpi'] = scores_df['score_cpiDev'] * IND['cpiDev']['weight']
            result['contrib_ppi'] = scores_df['score_ppi'] * IND['ppi']['weight']
            result['contrib_unemp'] = scores_df['score_unempGap'] * IND['unempGap']['weight']
            result['contrib_ret'] = scores_df['score_retMom'] * IND['retMom']['weight']

            return result

        except Exception as e:
            logging.error(f"Error calculating composite score", exc_info=True)
            raise
        
    async def assign_quadrants(self, composite_df: pd.DataFrame) -> pd.DataFrame:
        """Place each country in one quadrant of the price/demand plane,
        splitting each axis at the cross-section median.

        Mutates `composite_df` in place - adds `quadrant`, `price_median`,
        `demand_median` - and returns it.

        As coded, the four cases map to:
            price above median + demand above median -> "Overheating"
            price above median + demand below median -> "Weak"
            price below median + demand above median -> "Goldilocks"
            price below median + demand below median -> "Stagflation"

        KNOWN LABEL BUG: `price` is health-oriented (high = inflation
        *contained*, low = inflation *hot* - see calculate_composite), so the
        economic reality of the top-left/top-right cases is the reverse of the
        names: "price high + demand high" is actually Goldilocks (firm demand,
        no inflation) and "price low + demand high" is actually Overheating.
        Only "Stagflation" is labelled correctly; "Weak" is roughly right.
        Swap the 'Overheating' and 'Goldilocks' strings in `choices` to fix.

        NaN caveat: a country with NaN price or demand (missing indicator
        upstream) compares False on every '>' test, so it falls into the
        low/low branch and gets a real label rather than the `default`
        'Unclassified'. Filter on notna() before trusting a gap-prone
        country's quadrant.
        """
        try:
            # Split lines: median price and median demand across all countries
            # in this cross-section (NaN-skipping).
            price_median = composite_df['price'].median()
            demand_median = composite_df['demand'].median()

            # Per-country boolean masks, evaluated for every row at once.
            price_high = composite_df['price'] > price_median
            demand_high = composite_df['demand'] > demand_median

            # np.select takes the first condition that is True for each row.
            # The four cases are mutually exclusive and cover every non-NaN row.
            conditions = [
                (price_high & demand_high),      # -> choices[0]
                (price_high & ~demand_high),     # -> choices[1]
                (~price_high & demand_high),     # -> choices[2]
                (~price_high & ~demand_high)     # -> choices[3]
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
            per_country = json.loads(composite_df.to_json(orient="index"))

            medians = composite_df[["price", "demand"]].median()
            meta = {
                "price_median": None if pd.isna(medians["price"]) else float(medians["price"]),
                "demand_median": None if pd.isna(medians["demand"]) else float(medians["demand"]),
                "countries": [str(c) for c in composite_df.index],
                "updated": pd.Timestamp.now(tz="UTC").isoformat(),
            }

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
        indicator, then blend those ranks into one composite per country per
        quarter. This is the "compare all economies over time" view, separate
        from the single-snapshot Phillips-curve quadrant above.

        Returns {'quarters': [Timestamp, ...],
                 'per_indicator': {indicator: DataFrame(country x quarter)},
                 'composite': DataFrame(country x quarter)}.

        TWO KNOWN ISSUES with the code as it currently stands:

        1. Input shape. `full_df.resample('Q')` and
           `quarterly.xs(q, level='date')` require `full_df` to carry a
           quarterly-resamplable DatetimeIndex with a (date, country) level
           structure. load_data() returns a long-form frame with a plain
           RangeIndex and date/country as columns, so passing that directly
           raises "Only valid with DatetimeIndex...". It must be reshaped
           (set a DatetimeIndex, group by country) first.

        2. Key mismatch. The loop iterates `IND` (keys cpiDev / ppi /
           unempGap / retMom) and indexes `quarterly[indicator]`, but the
           agg below produces columns cpi / ppi / retail / unemp - so only
           'ppi' lines up. Step 5 then reads per_indicator['cpi'] /
           ['unemp'] / ['retail'], which the IND-keyed loop never created.
           Either agg into cpiDev/unempGap/retMom or iterate the raw column
           names.
        """
        try:
            # 1. Collapse each quarter to one number per indicator.
            #    cpi/ppi/retail: percent change from the quarter's first
            #    reading to its last. unemp: the quarter's final reading.
            quarterly = full_df.resample('Q').agg({
                'cpi': lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100,  # Quarter change
                'ppi': lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100,
                'retail': lambda x: (x.iloc[-1] / x.iloc[0] - 1) * 100,
                'unemp': 'last'
            })

            # 2. Keep only the most recent `quarters` rows.
            last_n_quarters = quarterly.tail(quarters)

            # 3. Per indicator, build a country x quarter frame of
            #    cross-sectional percentile ranks (sign-adjusted via IND so
            #    higher = healthier, same convention as the snapshot pipeline).
            per_indicator = {}
            composite = pd.DataFrame(index=quarterly.index.get_level_values('country').unique())

            for indicator, config in IND.items():
                quarter_ranks = []
                for q in last_n_quarters.index:
                    # All countries' values for this one quarter.
                    values = quarterly.xs(q, level='date')[indicator]
                    signed_values = values * config['sign']       # flip so bigger = healthier
                    ranks = signed_values.rank(pct=True) * 100     # 0-100 percentile
                    quarter_ranks.append(ranks)

                # One column per quarter, labelled by the quarter timestamp.
                ranks_df = pd.concat(quarter_ranks, axis=1)
                ranks_df.columns = last_n_quarters.index
                per_indicator[indicator] = ranks_df

            # 4. Weighted blend of the four indicator ranks, quarter by quarter
            #    (weights: 0.275 / 0.225 / 0.30 / 0.20, summing to 1.0).
            for i, q in enumerate(last_n_quarters.index):
                composite[q] = (
                    per_indicator['cpi'].iloc[:, i] * 0.275 +
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
        """Run the whole Phillips-curve / quadrant pipeline end to end and
        cache the results. Meant to be triggered on a schedule (e.g. after the
        monthly release batch lands). Read the cached output back with
        get_cross_section().
        """
        try:
            data = await self.load_data()                                   # (df_by_country, country_stats)
            eco_data = await self.derived_economic_standard(data[0])        # cpiDev / ppi / unempGap / retReal / retMom
            normalized_data = await self.normalized_factors(eco_data)       # (percentile_scores, z_scores)
            composite = await self.calculate_composite(normalized_data[1])  # z-score frame -> price / demand / composite
            quandrant = await self.assign_quadrants(composite)              # + quadrant / median columns
            await self.store_quadrants(quandrant)                           # write per-country blobs to Redis

            # Both narration steps only READ the snapshot store_quadrants just
            # wrote and write to disjoint keys (BREAKDOWN_KEY vs
            # BREAKDOWN_KEY:{code}), so run them together. return_exceptions:
            # a failed LLM narration is logged but must not undo a successful
            # quadrant store or abort the sibling narration.
            narration = await asyncio.gather(
                self.store_cross_section_breakdown(),      # global LLM narrative
                self.store_cross_section_by_country(),     # per-country LLM narratives
                return_exceptions=True,
            )
            for result in narration:
                if isinstance(result, Exception):
                    logging.error(f"Cross-section narration step failed: {result}", exc_info=result)
        except Exception as e:
            logging.error(f"Error updating the quandrant:{e}", exc_info=True)
            raise
        
    async def get_cross_section(self, include_summary: bool = True) -> dict:
        """Read the cached quadrant snapshot back out of Redis - the inverse of
        store_quadrants(). Returns
        {"meta": {...}, "countries": {code: {price, demand, composite,
        contrib_*, quadrant, price_median, demand_median}}, "summary": {...}}
        with whatever per-country blobs are still live. An empty "countries"
        dict means the cache has expired or update_quandrant() has never run.

        `summary` is the cached LLM narrative
        (store_cross_section_breakdown output: {"summary", "updated",
        "source_updated"}) or None if none is cached. Pass
        include_summary=False to skip that read - store_cross_section_breakdown
        does, so the narrative it feeds the LLM never contains a previous
        narrative.
        """
        try:
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

            if codes:
                pipeline = self.redis.pipeline()
                for code in codes:
                    pipeline.get(f"{QUADRANT_KEY_PREFIX}:{code}")
                rows = await pipeline.execute()
                countries = {
                    code: json.loads(row)
                    for code, row in zip(codes, rows)
                    if row is not None
                }
            else:
                countries = {}

            result = {"meta": meta, "countries": countries}
            if include_summary:
                result["summary"] = await self.get_cross_section_breakdown() or None
            return result
        except Exception as e:
            logging.error(f"Error getting cross section: {e}", exc_info=True)
            raise
        
    async def get_cross_section_by_country(self, country: str) -> dict | None:
        """Read one country's cached quadrant row plus its LLM narrative from
        Redis in a single pipelined round trip:
          - f"{QUADRANT_KEY_PREFIX}:{code}" -> {price, demand, composite,
            contrib_*, quadrant, price_median, demand_median} (store_quadrants).
            price_median / demand_median are stored per row, so this result is
            self-contained (no _meta read needed).
          - f"{BREAKDOWN_KEY}:{code}" -> {summary, updated, source_updated}
            (store_cross_section_by_country), folded in under "summary".

        Returns the quadrant row with "summary" added (None if that country's
        narrative isn't cached), or None if the country isn't in the current
        snapshot at all.
        """
        try:
            code = country.upper()

            # Both GETs in one round trip.
            pipe = self.redis.pipeline()
            pipe.get(f"{QUADRANT_KEY_PREFIX}:{code}")
            pipe.get(f"{BREAKDOWN_KEY}:{code}")
            data, summary_raw = await pipe.execute()

            if not data:
                return None

            # decode_responses=True -> str, so json.loads (parse a string),
            # not json.load (which expects a file-like object).
            row = json.loads(data)
            row["summary"] = json.loads(summary_raw) if summary_raw else None
            return row
        except Exception as e:
            logging.error(f"Error getting cross section for {country}: {e}", exc_info=True)
            raise
    
    async def store_cross_section_by_country(self, ttl: int = BREAKDOWN_TTL) -> dict:
        """For every country in the current snapshot, have the LLM explain that
        country's slot in the Phillips-curve cross-section
        (LLMController.breakdown_by_country) and cache each explanation in Redis
        at f"{BREAKDOWN_KEY}:{code}" as {"summary", "updated", "source_updated"}.

        Returns {code: summary_text} for the countries that produced one.

        The per-country breakdown_by_country coroutines are launched together
        with asyncio.gather (return_exceptions=True, so one country failing is
        logged and skipped rather than sinking the batch).

        Caveat: breakdown_by_country still calls requests.post synchronously,
        which blocks the event loop for each HTTP request - so the POSTs
        themselves are not truly concurrent, only whatever the coroutine
        awaits around them. For genuine overlap switch that call to an async
        HTTP client (httpx.AsyncClient).
        """
        try:
            # Country list comes from the snapshot's _meta blob (written by
            # store_quadrants). Missing/expired _meta -> nothing to do.
            meta_raw = await self.redis.get(f"{QUADRANT_KEY_PREFIX}:_meta")
            meta = json.loads(meta_raw) if meta_raw else {}
            codes = meta.get("countries") or []
            if not codes:
                logging.info("No cross-section snapshot to break down by country")
                return {}

            # Pull every country's quadrant row in one pipelined round trip
            # rather than a GET per country.
            read_pipe = self.redis.pipeline()
            for code in codes:
                read_pipe.get(f"{QUADRANT_KEY_PREFIX}:{code}")
            rows = await read_pipe.execute()

            # Only the countries whose row is still live, each paired with its
            # parsed data.
            present = [(code, json.loads(raw)) for code, raw in zip(codes, rows) if raw]
            if not present:
                return {}

            # Launch one breakdown_by_country coroutine per country and wait
            # for all of them. return_exceptions=True so one country's failure
            # doesn't sink the rest - it's logged and skipped below.
            texts = await asyncio.gather(
                *(self.llm.breakdown_by_country(row, code) for code, row in present),
                return_exceptions=True,
            )

            # Stamp every explanation from this run with the same timestamps:
            # `updated` = now, `source_updated` = the snapshot they describe.
            now = pd.Timestamp.now(tz="UTC").isoformat()
            source_updated = meta.get("updated")

            summaries: dict = {}
            write_pipe = self.redis.pipeline()
            for (code, _row), text in zip(present, texts):
                if isinstance(text, Exception):
                    logging.error(f"breakdown_by_country failed for {code}: {text}")
                    continue
                if not text:
                    continue
                summaries[code] = text
                write_pipe.set(
                    f"{BREAKDOWN_KEY}:{code}",
                    json.dumps({
                        "summary": text,
                        "updated": now,
                        "source_updated": source_updated,
                    }),
                    ex=ttl,
                )

            # One round trip for all the writes (no-op if nothing was produced).
            if summaries:
                await write_pipe.execute()
            return summaries
        except Exception as e:
            logging.error(f"Error storing country cross section: {e}", exc_info=True)
            raise

    async def get_cross_section_breakdown_by_country(self, country: str) -> dict:
        """Read one country's cached LLM explanation - the per-country inverse
        of store_cross_section_by_country(). Returns the stored
        {"summary", "updated", "source_updated"} dict, or {} if none is cached.
        """
        try:
            raw = await self.redis.get(f"{BREAKDOWN_KEY}:{country.upper()}")
            return json.loads(raw) if raw else {}
        except Exception as e:
            logging.error(f"Error getting country breakdown for {country}: {e}", exc_info=True)
            raise
    
    async def store_cross_section_breakdown(self, ttl: int = BREAKDOWN_TTL) -> str | None:
        """Read the cached quadrant snapshot, have the LLM write a narrative of
        it (global_breakdown), and cache that narrative in Redis at
        BREAKDOWN_KEY as {"summary", "updated", "source_updated"}. Returns the
        summary text (or None if there was nothing to summarise).
        """
        try:
            # include_summary=False: feed the LLM only the raw snapshot, never
            # a previously cached narrative.
            data = await self.get_cross_section(include_summary=False)

            # Nothing cached to describe -> don't spend an LLM call or overwrite
            # a good previous narrative with an empty one.
            if not data.get("countries"):
                logging.info("No cross-section snapshot to summarise")
                return None

            summary = await self.llm.global_breakdown(data)
            if not summary:
                logging.info("Empty cross-section summary from LLM")
                return None

            payload = {
                "summary": summary,
                "updated": pd.Timestamp.now(tz="UTC").isoformat(),
                # tie the narrative to the snapshot it was written from
                "source_updated": data.get("meta", {}).get("updated"),
            }
            await self.redis.set(BREAKDOWN_KEY, json.dumps(payload), ex=ttl)
            return summary
        except Exception as e:
            logging.error(f"Error break down cross section breakdown: {e}", exc_info=True)
            raise

    async def get_cross_section_breakdown(self) -> dict:
        """Read the cached LLM narrative back out of Redis - the inverse of
        store_cross_section_breakdown(). Returns the stored
        {"summary", "updated", "source_updated"} dict, or {} if none is cached.
        """
        try:
            raw = await self.redis.get(BREAKDOWN_KEY)
            return json.loads(raw) if raw else {}
        except Exception as e:
            logging.error(f"Error getting cross section breakdown: {e}", exc_info=True)
            raise
    
    

# Manual smoke-test harness, guarded so importing this module has no side
# effects. Needs a reachable Postgres + Redis (set REDIS_HOST for a local run)
# and, for store_cross_section_breakdown, a working LLM key.
#
# The active line summarises whatever snapshot is already cached. To rebuild
# the snapshot first, run update_quandrant() instead - or step through the
# pipeline stage by stage using the commented lines below.
# if __name__ == "__main__":
#     test = CrossSectionController()
#     print(asyncio.run(test.store_cross_section_by_country()))
#     # val = asyncio.run(test.load_data())
#     # val2 = asyncio.run(test.derived_economic_standard(val[0]))
#     # val3 = asyncio.run(test.normalized_factors(val2))
#     # val4 = asyncio.run(test.calculate_composite(val3[1]))

#     # quadrants = asyncio.run(test.assign_quadrants(val4))
#     # print(quadrants)
#     # asyncio.run(test.store_quadrants(quadrants))
#     print(asyncio.run(test.get_cross_section()))
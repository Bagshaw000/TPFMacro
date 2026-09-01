import asyncio
import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
from typing import Any, List
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import text
from database.db import session_scope
from custom_types.cpi import ECON_INDICATOR_TYPES, LSEType, PPIType, _EconIndicatorWithForecast, countries

# Columns that identify a row for upsert purposes (matches each table's
# UniqueConstraint('country_code', 'period')) - never overwritten. `period` is
# the reference month, so a flash / final / revision for the same month all
# target the same row; `report_date` is now a regular column and DOES get
# updated on conflict, so the latest publication wins.
_CONFLICT_COLUMNS = ("country_code", "period")


class LSEModel:
    
    async def get_last_report(self, table:str):
        try:
            async with session_scope() as session:
                table_name = table+"_recent"
                query = text(f"""
                       SELECT * FROM {table_name}      
                             
                             """)
                
                result = await session.exec(query)
                
                
                rows  = result.mappings().all()
                
                
                if not rows:
                    return table,[]
                
                return table,[LSEType.model_validate(row) for row in rows]
            
        except Exception as e:
            logging.error(f"Error getting the last PPI report for each country: {e}", exc_info=True)
            raise

    async def get_trailing_stats(self, table: str, years: int = 5) -> dict[str, dict[str, float]]:
        """Trailing mean/stdev of `index_value` per country for `table`,
        over the last `years` years - the (mu, sigma) input the economic-
        cycle z-score calculation standardizes each factor's latest
        reading against (see controller/macro.py's refresh_factor_stats).

        Computed with AVG/STDDEV_SAMP in Postgres (one query per factor)
        rather than pulling full row history into Python, since only the
        two aggregate numbers per country are actually needed.
        """
        try:
            if table not in ECON_INDICATOR_TYPES:
                raise ValueError(f"Unknown indicator table: {table}")

            cutoff = datetime.now() - timedelta(days=365 * years)

            async with session_scope() as session:
                # `table` is only ever one of ECON_INDICATOR_TYPES' own
                # keys (validated above), never raw user input, so
                # interpolating it as an identifier here is safe - bind
                # params only cover values (like `cutoff`), not table names.
                query = text(f"""
                    SELECT country_code,
                           AVG(index_value) AS mu,
                           STDDEV_SAMP(index_value) AS sigma
                    FROM {table}
                    WHERE report_date >= :cutoff
                    GROUP BY country_code
                """).bindparams(cutoff=cutoff)

                result = await session.exec(query)
                rows = result.mappings().all()

            return {
                row["country_code"]: {
                    "mu": float(row["mu"]) if row["mu"] is not None else 0.0,
                    "sigma": float(row["sigma"]) if row["sigma"] is not None else 0.0,
                }
                for row in rows
            }

        except Exception as e:
            logging.error(f"Error getting trailing stats for {table}: {e}", exc_info=True)
            raise

    async def get_monthly_series(self, table: str, months: int = 72) -> dict[str, List[tuple]]:
        """The last `months` monthly (report_date, index_value) readings per
        country for `table`, oldest -> newest - paired with each reading's
        actual release date, not bare values, so a caller can tell how
        fresh a series is (e.g. detect a stalled sync) or align chained
        output back to real calendar months, instead of having to assume
        the series is gap-free and its last point is "now". Raw input for
        chaining a MoM series into an approximate YoY series (see
        controller/macro.py's _mom_to_yoy), since the LSE-sourced
        cpi/ppi/retail/inflation releases are all month-over-month, not
        year-over-year.

        Fetches every row within a generous cutoff (a few months more than
        requested, to absorb irregular release timing) in one query, then
        groups/sorts/trims to the last `months` per country in Python -
        cheaper than a per-country windowed query for the country counts
        this app tracks.
        """
        try:
            if table not in ECON_INDICATOR_TYPES:
                raise ValueError(f"Unknown indicator table: {table}")

            cutoff = datetime.now() - timedelta(days=31 * (months + 2))

            async with session_scope() as session:
                query = text(f"""
                    SELECT country_code, report_date, index_value
                    FROM {table}
                    WHERE report_date >= :cutoff
                    ORDER BY country_code, report_date
                """).bindparams(cutoff=cutoff)

                result = await session.exec(query)
                rows = result.mappings().all()

            by_country: dict[str, list[tuple[datetime, float]]] = {}
            for row in rows:
                if row["index_value"] is None:
                    continue
                by_country.setdefault(row["country_code"], []).append(
                    (row["report_date"], row["index_value"])
                )

            return {
                country: sorted(points)[-months:]
                for country, points in by_country.items()
            }

        except Exception as e:
            logging.error(f"Error getting monthly series for {table}: {e}", exc_info=True)
            raise

    async def insert_event(self, data: List[_EconIndicatorWithForecast]) -> List[_EconIndicatorWithForecast]:
        try:
            if not data:
                return []

            # __table__ is injected onto table=True subclasses by SQLModel's
            # metaclass at runtime; it's not declared on the base class, so
            # the type checker needs a hint here.
            model_class: Any = data[0].__class__
            table = model_class.__table__

            # Postgres refuses to UPDATE the same conflict target twice within
            # one INSERT command, so the batch itself must be free of duplicate
            # (country_code, period) pairs - a country whose flash and final
            # for one month both land in the same fetch, or overlapping
            # catch-up windows, will produce them. Keep the row with the newest
            # report_date so the batch's winner matches the DB's.
            deduped: dict[tuple, _EconIndicatorWithForecast] = {}
            for record in data:
                key = (record.country_code, record.period)
                existing = deduped.get(key)
                if existing is None or record.report_date > existing.report_date:
                    deduped[key] = record
            values = [record.model_dump(exclude={"id"}) for record in deduped.values()]

            stmt = pg_insert(table).values(values)
            update_cols = {
                col.name: getattr(stmt.excluded, col.name)
                for col in table.columns
                if col.name not in _CONFLICT_COLUMNS and col.name != "id"
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=list(_CONFLICT_COLUMNS),
                set_=update_cols,
                # Only overwrite when the incoming row is at least as recent as
                # the stored one, so a late-arriving older publication (e.g. a
                # re-fetched flash) can't clobber a newer revision.
                where=(table.c.report_date <= stmt.excluded.report_date),
            )

            async with session_scope() as session:
                await session.execute(stmt)

            return list(deduped.values())

        except Exception as e:
            logging.error(f"Error inserting data into the table: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    test = LSEModel()
    print(asyncio.run(test.get_monthly_series("inflation")))
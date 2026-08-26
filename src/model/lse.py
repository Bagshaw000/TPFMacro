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
# UniqueConstraint('report_date', 'country_code', ...)) - never overwritten.
_CONFLICT_COLUMNS = ("report_date", "country_code")


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
            # one INSERT command, so the batch itself must be free of
            # duplicate (report_date, country_code) pairs - e.g. a country
            # appearing more than once in `recent_event` can produce
            # overlapping fetch windows that land the same release twice.
            deduped: dict[tuple, _EconIndicatorWithForecast] = {}
            for record in data:
                key = (record.report_date, record.country_code)
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
            )

            async with session_scope() as session:
                await session.execute(stmt)

            return list(deduped.values())

        except Exception as e:
            logging.error(f"Error inserting data into the table: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    test = LSEModel()
    print(asyncio.run(test.get_trailing_stats("cpi")))
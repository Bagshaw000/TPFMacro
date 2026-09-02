"""Postgres accessor for the COT (Commitment of Traders) tables.

`CotModell` is the only place raw SQL against `cot_ttf` / `cot_last_entry`
lives. The COT ingest (src/cot.py) writes through `insert_tff_report` /
`update_ttf_report`; the analytics side (controller/cot.py) reads through the
`get_*` methods:

  - get_last_report / get_last_entry  : newest stored row(s), for catch-up sync
  - get_all_last_year_cot             : ~60 weeks per instrument (Redis warm-up)
  - get_symbol_last_year_cot          : ~1 year for one instrument
  - get_distinct_instruments          : every (market, name) pair - the universe

Every method runs inside `session_scope()` (auto commit / rollback).
"""

import asyncio
from datetime import datetime
import logging
logger = logging.getLogger(__name__)
import os
import sys
from typing import List
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_types.cot import CFTCData, CotData
from database.db import db_connect, init_db_schemas, session_scope, cot_ttf_table, cot_last_entry
from sqlmodel import DateTime, String, Table, cast, col, func, insert, select, text


# Reads and writes for every COT instrument.
class CotModell:
    # global cot_last_entry
   
   
    
    async def get_last_report(self)->CotData | None :
        """The single most recent row across the whole table (max report_date),
        or None if empty."""
        try:
            async with session_scope() as session:
            
                async with session.begin():
                    query = select(CotData).order_by(col(CotData.report_date_as_yyyy_mm_dd).desc()).limit(1)
                    row = await session.exec(query)
                    
                    result = row.first()
                    if not result:
                        return None
                    
                    return result
            
        except Exception as e:
            logger.error("Error getting the last report")
            raise
        
    async def insert_tff_report(self, data:List[CotData]):
        """Plain bulk insert (no conflict handling) - for the first-time table
        fill. Refreshes each row so callers see the generated ids."""
        try:
            async with session_scope() as session:
                async with session.begin():
                    
                    session.add_all(data)
                    await session.flush()
                    for record in data:
                        await session.refresh(record)
                    return data
            
            
        except Exception as e:
            logger.error(f"Error inserting cot report: {e}")
            raise
    
    
    async def get_latest_cot_data(self, start:int, end:int)-> List[CotData]:
        """Newest-first page of rows: `offset(start).limit(end)`. NOTE `end` is
        used as the page size, not an absolute end index."""
        try:
            async with session_scope() as session:
                async with session.begin():
                    query= select(CotData).order_by(col(CotData.report_date_as_yyyy_mm_dd).desc()).offset(start).limit(end)

                    result = await session.exec(query)
                    
                    return list(result.all())
        except Exception as e:
            logger.error(f"Error getting latest cot data {e}")
            raise
        
        
    async def get_cot_data_size(self)->int | None:
        """Total row count in `cot_ttf`."""
        try:
            async with session_scope() as session:
                async with session.begin():
                    count_query = select(func.count()).select_from(CotData)
                    
                    total = await session.exec(count_query)
                    total = total.one()

                    if not total :
                        return None
                    
                    return total
        except Exception as e:
            logger.error(f"Error getting cot database size: {e}")
            raise
        
        
    async def update_ttf_report(self,data:List[CotData]):
        """Upsert by (market_and_exchange_names, report_date): update the row's
        columns in place if it exists (except id / created_at), else insert.
        Returns a {status, inserted, updated, total} summary. Does one SELECT
        per row - fine for the weekly delta, not for a bulk load."""
        try:

            async with session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for item in data:
                    if not item.report_date_as_yyyy_mm_dd:
                        continue
                    
                    
                # ✅ Use execute() not exec()
                    result = await session.exec(
                        select(CotData).where(
                            CotData.market_and_exchange_names == item.market_and_exchange_names,
                            CotData.report_date_as_yyyy_mm_dd == item.report_date_as_yyyy_mm_dd
                        )
                    )
                   
                    existing_record = result.first()
                
                    if existing_record:
                        # Update existing record
                    
                        for key, value in item.model_dump().items():
                            if hasattr(existing_record, key) and key not in ['id', 'created_at']:
                                setattr(existing_record, key, value)
                        updated_count += 1
                    else:
                        
                        session.add(item)
                        inserted_count += 1

                await session.commit()
                
                return {
                "status": "success",
                "inserted": inserted_count,
                "updated": updated_count,
                "total": len(data)
            }
            
        except Exception as e:
            logger.error(f"Error updating TFF report : {e}")
            raise
        
    async def get_last_entry(self)->List[CotData] :
        """One row per instrument - the latest stored report for each - from the
        `cot_last_entry` DB view. `update_cot` uses this to decide which
        contracts have a newer weekly report to pull."""
        try:

            async with session_scope() as session:

                query = text("""SELECT *, TO_CHAR(report_date_as_yyyy_mm_dd, 'YYYY-MM-DD') as report_date_as_yyyy_mm_dd   FROM cot_last_entry""")
                
                result = await session.exec(query)
               
                rows = result.mappings().all()
                
                if not rows:
                    return []
                
            
                return [CotData.model_validate(row) for row in rows ]
                
        
        except Exception as e:
            logger.error(f"Error getting the last entry: {e}", exc_info=True)
            raise 
        
        
    async def get_symbol_last_year_cot(self, asset_name)-> List[CotData]:
        """The last 53 weekly reports (~1 year) for one instrument, newest-first."""
        try:
            async with session_scope() as session:
                async with session.begin():
                    query= select(CotData).where(CotData.market_and_exchange_names == asset_name).order_by(col(CotData.report_date_as_yyyy_mm_dd).desc()).limit(53)

                    result = await session.exec(query)
                    
                    return list(result.all())   
        except Exception as e:
            logger.error(f"Error getting last year of cot data for {asset_name}: {e}", exc_info=True)  
            raise
        
    async def get_all_last_year_cot(self)->List[CotData]:
        """The last 60 weekly reports for EVERY instrument in one query (a
        LATERAL join per distinct name). This is what COTController.setup_redis
        / get_cot_data use to warm the `cot_ttf:*` Redis hashes from cold."""
        try:
            async with session_scope() as session:
                async with session.begin():
                    query = text("""SELECT
                            symbols.market_and_exchange_names,
                            latest_60.*
                        FROM (
                            SELECT DISTINCT market_and_exchange_names
                            FROM cot_ttf
                            ORDER BY market_and_exchange_names
                        ) symbols
                        CROSS JOIN LATERAL (
                            SELECT *
                            FROM cot_ttf c
                            WHERE c.market_and_exchange_names = symbols.market_and_exchange_names
                            ORDER BY c.report_date_as_yyyy_mm_dd DESC
                            LIMIT 60
                        ) latest_60
                        ORDER BY symbols.market_and_exchange_names, latest_60.report_date_as_yyyy_mm_dd DESC;
                        """)
                    result = await session.exec(query)
                    
                    rows = result.all()
                                    
                    if not rows:
                        return []
                    
                    return [CotData.model_validate(row) for row in rows ]
            
        except Exception as e:
            logger.error(f"Error getting all cot data")
            raise

    async def get_distinct_instruments(self) -> List[tuple[str, str]]:
        """Every (market, market_and_exchange_names) pair in cot_ttf - the full
        instrument universe. `market` is included because the Redis keys are
        cot_ttf:{market}:{name}:{date}, so a caller needs it to build the scan
        pattern. Rows with either column NULL are skipped.
        """
        try:
            async with session_scope() as session:
                query = text("""
                    SELECT DISTINCT market, market_and_exchange_names
                    FROM cot_ttf
                    WHERE market IS NOT NULL
                      AND market_and_exchange_names IS NOT NULL
                """)
                result = await session.exec(query)
                return [
                    (row["market"], row["market_and_exchange_names"])
                    for row in result.mappings().all()
                ]
        except Exception as e:
            logger.error(f"Error getting distinct cot instruments: {e}", exc_info=True)
            raise

# test = CotModell()
# val = asyncio.run(test.get_all_last_year_cot())

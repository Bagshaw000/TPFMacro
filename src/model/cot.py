import asyncio
from datetime import datetime
import logging
import os
import sys
from typing import List

from sqlmodel import Table, col, func, insert, select, text
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from custom_types.cot import CFTCData, CotData
from database.db import db_connect, init_db_schemas, session_scope, cot_ttf_table, cot_last_entry
# from main import init_db_schemas, cot_ttf_table, cot_last_entry


# class CotModel:
    
#     def __init__(self):
#         self.conn = db_connect()
       
        
#     # Get the last report 
#     async def get_last_report(self):
#         try:

#             data =  self.conn.table('cot_ttf').select("*").order("Market_and_Exchange_Names").order("Report_Date_as_YYYY_MM_DD", desc=True).limit(1).execute()

#             return data
#         except Exception as e:
#             logging.error(f"error getting last report : {e}")
   
#     # Insert the cot tff report 
#     async def insert_tff_report(self, data:list):
#         try:
#             # await self.conn.db.execute("SET statement_timeout = '10min'")
#             response = self.conn.table("cot_ttf").insert(data).execute()
            
#             return response
            
#         except Exception as e:
#             logging.error(f"Error inserting TFF report : {e}",exc_info=True)
            
#     async def update_ttf_report(self, data:list):
#         try:
#             # await self.conn.db.execute("SET statement_timeout = '10min'")
#             response = self.conn.table("cot_ttf").upsert(data,  on_conflict="Market_and_Exchange_Names, Report_Date_as_YYYY_MM_DD").execute()
            
#             return response
            
#         except Exception as e:
#             logging.error(f"Error inserting TFF report : {e}")
            
#     # Get the latest Cot data for all instruments   
#     async def get_latest_cot_data(self, start, end):
#         try:
            
#             response = self.conn.table("cot_ttf").select("*",count="exact").order("Report_Date_as_YYYY_MM_DD", desc=True).range(start,end).execute()
            
#             return response 
#         except Exception as e:
#             logging.error(f"Error returning the latest cot data : {e}")
            
#     async def get_cot_data_size(self):
#         try:
#             response = self.conn.table("cot_ttf").select("*" ,count="exact").execute()
            
#             return response
#         except Exception as e:
#             logging.error(f"Error getting number of row : {e}", exc_info=True)

#     async def get_last_entry(self):
#         try:
#             response = self.conn.table("last_entry").select("*").execute()
            
#             return response
#         except Exception as e:
#             logging.error(f"Error getting the last entry", exc_info=True)
          
    # Get the all cot Data for all Instruments
            
class CotModell:
    # global cot_last_entry 
   
   
    
    async def get_last_report(self)->CotData | None :
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
            logging.error("Error getting the last report")
            raise
        
    async def insert_tff_report(self, data:List[CotData]):
        try:
            async with session_scope() as session:
                async with session.begin():
                    
                    session.add_all(data)
                    await session.flush()
                    for record in data:
                        await session.refresh(record)
                    return data
            
            
        except Exception as e:
            logging.error(f"Error inserting cot report: {e}")
            raise
    
    
    async def get_latest_cot_data(self, start:int, end:int)-> List[CotData]:
        try:
            async with session_scope() as session:
                async with session.begin():
                    query= select(CotData).order_by(col(CotData.report_date_as_yyyy_mm_dd).desc()).offset(start).limit(end)

                    result = await session.exec(query)
                    
                    return list(result.all())
        except Exception as e:
            logging.error(f"Error getting latest cot data {e}")
            raise
        
        
    async def get_cot_data_size(self)->int | None:
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
            logging.error(f"Error getting cot database size: {e}")
            raise
        
        
    async def update_ttf_report(self,data:List[CotData]):
        try:
            
            async with session_scope() as session:
                inserted_count = 0
                updated_count = 0
                
                for item in data:
                    existing = await session.exec(
                    select(CotData).where(
                        CotData.market_and_exchange_names == item.market_and_exchange_names,
                        CotData.report_date_as_yyyy_mm_dd == item.report_date_as_yyyy_mm_dd
                    )
                )
                    existing_record = existing.first()
                
                    if existing_record:
                        # Update existing record
                        for key, value in item.model_dump().items():
                            if hasattr(existing_record, key) and key not in ['id', 'created_at']:
                                setattr(existing_record, key, value)
                        updated_count += 1
                    else:
                        # Insert new record
                        new_record = CotData(**item.model_dump())
                        session.add(new_record)
                        inserted_count += 1

                await session.commit()
                
                return {
                "status": "success",
                "inserted": inserted_count,
                "updated": updated_count,
                "total": len(data)
            }
            
        except Exception as e:
            logging.error(f"Error updating TFF report : {e}")
            raise
        
    async def get_last_entry(self)->List[CotData] | None:
        try:
            
            async with session_scope() as session:
                
                        
                query = text("""SELECT *,
                                TO_CHAR(report_date_as_yyyy_mm_dd, 'YYYY-MM-DD') 
                                as report_date_as_yyyy_mm_dd 
                                FROM cot_last_entry""")
                
                result = await session.exec(query)
                
                rows = result.mappings().all()
                
                if not rows:
                    return []
            
                return [CotData.model_validate(row) for row in rows ]
                    
            
        except Exception as e:
            logging.error(f"Error getting the last entry: {e}")
     
            
# test = CotModell()
# loop = asyncio.get_event_loop()

# if loop.is_running():
#     # If loop is already running, schedule the coroutine
#     val = asyncio.create_task(test.get_last_entry())
#     print(val)
# else:
#     # If no loop is running, run it synchronously
#     val = asyncio.run(test.get_last_entry())

#     print(val)
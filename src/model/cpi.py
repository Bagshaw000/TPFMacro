import asyncio
import logging
import os
import sys

from sqlmodel import text
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List
from database.db import session_scope
from custom_types.cpi import CPIType


class CPIModel:
    
    async def get_last_report(self)->List[CPIType]:
        try:
            async with session_scope() as session:
                query = text("""
                       SELECT * FROM cpi_recent      
                             
                             """)
                
                result = await session.exec(query)
                
                
                rows  = result.mappings().all()
                
                
                if not rows:
                    return []
                
                return [CPIType.model_validate(row) for row in rows ]
            
        except Exception as e:
            logging.error(f"Error getting the last CPI report for each country: {e}", exc_info=True)
            raise
        
    async def insert_cpi_report(self, data: List[CPIType]):
        try:
            async with session_scope() as session:
                async with session.begin():
                    
                    session.add_all(data)
                    await session.flush()
                    
                    for record in data:
                        await session.refresh(record)
                    return data
        except Exception as e:
            logging.error(f"Error inserting into the CPI table {e}", exc_info=True)
            raise

# test = CPIModel()


# loop = asyncio.get_event_loop()

# if loop.is_running():
# #     # If loop is already running, schedule the coroutine
#     val = asyncio.run(test.get_last_report())
#     print(val)
# else:
#     # If no loop is running, run it synchronously
#     val = asyncio.run(test.get_last_report())
#     print(val)
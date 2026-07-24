import logging
import os
import sys
from sqlmodel import text
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List
from database.db import session_scope
from custom_types.cpi import GDPType

class GDPModel:
    
    async def get_last_report(self)-> List[GDPType]:
        try:
            async with session_scope() as session:
                query = text("""
                       SELECT * FROM gdp_recent      
                             
                             """)
                
                result = await session.exec(query)
                
                
                rows  = result.mappings().all()
                
                
                if not rows:
                    return []
                
                return [GDPType.model_validate(row) for row in rows ]
        except Exception as e:
            logging.error(f"Error getting GDP last entry: {e}", exc_info=True)
            raise
    
    async def insert_gdp_report(self, data:List[GDPType]):
        try:
            async with session_scope() as session:
                async with session.begin():
                    
                    session.add_all(data)
                    await session.flush()
                    
                    for record in data:
                        await session.refresh(record)
                    return data
            
        except Exception as e:
            logging.error(f"Error inserting data")

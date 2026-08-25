# import asyncio
# import logging
# import os
# import sys
# from sqlmodel import text
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from typing import List
# from database.db import session_scope
# from custom_types.cpi import PPIType, countries


# class PPIModel:
    
#     async def get_last_report(self)->List[PPIType]:
#         try:
#             async with session_scope() as session:
#                 query = text("""
#                        SELECT * FROM ppi_recent      
                             
#                              """)
                
#                 result = await session.exec(query)
                
                
#                 rows  = result.mappings().all()
                
                
#                 if not rows:
#                     return []
                
#                 return [PPIType.model_validate(row) for row in rows ]
            
#         except Exception as e:
#             logging.error(f"Error getting the last PPI report for each country: {e}", exc_info=True)
#             raise
        
#     async def insert_ppi_report(self, data: List[PPIType]):
#         try:
#             async with session_scope() as session:
#                 async with session.begin():
                    
#                     session.add_all(data)
#                     await session.flush()
                    
#                     for record in data:
#                         await session.refresh(record)
#                     return data
#         except Exception as e:
#             logging.error(f"Error inserting into the PPI table {e}", exc_info=True)
#             raise
    
#     async def get_percent_ppi(self):
#             try:
#                 global countries
               
#                 async with session_scope() as session:
#                     query=text(f"""WITH RankedPPI AS (
#                             SELECT 
#                                 id,
#                                 country_code,
#                                 report_date,
#                                 index_value,
#                                 ROW_NUMBER() OVER (
#                                     PARTITION BY country_code 
#                                     ORDER BY report_date DESC
#                                 ) AS rn
#                             FROM public.ppi
#                             WHERE country_code = ANY(:countries)
#                         )
#                         SELECT 
#                             country_code,
#                             report_date,
#                             index_value
#                         FROM RankedPPI 
#                         WHERE rn <= 2
#                         ORDER BY country_code, report_date DESC  """)
                    
#                     result = await session.exec(query, params={"countries": countries})
#                     rows  = result.mappings().all()
                                    
                                    
#                     if not rows:
#                         return []
                    
#                     return [PPIType.model_validate(row) for row in rows ]
                
#             except Exception as e:
#                 logging.error(f"Error in getting cpi value from database: {e}", exc_info=True)
    
#                 raise
            
# # test = PPIModel()
# # loop = asyncio.get_event_loop()

# # if loop.is_running():
# # #     # If loop is already running, schedule the coroutine
# #     val = asyncio.run(test.get_percent_ppi())
# #     print(val)
# # else:
# #     # If no loop is running, run it synchronously
# #     val = asyncio.run(test.get_percent_ppi())
# #     print(val)
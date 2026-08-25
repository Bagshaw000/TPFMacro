# import logging
# import os
# import sys
# from typing import List
# from sqlmodel import text
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from custom_types.cpi import UNEMPType, countries
# from database.db import session_scope


# class UNEMP_Model:
    
    
#     async def get_last_report(self)->List[UNEMPType]:
#         try:
#             async with session_scope() as session:
#                 query = text("""
#                        SELECT * FROM unemp_recent      
                             
#                              """)
                
#                 result = await session.exec(query)
                
                
#                 rows  = result.mappings().all()
                
                
#                 if not rows:
#                     return []
                
#                 return [UNEMPType.model_validate(row) for row in rows ]
            
#         except Exception as e:
#             logging.error(f"Error getting the last CPI report for each country: {e}", exc_info=True)
#             raise
        
#     async def insert_unemp_report(self, data: List[UNEMPType]):
#         try:
#             async with session_scope() as session:
#                 async with session.begin():
                    
#                     session.add_all(data)
#                     await session.flush()
                    
#                     for record in data:
#                         await session.refresh(record)
#                     return data
#         except Exception as e:
#             logging.error(f"Error inserting into the CPI table {e}", exc_info=True)
#             raise
        
    
#     async def get_percent_unemp(self):
#         try:
#             global countries
            
#             async with session_scope() as session:
#                 query=text(f"""WITH RankedUNEMP AS (
#                         SELECT 
#                             id,
#                             country_code,
#                             report_date,
#                             index_value,
#                             ROW_NUMBER() OVER (
#                                 PARTITION BY country_code 
#                                 ORDER BY report_date DESC
#                             ) AS rn
#                         FROM public.ppi
#                         WHERE country_code = ANY(:countries)
#                     )
#                     SELECT 
#                         country_code,
#                         report_date,
#                         index_value
#                     FROM RankedUNEMP 
#                     WHERE rn <= 2
#                     ORDER BY country_code, report_date DESC  """)
                
#                 result = await session.exec(query, params={"countries": countries})
#                 rows  = result.mappings().all()
                                
                                
#                 if not rows:
#                     return []
                
#                 return [UNEMPType.model_validate(row) for row in rows ]
            
#         except Exception as e:
#             logging.error(f"Error in getting unemployment rate value from database: {e}", exc_info=True)

#             raise
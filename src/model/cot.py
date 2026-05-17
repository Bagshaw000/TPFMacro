import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.db import db_connect


class CotModel:
    
    def __init__(self):
        self.conn = db_connect()
       
        
    # Get the last report 
    async def get_last_report(self):
        try:

            data =  self.conn.table('cot_ttf').select("*").order("Market_and_Exchange_Names").order("Report_Date_as_YYYY_MM_DD", desc=True).limit(1).execute()

            return data
        except Exception as e:
            logging.error(f"error getting last report : {e}")
   
    # Insert the cot tff report 
    async def insert_tff_report(self, data:list):
        try:
            # await self.conn.db.execute("SET statement_timeout = '10min'")
            response = self.conn.table("cot_ttf").insert(data).execute()
            
            return response
            
        except Exception as e:
            logging.error(f"Error inserting TFF report : {e}",exc_info=True)
            
    async def update_ttf_report(self, data:list):
        try:
            # await self.conn.db.execute("SET statement_timeout = '10min'")
            response = self.conn.table("cot_ttf").upsert(data,  on_conflict="Market_and_Exchange_Names, Report_Date_as_YYYY_MM_DD").execute()
            
            return response
            
        except Exception as e:
            logging.error(f"Error inserting TFF report : {e}")
            
    # Get the latest Cot data for all instruments   
    async def get_latest_cot_data(self, start, end):
        try:
            
            response = self.conn.table("cot_ttf").select("*",count="exact").order("Report_Date_as_YYYY_MM_DD", desc=True).range(start,end).execute()
            
            return response 
        except Exception as e:
            logging.error(f"Error returning the latest cot data : {e}")
            
    async def get_cot_data_size(self):
        try:
            response = self.conn.table("cot_ttf").select("*" ,count="exact").execute()
            
            return response
        except Exception as e:
            logging.error(f"Error getting number of row : {e}", exc_info=True)

    async def get_last_entry(self):
        try:
            response = self.conn.table("last_entry").select("*").execute()
            
            return response
        except Exception as e:
            logging.error(f"Error getting the last entry", exc_info=True)
          
    # Get the all cot Data for all Instruments
            

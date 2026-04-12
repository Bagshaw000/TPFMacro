import logging
from .database import db_connect


class CotModel:
    
    def __init__(self):
        self.conn = db_connect()
        pass
        
    # Get the last report 
    async def get_last_report(self):
        try:

            data =  self.conn.table('cot_tff').select("*").order("pair").order("date", desc=True).limit(1).execute()
            
            print(data)

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
            logging.error(f"Error inserting TFF report : {e}")
            
    # Get the latest Cot data for all instruments   
    async def get_latest_cot_data(self):
        try:
            
            response = self.conn.table("last_entry").select("*").execute()
            
            return response
        except Exception as e:
            logging.error(f"Error returning the latest cot data : {e}")
            

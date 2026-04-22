import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from convex import ConvexClient
from dotenv import load_dotenv
import logging
from supabase import Client, ClientOptions, create_client
from config.config import get_doppler_env


secrets = get_doppler_env()

options = ClientOptions(
    schema="public",
    headers={"apikey": secrets.supabase_key},
    auto_refresh_token=True,
    persist_session=True
)

supabase:Client =  create_client(
    secrets.supabase_url,
    secrets.supabase_key,
    # options=options
   )

def db_connect():
    try:
        return supabase
    except Exception as e:
        logging.error(f'Error connection to the database : {e}', exc_info=True)
        


from convex import ConvexClient
from dotenv import load_dotenv
import logging

from supabase import Client, create_client
from .config.config import get_doppler_env

secrets = get_doppler_env()

supabase:Client =  create_client(secrets.supabase_url, secrets.supabase_key)

def db_connect():
    try:
        return supabase
    except Exception as e:
        logging.error(f'Error connection to the database : {e}', exc_info=True)
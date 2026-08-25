from contextlib import asynccontextmanager
import sys
import os
from typing import Optional

import asyncpg
from psycopg_pool import AsyncConnectionPool
from sqlmodel import MetaData, Table, create_engine
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from convex import ConvexClient
from dotenv import load_dotenv
import logging
from supabase import Client, ClientOptions, create_client
from config.config import get_doppler_env
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

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



_pool:asyncpg.Pool| None = None     


metadata = MetaData()
cot_ttf_table:Table | None = None
cot_last_entry: Table | None = None
DB_URL = f"postgresql+asyncpg://{secrets.db_user}:{secrets.db_password}@{secrets.db_server}/{secrets.db_name}"


db_engine =  create_async_engine(DB_URL, pool_size=10, max_overflow=20, pool_pre_ping=True, echo=False,  connect_args={
        "timeout": 90,  # Connection timeout in seconds
        "command_timeout": 90,  # Command timeout
    })

async def init_db_schemas():
    global cot_ttf_table , cot_last_entry
    async with db_engine.connect() as conn:
        cot_ttf_table = await conn.run_sync(
            lambda sync_conn: Table("cot_ttf",metadata ,autoload_with=sync_conn) 
        )
        
        cot_last_entry = await conn.run_sync(
            lambda sync_conn: Table("cot_last_entry",metadata ,autoload_with=sync_conn) 
        ) 


@asynccontextmanager
async def session_scope():
    
    async with AsyncSession(db_engine, expire_on_commit=False) as session:
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise
        
 
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


db_engine =  create_async_engine(DB_URL, pool_size=10, max_overflow=20, pool_pre_ping=True, echo=False)

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
        
        
# _pool:Optional[AsyncConnectionPool] = None
# DB_URL = f"postgresql://{secrets.db_user}:{secrets.db_password}@{secrets.db_server}/{secrets.db_name}"

        
# async def create_db_pool():
#     global _pool
        
#     if _pool is not None:
#         logging.warning("Pool already exists, closing existing pool first")
#         return  _pool
#     try:
#         # Create and initialize db connection

#         _pool = AsyncConnectionPool(conninfo=DB_URL, 
#                                     min_size=2, 
#                                     max_size=8, 
#                                     open=False, 
#                                     timeout=30.0,  # Connection timeout
#                                     max_idle=300.0,  # Close idle connections after 5 minutes
#                                     reconnect_timeout=5.0,  # Retry connection after failure
#                                     reconnect_failed=lambda exc: logging.info(f"Connection failed: {exc}"))

#         await _pool.open()
#         return _pool
        
#     except Exception as e:
#         logging.error(f"Error creating database pool: {e}")
#         raise 
    
# async def close_db_pool():
#     global _pool
    
#     if _pool is not None:
#         await _pool.close()
#         _pool = None
#         logging.info("Database pool closed")
        

# @asynccontextmanager
# async def get_connections():
#     try:
#         if _pool is None:
#             logging.info("Error getting database connections")
#             raise
        
#         async with _pool.connection() as conn:
#             yield conn
#     except Exception as e:
#         logging.error(f"Error getting database connections: {e}")


# @asynccontextmanager
# async def get_cursor():
#     try:
#         if _pool is None:
#             raise RuntimeError("Database pool not initialized. Call create_db_pool() first.")
    
#         async with _pool.connection() as conn:
#             async with conn.cursor() as cur:
#                 yield cur
                
#     except Exception as e:
#         logging.error(f"Error getting cursor: {e}")


# async def execute_query(query:str, params:tuple=None):
#     try:
#         async with get_cursor() as cur:
#             await cur.execute(query , params or ())
#             return await cur.fetchall()
        
#     except Exception as e:
#         logging.error(f"Error executing query: {e}")
#         raise

# async def execute_one(query: str, params:tuple=None):
#     """Execute a query and return one result"""
#     try:
#         async with get_cursor() as cur:
#             await cur.execute(query, params or ())
#             return await cur.fetchone()
#     except Exception as e:
#         logging.error(f"Error executing query: {e}")
#         raise
    

# async def execute_write(query: str, params: tuple = None):
#     """Execute a write operation (INSERT, UPDATE, DELETE)"""
#     try:
#         async with get_connections() as conn:
#             async with conn.cursor() as cur:
#                 await cur.execute(query, params or ())
#                 await conn.commit()
#                 return cur.rowcount
#     except Exception as e:
#         logging.error(f"Error executing write: {e}")
#         raise   

        
# async def db_health():
#     try:
#         result = await execute_one("SELECT 1")
        
#         if result and result[0] == 1:
#             logging.info("Database health check: OK")
#             return True
#         else:
#             logging.error("Database health check: Unexpected result")
#             return False
    
#     except Exception as e:
#         logging.error(f"Error checking database health: {e}")
        

"""Database connectivity - Postgres (the store of record) and Supabase.

Exposes:
  - `db_engine`      : the SQLAlchemy async engine (asyncpg driver), pooled.
  - `session_scope()`: async context manager yielding an AsyncSession that
                       commits on clean exit and rolls back on any exception.
                       Every model/*.py query runs inside one of these.
  - `init_db_schemas()`: reflects the `cot_ttf` / `cot_last_entry` tables from
                       the live database into `metadata` at startup (they are
                       not declared as SQLModel classes).
  - `db_connect()`   : returns the Supabase client (used for its REST/PostgREST
                       and auth surface, separate from the SQL engine).

Credentials come from Doppler via `get_doppler_env()` (see config/config.py).
"""

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



_pool:asyncpg.Pool| None = None     # reserved for a raw asyncpg pool; unused


# Tables reflected from the live DB by init_db_schemas() rather than declared
# as SQLModel classes - populated in place, so importers see None until startup.
metadata = MetaData()
cot_ttf_table:Table | None = None
cot_last_entry: Table | None = None
DB_URL = f"postgresql+asyncpg://{secrets.db_user}:{secrets.db_password}@{secrets.db_server}/{secrets.db_name}"


# pool_pre_ping guards against stale connections dropped by the DB / a proxy;
# the 90s timeouts are generous because some analytics queries are heavy.
db_engine =  create_async_engine(DB_URL, pool_size=10, max_overflow=20, pool_pre_ping=True, echo=False,  connect_args={
        "timeout": 90,  # Connection timeout in seconds
        "command_timeout": 90,  # Command timeout
    })

async def init_db_schemas():
    """Reflect the two non-SQLModel tables into `metadata`. Call once at
    startup, before any model that imports `cot_ttf_table` / `cot_last_entry`
    actually uses them."""
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
    """Async session that commits on success and rolls back on any exception.
    `expire_on_commit=False` keeps returned ORM objects usable after the block."""
    async with AsyncSession(db_engine, expire_on_commit=False) as session:
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise
        
 
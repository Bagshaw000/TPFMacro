"""arq worker entry point: defines the scheduled (cron) background jobs
that keep Redis/Postgres populated with market data - COT positioning,
currency snapshots, economic calendar events, LSE indicators (CPI/PPI/
UNEMP/GDP/inflation/retail), and news sentiment.

Run via arq against this module (`arq worker.WorkerSettings`); arq reads
`WorkerSettings.cron_jobs` and `WorkerSettings.redis_settings` to know what
to run and where its own job queue lives.

# BROKEN IMPORT: `from .cot import COT` below expects a class named `COT`
# in src/cot.py, but that file only defines `COTNew` (which does have the
# `update_cot()` method this worker calls) - there is no `COT` symbol in
# src/cot.py at all. As it stands, importing this module raises
# `ImportError: cannot import name 'COT' from 'cot'`. Looks like a rename
# that missed this import; needs either `from .cot import COTNew as COT`
# or updating the reference to `COTNew` directly.
"""

import asyncio
import logging
logger = logging.getLogger(__name__)
import os
import sys

# worker.py sits directly in src/, so src/ itself is what needs to be on the
# path for the bare `model.*` / `controller.*` / `custom_types.*` imports used
# throughout this codebase (one dirname, not two).
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from httpx import AsyncClient
from arq import create_pool
from arq.connections import RedisSettings
from arq import cron
from cot import COT
from controller.cot import COTController
from model.market_overview import MarketOverview
from controller.macro import MacroController
from controller.cross_section import CrossSectionController
from controller.economic_event import EconomicEventController
# from controller.gdp import GDPController
# from controller.unemp import UNEMPController
from controller.news import NewsSentimentController
# from controller.cross_section import CrossSectionController
from controller.lse_ import LSEController
from logging_config import configure_logging

try:
    # Check if a loop already exists
    asyncio.get_event_loop()
except RuntimeError:
    if sys.platform == 'win32':
        # Windows-specific network engine
        # Windows' default asyncio loop (ProactorEventLoop) is
        # incompatible with some libraries used here (e.g. certain
        # psycopg/asyncpg internals); the Selector-based policy avoids that.
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    else:
        # Ultra-efficient Linux native initialization
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)



# Controller/model instances are constructed once at module import time and
# reused across every job run - arq workers are long-running processes, so
# this avoids re-establishing DB/Redis connections on every scheduled
# invocation.
cot_model = COT()
cot_ctrl = COTController()
market_ovw = MarketOverview()
# cpi_crtl = CPIController()
# ppi_ctrl = PPIController()
# unemp_ctrl = UNEMPController()
# gdp_ctrl = GDPController()
econ_event_ctrl = EconomicEventController()
news_sentiment_ctrl = NewsSentimentController()
lse_ctrl = LSEController()
macro_ctrl = MacroController()
cross_section_ctrl = CrossSectionController()

# Each job below is an arq task function: arq always calls it with a `ctx`
# dict (job context - not used by any of these), and the function's job is
# just to delegate to the matching controller/model and log completion.

async def cot_update(ctx):
    await cot_model.update_cot()
    # print("Running")
    logger.info(f"Running COT worker with id ")

async def currency_snapshot(ctx):

    await market_ovw.get_currency()
    logger.info(f"Running currency snapshot worker")

async def get_events(ctx):
    await econ_event_ctrl.store_economic_event()
    logger.info(f"Running economic event worker")

async def get_lse(ctx):
    await lse_ctrl.get_event_cal()
    logger.info(f"Running LSE worker with id ")

# async def get_ppi(ctx):
#     await ppi_ctrl.get_ppi()
#     logger.info(f"Runnin Ppi worker with id")

# async def get_unemp(ctx):
#     await unemp_ctrl.get_unemp()
#     logger.info(f"Runnin Unemployment rate worker with id")

# async def get_gdp(ctx):
#     await gdp_ctrl.get_gdp()
#     logger.info(f"Running GDP worker")

async def get_new_sentiment(ctx):
    await news_sentiment_ctrl.all_country_sentiment()
    logger.info(f"Running news sentiment")

async def refresh_factor_stats(ctx):
    await macro_ctrl.refresh_factor_stats()
    logger.info(f"Running factor stats refresh worker")

async def refresh_cross_section(ctx):
    await cross_section_ctrl.update_quandrant()
    logger.info(f"Running Cross Section analysis")

async def full_cot_positioning(ctx):
    await cot_ctrl.full_positioning()
    logger.info(f"Running full COT positioning (non-curated instruments)")

async def curated_cot_positioning(ctx):
    await cot_ctrl.instituitional_pos()
    logger.info(f"Running curated COT positioning refresh (cot_pos:_meta)")


async def on_startup(ctx):
    # Runs after arq has installed its own logging config, so this call
    # (force=True) restores the project's format + file handler for the worker
    # process. The API process does the equivalent from main.py.
    configure_logging()
    logger.info("arq worker started")


class WorkerSettings:
    # arq reads this class directly (via `arq worker.WorkerSettings`) to
    # know which jobs to schedule and how to reach its own job-queue Redis.

    on_startup = on_startup

    cron_jobs = [
        # Every Wednesday at 23:00 - weekly CFTC COT reports are typically
        # released Friday afternoons (for the prior Tuesday's data), so this
        # actually reflects data current as of ~1 week earlier than a
        # Friday run would; unique=True prevents overlapping runs if one is
        # still in progress, run_at_startup=False means it won't fire
        # immediately when the worker process starts.
        cron(cot_update,  weekday="wed", hour=23, unique=True,
            run_at_startup=False),
        # Every day at 05:00.
        cron(currency_snapshot, hour=5 , minute=0,
            unique=True,
            run_at_startup=False),
        # Every Saturday at 23:00.
        cron(get_events,weekday='sat', hour=23, unique=True,
            run_at_startup=False),
        # On the 1st, 5th, 10th, 15th, 20th, 25th, and 30th of every month at
        # 23:00 (roughly every 5 days, catching most monthly release dates).
        cron(get_lse,day={1, 5, 10, 15, 20, 25, 30}, hour=23, unique=True,
            run_at_startup=False),
        # Every 3 hours (00/03/06/09/12/15/18/21), on the hour.
        # No unique/run_at_startup override, so this uses arq's defaults
        # (unique=True, run_at_startup=True) unlike the other jobs above.
        cron(get_new_sentiment, hour={0, 3, 6, 9, 12, 15, 18, 21},  # Every 3rd hour of the day
            minute=0 ),
        # Every Sunday at 22:00 - trailing (mu, sigma) stats barely move
        # between individual releases, so a weekly refresh keeps the cache
        # (see controller/macro.py's STATS_TTL) well ahead of its 9-day
        # expiry without hitting Postgres on every economic-cycle read.
        cron(refresh_factor_stats, weekday="sun", hour=22, unique=True,
            run_at_startup=False),
        # 1st of every month at 22:30 - PPI->CPI lead is a structural read
        # meant to be stable (see cross_section.py's module docstring on
        # why it's estimated once and cached, not re-fit live), so a
        # monthly cadence is plenty, well ahead of LEAD_TTL's 40-day expiry.
        cron(refresh_cross_section, day={1, 10, 15, 20, 25}, hour=22, minute=30, unique=True,
            run_at_startup=False),
        # Every Sunday at 01:00 - a few days after cot_update has synced the new
        # weekly report, so the long-tail positioning (full_positioning: every
        # non-curated cot_ttf instrument, with an LLM breakdown each) is scored
        # against fresh data. Slow (hundreds of rate-limited LLM calls); runs
        # overnight. Writes cot_pos:_meta_all; never touches cot_pos:_meta.
        cron(full_cot_positioning, weekday="sun", hour=1, unique=True,
            run_at_startup=False),
        # Every Saturday at 00:30 - ahead of the full sweep above. Rescores the
        # curated COT_CURATED_ASSETS shortlist and rewrites cot_pos:_meta + its
        # blobs (each with an LLM summary). Only ~11 instruments, so this is
        # quick. This is the job that keeps the startup snapshot
        # (ensure_positioning) fresh while the app runs.
        cron(curated_cot_positioning, weekday="sat", hour=0, minute=30, unique=True,
            run_at_startup=False),
    ]

    # Redis instance arq itself uses to store/dispatch jobs - separate from
    # RedisConnection (database/redis_.py) used by the app's own data cache.
    redis_settings = RedisSettings(host='redis')


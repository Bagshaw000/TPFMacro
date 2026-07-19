import asyncio
import logging
import os
import sys

from controller.ppi import PPIController
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from httpx import AsyncClient
from arq import create_pool
from arq.connections import RedisSettings
from arq import cron
from .cot import COT
from model.market_overview import MarketOverview
from controller.cpi import CPIController


try:
    # Check if a loop already exists
    asyncio.get_event_loop()
except RuntimeError:
    if sys.platform == 'win32':
        # Windows-specific network engine
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    else:
        # Ultra-efficient Linux native initialization
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        

cot_model = COT()
market_ovw = MarketOverview()
cpi_crtl = CPIController()
ppi_ctrl = PPIController()


async def test_cleanup(ctx):
    await cot_model.update_cot()
    print("Running")
    logging.info(f"Running COT worker with id ")
    
async def currency_snapshot(ctx):
    
    await market_ovw.get_currency()
    logging.info(f"Running currency snapshot worker with id ")
    
async def get_events(ctx):
    await market_ovw.get_economic_event()
    logging.info(f"Running economic event worker with id ")
    
async def get_cpi(ctx):
    await cpi_crtl.get_cpi()
    logging.info(f"Running Cpi worker with id ")
    
async def get_ppi(ctx):
    await ppi_ctrl.get_ppi()
    logging.info(f"Runnin Ppi worker with id")


class WorkerSettings:
    
    
    cron_jobs = [
        cron(test_cleanup,  weekday="wed", hour=23, unique=True,
            run_at_startup=False),
        cron(currency_snapshot, hour=5 , minute=0, 
            unique=True,
            run_at_startup=False),
        cron(get_events,weekday='sat', hour=23, unique=True,
            run_at_startup=False),
        cron(get_cpi,day={1, 10, 20}, hour=23, unique=True,
            run_at_startup=False),
        cron(get_ppi,day={1, 10, 20}, hour=23, unique=True,
            run_at_startup=False)
    ]
    
    redis_settings = RedisSettings(host='redis')
    
    
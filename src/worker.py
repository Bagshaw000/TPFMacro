import asyncio
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from httpx import AsyncClient
from arq import create_pool
from arq.connections import RedisSettings
from arq import cron
from .cot import COT
from model.market_overview import MarketOverview


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
        
        
# REDIS_SETTINGS = RedisSettings(host="redis")
cot_model = COT()
market_ovw = MarketOverview()

async def test_cleanup(ctx):
    await cot_model.update_cot()
    print("Running")
    
async def currency_snapshot(ctx):
    
    await market_ovw.get_currency()
    
async def get_events(ctx):
    await market_ovw.get_economic_event()

class WorkerSettings:
    
    
    cron_jobs = [
        cron(test_cleanup,  weekday="wed", hour=23, unique=True,
            run_at_startup=False),
        cron(currency_snapshot, hour=5 , minute=0, 
            unique=True,
            run_at_startup=False),
        cron(get_events,weekday='sat', hour=23, unique=True,
            run_at_startup=False)
    ]
    
    
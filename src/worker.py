import asyncio
from httpx import AsyncClient
from arq import create_pool
from arq.connections import RedisSettings
from arq import cron
from .cot import COT


REDIS_SETTINGS = RedisSettings()
cot_model = COT()

async def test_cleanup(ctx):
    await cot_model.update_cot()
    print("Running")
    

class WorkerSettings:
    functions = [test_cleanup]
    
    cron_jobs = [
        cron(test_cleanup,  weekday="wed", hour=23)
    ]
    
    redis_settings = RedisSettings(host="localhost")
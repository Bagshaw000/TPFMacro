import sys
import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sqlmodel import MetaData, Table
import asyncio
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import pandas as pd
from src.routes import cot, symbol, macro
from fastapi.middleware.cors import CORSMiddleware
from model.market_overview import MarketOverview
from psycopg_pool import AsyncConnectionPool
from config.config import get_doppler_env
from sqlalchemy.ext.asyncio import create_async_engine
from faststream import FastStream
from faststream.nats.fastapi import NatsRouter
from src.controller.cot import COTController
from src.controller.macro import MacroController
from src.controller.lse_ import LSEController
from src.controller.cross_section import CrossSectionController

import logging

market_overview = MarketOverview()
cot_ctrl =  COTController()
macro_ctrl = MacroController()
lse_ctrl = LSEController()
cross_sec = CrossSectionController()
env_var = get_doppler_env()
nats_router = NatsRouter("nats://localhost:4222/")

# Load database schema




@asynccontextmanager
async def lifespan(app: FastAPI):
    
   
    await nats_router.startup()
    await cot_ctrl.setup_redis()
    
    # 3. Data loading (concurrent with error handling)
    results = await asyncio.gather(
        macro_ctrl.refresh_factor_stats(),
        market_overview.get_currency(),
        lse_ctrl.get_event_cal(),
        cot_ctrl.instituitional_pos(),
        cross_sec.update_quandrant(),
        return_exceptions=True
    )
    
    await asyncio.gather(macro_ctrl.get_global_cycle())
    
    # 4. Log failures but continue
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Startup task failed: {result}")
    
    yield
    await nats_router.shutdown()
    # await cot_ctrl.shutdown()

    
app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "https://domianmt5.xyz",
        "http://domianmt5.xyz"
        # Next.js dev
          # Production frontend
        
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "Accept", "X-Requested-With"],               # Allow all headers
    expose_headers=["*"],
    max_age=3600,                      # Cache preflight for 1 hour
)

app.include_router(cot.router)
app.include_router(symbol.router)
app.include_router(macro.router)

@app.get("/health")
async def read_root():
    try:
    # Test redis health
        return {"status": "ok"}
    except:
        raise HTTPException(status_code=503, detail="Redis unavailable")


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}


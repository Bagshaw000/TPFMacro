import sys
import os
import time
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastapi.responses import PlainTextResponse
from sqlmodel import MetaData, Table
import asyncio
from fastapi import FastAPI, HTTPException, Request
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
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

import logging
from src.logging_config import configure_logging

# Configure root logging for the API process before anything logs. Every module
# does `logger = logging.getLogger(__name__)` and logs through that.
configure_logging()
logger = logging.getLogger(__name__)


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
        cot_ctrl.ensure_positioning(),
        cross_sec.update_quandrant(),
        return_exceptions=True
    )
    
    await asyncio.gather(macro_ctrl.get_global_cycle())
    
    # 4. Log failures but continue
    for result in results:
        if isinstance(result, Exception):
            logger.error("Startup task failed: %s", result, exc_info=result)
    
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

# 2. Define Prometheus Metrics
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total", 
    "Total number of HTTP requests", 
    ["method", "endpoint", "status_code"]
)
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds", 
    "HTTP request latency in seconds", 
    ["method", "endpoint"]
)

app.include_router(cot.router)
app.include_router(symbol.router)
app.include_router(macro.router)


# 3. Middleware to intercept requests for BOTH Logging and Metrics
@app.middleware("http")
async def monitor_and_log_middleware(request: Request, call_next):
    start_time = time.time()
    endpoint = request.url.path
    method = request.method
    
    # Process the request
    response = await call_next(request)
    
    # Calculate duration
    duration = time.time() - start_time
    status_code = response.status_code
    
    # Action A: Update Prometheus Metrics (Numbers)
    HTTP_REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status_code=status_code).inc()
    HTTP_REQUEST_DURATION_SECONDS.labels(method=method, endpoint=endpoint).observe(duration)
    
    # Action B: Write to your In-House Logging Mechanism (Text)
    logger.info(f"Method: {method} | Path: {endpoint} | Status: {status_code} | Latency: {duration:.4f}s")
    
    return response

# 4. The Metrics Endpoint for Prometheus to scrape
@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def read_root():
    # Liveness + Redis reachability. The cache is on the hot path for every
    # read endpoint, so a failed PING is a real 503.
    try:
        await cot_ctrl.aioredis.ping()
    except Exception as exc:
        logger.error("Health check failed: Redis unreachable: %s", exc)
        raise HTTPException(status_code=503, detail="Redis unavailable")
    return {"status": "ok"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}


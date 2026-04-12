from fastapi import FastAPI
import os
from src.database import db_connect
from convex import ConvexClient
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from src.cot import COT
from src.config.config import get_doppler_env




@asynccontextmanager
async def lifespan(app: FastAPI):
    
    load_dotenv()
    
    yield
    
app = FastAPI(lifespan=lifespan)




@app.get("/health")
async def read_root():

    # Test redis health
    return {"status": "ok"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}
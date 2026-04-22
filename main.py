from fastapi import FastAPI, HTTPException
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
    try:
    # Test redis health
        return {"status": "ok"}
    except:
        raise HTTPException(status_code=503, detail="Redis unavailable")


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}
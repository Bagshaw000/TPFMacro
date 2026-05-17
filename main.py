from fastapi import FastAPI, HTTPException
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from src.routes import cot,symbol
from fastapi.middleware.cors import CORSMiddleware




@asynccontextmanager
async def lifespan(app: FastAPI):
    
    load_dotenv()
    
    yield
    
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
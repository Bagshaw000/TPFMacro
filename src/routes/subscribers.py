import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.model.nat import NatsService, nats_router

@nats_router.subscriber("symbol.ingested")
async def ingest_symbol(data:dict):
    print("Ttetet")
    print(dict)
    return {"status": "done"}

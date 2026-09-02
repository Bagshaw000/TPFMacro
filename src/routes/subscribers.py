"""NATS subscriber stub for the symbol-ingest stream (see src/nat.py).

Currently dead: `from nat import NatsService, nats_router` does not match what
nat.py exports (`broker`, `app`, `stream`), so importing this module raises.
It is not included by main.py. Left as a placeholder for the message-bus
migration - handler for tick data published on "symbol.ingested" by
routes/symbol.py::ingest_symbol.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nat import NatsService, nats_router

@nats_router.subscriber("symbol.ingested")
async def ingest_symbol(data:dict):
    print("Ttetet")
    print(dict)
    return {"status": "done"}

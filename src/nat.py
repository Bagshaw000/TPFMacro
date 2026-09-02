"""Experimental NATS JetStream wiring (FastStream).

Scaffolding for moving the pipeline onto a message bus - the ingest side would
`publish` onto subjects like "cpi.new" / "symbol.ingested" and the analytics
side would `subscribe`, instead of the current "worker writes Redis, API reads
Redis" arrangement. Not wired into `main.py`; the `nats` service in
docker-compose exists for this. See routes/subscribers.py for the other half.

Run standalone with `faststream run src.nat:app`.
"""

import sys
import os

import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from faststream.nats import NatsBroker, JStream
from nats.js.api import DeliverPolicy
# from faststream.nats.fastapi import NatsRouter
from faststream import FastStream, Logger
import logging


# Single broker + JetStream stream shared by every subscriber/publisher here.
broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)
stream = JStream(name="stream")


# DeliverPolicy.NEW - only messages published after this consumer connects,
# i.e. skip the backlog on (re)start.
@broker.subscriber("cpi.new", stream=stream, deliver_policy=DeliverPolicy.NEW)
async def process(msg, logger:Logger):
    # print(msg)
    return msg



@app.after_startup
async def send_messages():
    # Smoke test: publish one message once the app is up.
    data = await broker.publish("published", "cpi.new", stream="stream")
    print(data)
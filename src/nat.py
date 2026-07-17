import sys
import os

import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from faststream.nats import NatsBroker, JStream
from nats.js.api import DeliverPolicy
# from faststream.nats.fastapi import NatsRouter
from faststream import FastStream, Logger
import logging


broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)
stream = JStream(name="stream")


@broker.subscriber("cpi.new", stream=stream, deliver_policy=DeliverPolicy.NEW)
async def process(msg, logger:Logger):
    # print(msg)
    return msg



@app.after_startup
async def send_messages():
    data = await broker.publish("published", "cpi.new", stream="stream")
    print(data)
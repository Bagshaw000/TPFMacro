import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from faststream.nats import NatsBroker
from faststream.nats.fastapi import NatsRouter
from faststream import FastStream
import logging


class NatsService:
    _instance = None
    _broker = None
    _router = None
    
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def get_broker(cls)->NatsBroker:
        if cls._broker is None:
            cls._broker = NatsBroker(servers="nats://localhost:4222")
            
        return cls._broker
    
    @classmethod
    def get_router(cls)->NatsRouter:
        if cls._router is None:
            cls._router = NatsRouter(servers="nats://localhost:4222")
            
        return cls._router
    
    
    @classmethod
    async def start(cls):
        if not cls._broker:
            cls.get_broker()
        try:
            await cls._broker.start()
            logging.info('NATS broker started successfully')
        except Exception as e:
            logging.error(f"Failed to start NATS broker: {e}", exc_info=True)

    @classmethod
    async def close(cls):
        if cls._broker:
            try:
                await cls._broker.stop()
                logging.info("NATS Broker closed")
                
            except Exception as e:
                logging.error(f' Error closing error: {e}', exc_info=True)
    
broker =NatsService.get_broker()
nats_router = NatsService.get_router()
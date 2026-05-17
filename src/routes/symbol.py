import asyncio
from collections.abc import AsyncIterable
import json
import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from model.market_overview import MarketOverview
from fastapi import APIRouter,WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.sse import EventSourceResponse
from fastapi.concurrency import run_in_threadpool


router = APIRouter(prefix="/v1/symbol")
symbol_obj = MarketOverview()




async def new_event(country: str):
    """
    Separate generator function to cleanly handle the streaming loop.
    """
    try:
        while True:
            # Fetch data using your existing async call
            
            data = await run_in_threadpool(symbol_obj.get_news_events,country)
            
            print(data)
            # SSE expects plain text structured as "data: <payload>\n\n"
            # EventSourceResponse handles the "data:" framing, but we must yield string data
            
            yield json.dumps(data)
            await asyncio.sleep(1)
            # Pauses execution and yields control to allow other users to connect
            
            
    except asyncio.CancelledError:
        # This triggers automatically when the client closes the browser or tab
        logging.info(f"Client disconnected from {country} stream.")
    except Exception as e:
        logging.error(f"Error in stream for {country}: {e}", exc_info=True)


async def get_symbol(category: str, pair: str):
    try:
        while True:
            # print(category)
            
            data = await run_in_threadpool(symbol_obj.get_symbol_data,pair, category)
            # print(data)
            
            
            yield json.dumps(data)
            
            await asyncio.sleep(1)
            
            
    except asyncio.CancelledError:
        # This triggers automatically when the client closes the browser or tab
        logging.info(f"Client disconnected from {pair} {category} stream.")
    except Exception as e:
        logging.error(f"Error in stream for {pair} {category} : {e}", exc_info=True)
        
        
# @router.get("/")
# async def get_data():
    
#     data = await cot_obj.get_cot_data()
 
    
#     if data:
#         return JSONResponse(status_code=200, content= data)
#     return JSONResponse(status_code=500, content=None)

@router.websocket("/ws/{category}/{pair}")
async def get_news_events(websocket:WebSocket,category: str, pair: str):
    await websocket.accept()
    print(category)
    data = await symbol_obj.get_symbol_data(pair, category)
    try:
        while True:
            
            await asyncio.sleep(2) 
            await websocket.send_json(json.dumps(data))
            
    except WebSocketDisconnect:
        print("Client disconnected")
        logging.error("Client disconnected", exc_info=True)


@router.websocket("/ws/event/{country}")
async def get_symbol_datas(websocket:WebSocket,country: str,):
    await websocket.accept()
    # print(category)
    data = await symbol_obj.get_news_events(country)
    try:
        while True:
            
            await asyncio.sleep(2) 
            await websocket.send_json(json.dumps(data))
            
    except WebSocketDisconnect:
        print("Client disconnected")
        logging.error("Client disconnected", exc_info=True)


@router.get('/snapshot/{category}/{ticker}')
async def get_market_snapshot(category:str, ticker:str):
    data  = await symbol_obj.symbol_snapshot(ticker, category)
   
    if data:
        # return data
       return JSONResponse(content=data, status_code=200 )
      
    return JSONResponse(status_code=500, content=None)

@router.get('/corr/{category}/{ticker}')
async def get_symbol_corr(category:str, ticker:str):
    
    data = await symbol_obj.symbol_correlation(ticker, category)
    print(data)
    
    if data:
        return JSONResponse(content=data,status_code=200)
    
    return JSONResponse(status_code=500, content=None)

@router.get('/technical/{category}/{ticker}')
async def get_technical_signal(category:str, ticker:str):
    data = await symbol_obj.symbol_technical_signals(ticker, category)
 
    
    if data:
        return JSONResponse(content=data,status_code=200)
    
    return JSONResponse(status_code=500, content=None)


# @router.websocket("/ws/featured_pairs")
# async def featured_pairs(websocket:WebSocket):
#     await websocket.accept()
#     data = await symbol_obj.get_featured_pairs()
#     try:
#         while True:
          
#             await websocket.send_json(json.dumps(data))
#             await asyncio.sleep(2) 
#     except WebSocketDisconnect:
#         print("Client disconnected")
#         logging.error("Client disconnected", exc_info=True)
        
# @router.get("/event/{country}",response_class=EventSourceResponse)
# async def featured_pair(country: str):

#     return EventSourceResponse(new_event(country))
        
# @router.get("/event/{category}/{pair}",response_class=EventSourceResponse)
# async def get_symbol_data(category: str, pair: str):
#     print(category)
#     symbol = get_symbol(category,pair)
#     return EventSourceResponse(symbol)

    
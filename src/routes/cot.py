# All Cot routes will show here

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from controller.cot import COTController
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/v1/cot")
cot_ctrl = COTController()

@router.get("/")
async def get_data():
    
    data = await cot_ctrl.get_cot_data()
 
    
    if data:
        return JSONResponse(status_code=200, content= data)
    return JSONResponse(status_code=500, content=None)

@router.get("/cot_pos")
async def cross_section_panel():
    data = await cot_ctrl.get_positioning()
    # print(data)

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=500, content=None)
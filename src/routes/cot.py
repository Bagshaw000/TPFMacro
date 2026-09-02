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


@router.get("/net_pct_oi")
async def net_pct_oi_timeseries(scope: str = "tracked", weeks: int = 52):
    """Net % of open interest as a weekly time series, per trader category, for
    every instrument in the positioning meta index.

    scope=tracked (default) -> curated shortlist; scope=all -> curated + tail.
    weeks caps how much trailing history each series returns.
    """
    if scope not in ("tracked", "all"):
        return JSONResponse(
            status_code=422,
            content={"detail": "scope must be 'tracked' or 'all'"},
        )
    if weeks < 1:
        return JSONResponse(
            status_code=422,
            content={"detail": "weeks must be >= 1"},
        )

    data = await cot_ctrl.net_pct_oi_timeseries(scope=scope, weeks=weeks)

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=500, content=None)
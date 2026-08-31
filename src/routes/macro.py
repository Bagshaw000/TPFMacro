import os
import sys

from fastapi.responses import JSONResponse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fastapi import APIRouter
from controller.macro import MacroController
from controller.cross_section import CrossSectionController


router = APIRouter(prefix="/v1/macro")
macro_ctrl = MacroController()
cross_section_ctrl = CrossSectionController()


@router.get("/global_avg")
async def get_global_avg():
    data = await macro_ctrl.get_global_avg()
    
    if data:
        return JSONResponse(status_code=200, content=data)
    
    return JSONResponse(status_code=500, content=None)

@router.get("/economies")
async def economies_summary():
    data = await macro_ctrl.get_global_cycle()
    
    if data:
        return JSONResponse(status_code=200, content=data)
    
    return JSONResponse(status_code=500, content=None)

@router.get("/economy/{country}")
async def country_stats(country:str):
    data = await macro_ctrl.get_country_stats(country.upper())

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=500, content=None)

@router.get("/cross_section")
async def cross_section_panel():
    data = await cross_section_ctrl.get_cross_section()
    # print(data)

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=500, content=None)

@router.get("/cross_section/{country}")
async def cross_section_panel(country:str):
    data = await cross_section_ctrl.get_cross_section_by_country(country)
    # print(data)

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=500, content=None)



# @router.get("/cross_section/analysis")
# async def cross_section_analysis(mode: str = "percentile"):
#     data = await cross_section_ctrl.get_snapshot(mode=mode)

#     if data:
#         return JSONResponse(status_code=200, content=data)

#     return JSONResponse(status_code=500, content=None)
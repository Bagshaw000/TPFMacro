# All Cot routes will show here
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from controller.cot import COTController


router = APIRouter(prefix="/v1/cot_ttf")
cot_obj = COTController()

@router.get("/")
async def get_data():
    
    data = await cot_obj.get_cot_data()
    
    if data:
        return JSONResponse(status_code=200, content= data)
    return JSONResponse(status_code=500)
    
"""/v1/cot routes - COT (Commitment of Traders) positioning + net-position change.

All work lives in controller/cot.py; these handlers just call it and wrap the
result in a JSONResponse (200 with the payload, or an error status with null).
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from controller.cot import COTController
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/v1/cot")
# Process-lifetime singleton - opens the Redis pools and the LLM client once.
cot_ctrl = COTController()

@router.get("/")
async def get_data():
    """Net-position pct change (1/3/6/12-month) for every configured instrument,
    grouped by asset class. Backed by the cot_ttf:* weekly hashes."""
    data = await cot_ctrl.get_cot_data()


    if data:
        return JSONResponse(status_code=200, content= data)
    return JSONResponse(status_code=500, content=None)

@router.get("/cot_pos")
async def cross_section_panel():
    """The cached positioning snapshot for the curated shortlist:
    {meta, instruments: {asset: {category: {net_pct_oi, percentile, score, z,
    mom_4w, label}, summary}}}. Produced by instituitional_pos / the worker."""
    data = await cot_ctrl.get_positioning()
    # print(data)

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=500, content=None)


@router.get("/net_pct_oi/{scope}")
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


@router.get("/asset_changes/{asset}")
async def asset_group_changes(asset: str, market: str | None = None, weeks: int = 52):
    """One instrument's last `weeks` weekly COT reports: each TFF trader group's
    net-position series plus its pct change over 1 / 3 / 6 / 12-month windows.

    `asset` is the instrument name (path); `market` is optional and looked up
    from Postgres when omitted.
    """
    if weeks < 1:
        return JSONResponse(status_code=422, content={"detail": "weeks must be >= 1"})

    data = await cot_ctrl.asset_group_changes(asset, market=market, weeks=weeks)

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=404, content=None)


@router.get("/instrument/{asset}")
async def get_instrument(asset: str, market: str | None = None, weeks: int = 52):
    """Everything cached for ONE instrument in a single call: positioning
    metrics (percentile/score/z/mom_4w/label per trader group, computed fresh
    - works for any cot_ttf instrument, not just the curated shortlist),
    net-%-of-OI history, net-position change per trader group, the cached LLM
    summary if the positioning cron has already scored it, and the raw
    unprocessed weekly hashes themselves (every field CFTC publishes, as the
    raw strings Redis stored).

    `asset` is the instrument name (path); `market` is optional and looked up
    from Postgres when omitted.
    """
    if weeks < 1:
        return JSONResponse(status_code=422, content={"detail": "weeks must be >= 1"})

    data = await cot_ctrl.get_instrument(asset, market=market, weeks=weeks)

    if data:
        return JSONResponse(status_code=200, content=data)

    return JSONResponse(status_code=404, content=None)
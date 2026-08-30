import asyncio
import json
import logging
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from config.config import get_doppler_env
import re

class LLMController:

    def __init__(self) -> None:
        self.secret = get_doppler_env()

    async def breakdown_inst_positioning(self, data):
        try:
            prompt = (
                "Based on the commitment of trader position calculation, provide a "
                "max 20 word summary of what the data is suggesting.\n"
                "Draw a conclusion in the response as a full sentence and talk in most likely action.\n"
                "Response should be a sentence. Do not add these phrases and word like **Summary (20 words max):** and Conculsion in the response."
                f"This is the data in JSON format: {json.dumps(data)}"
            )
            response = requests.post(
                "https://api.modelrail.dev/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.secret.modelrail_key}"},
                json={
                    "model": "modelrail-auto",
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            completion = response.json()
            content = completion["choices"][0]["message"]["content"]
            return re.sub(r"\n[ \t]*\n+", "\n\n", content).strip()
        except Exception as e:
            logging.error(f"Error breaking down Instituitional positioning: {e}", exc_info=True)
            raise
        
        
    async def global_breakdown(self, data):
        try:
            prompt = (
                "You are a macro strategist interpreting a cross-sectional "
                "Phillips-curve analysis of major economies. Every score below "
                "is a 0-100 rank of that economy against the OTHER economies in "
                "this same snapshot (current month), and higher is always "
                "healthier.\n\n"
                "AXES\n"
                "- price: inflation pressure. HIGH = inflation contained "
                "(at or below target, soft producer prices); LOW = inflation "
                "running hot. Blends a CPI-vs-target score (55%) and a "
                "producer-price score (45%).\n"
                "- demand: demand strength. HIGH = tight labour market "
                "(unemployment below its own trend) and accelerating real "
                "retail sales; LOW = labour-market slack and stalling retail. "
                "Blends an unemployment-gap score (60%) and a retail-momentum "
                "score (40%).\n"
                "- composite: overall health, all four underlying factors "
                "weighted together.\n"
                "- contrib_cpi / contrib_ppi / contrib_unemp / contrib_ret: "
                "each factor's weighted share of composite (they sum to it) - "
                "use these to name the factor driving an economy.\n"
                "- price_median / demand_median: the split lines. An economy "
                "is 'high' on an axis when it is above the median.\n\n"
                "REGIMES (classify each economy from its price/demand position, "
                "NOT from any precomputed 'quadrant' field, which may be "
                "mislabelled):\n"
                "- price high + demand high -> Goldilocks: demand firm, "
                "inflation under control.\n"
                "- price low + demand high -> Overheating: strong demand "
                "pushing inflation up.\n"
                "- price low + demand low -> Stagflation: inflation pressure "
                "with weak demand.\n"
                "- price high + demand low -> Disinflation / soft patch: "
                "inflation cooling as demand fades.\n"
                "A null score means that economy is missing an indicator this and do not mention that in the response but take it into account in the analysis."
                "month - flag it, do not guess.\n\n"
                f"DATA (JSON)\n{json.dumps(data)}\n\n"
                "TASK\n"
                "1. One sentence per economy: its regime and the single factor "
                "driving it, citing the relevant contrib_* or axis score.\n"
                "2. Two to three sentences on the cross-section as a whole: the "
                "outliers, where the group clusters, and any divergence between "
                "price pressure and demand strength across regions.\n"
                "3. One sentence on the read-through for monetary policy or "
                "markets.\n"
                "Be concrete and quantitative, reference actual scores, keep it "
                "under 200 words, and give no preamble or headings."
            )

            response = requests.post(
                "https://api.modelrail.dev/v1/chat/completions",
                headers={"Authorization": f"Bearer {self.secret.modelrail_key}"},
                json={
                    "model": "modelrail-auto",
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            completion = response.json()
            content = completion["choices"][0]["message"]["content"]
            return re.sub(r"\n[ \t]*\n+", "\n\n", content).strip()
        except Exception as e:
            logging.error(f"Error summarizing global breakdown: {e}", exc_info=True)
            raise

# if __name__ == "__main__":
#     test = LLMController()
#     data = {
#         "AUSTRALIAN DOLLAR": {
#             "asset_mgr": {
#                 "net_pct_oi": -15.97,
#                 "percentile": 44.2,
#                 "score": -12,
#                 "z": -0.2,
#                 "mom_4w": 3.49,
#                 "label": "balanced",
#             },
#             "lev_money": {
#                 "net_pct_oi": 18.17,
#                 "percentile": 73.1,
#                 "score": 46,
#                 "z": 0.7,
#                 "mom_4w": 5.11,
#                 "label": "leaning long",
#             },
#             "dealer": {
#                 "net_pct_oi": -12.42,
#                 "percentile": 48.1,
#                 "score": -4,
#                 "z": -0.15,
#                 "mom_4w": -8.82,
#                 "label": "balanced",
#             },
#         }
#     }
#     print(asyncio.run(test.breakdown_inst_positioning(data)))

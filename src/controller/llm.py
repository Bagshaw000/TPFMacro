"""LLM narration for the macro dashboards.

One class, LLMController, that turns a computed payload (a COT positioning
snapshot, or the Phillips-curve cross-section) into a short natural-language
summary by calling the ModelRail chat-completions API. Every public method is
a thin prompt-builder over the shared `_chat` helper, which owns the HTTP
client, the concurrency cap, and the 429/5xx retry logic.

The controller is a process-lifetime singleton (constructed once inside the
other controllers), so the pooled httpx client and the semaphore live for the
whole run.
"""

import asyncio
import json
import logging
import os
import random
import sys

# Run both as part of the package and directly; add src/ to sys.path so the
# bare `config.config` import below resolves in the script case too.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import httpx
from config.config import get_doppler_env   # Doppler-backed secrets (holds modelrail_key)
import re

# --- ModelRail chat-completions endpoint config -------------------------------
_LLM_URL = "https://api.modelrail.dev/v1/chat/completions"
_LLM_MODEL = "modelrail-auto"   # provider's auto-routed model
# LLM completions are slow - httpx defaults to a 5s timeout, which would abort
# most of these calls. Bump it well past a typical completion.
_LLM_TIMEOUT = httpx.Timeout(120.0)
# Max requests in flight at once. Callers asyncio.gather many of these (one per
# country / instrument); without a cap the whole batch fires simultaneously and
# the API returns 429. Keep this at or below the provider's concurrency limit.
_LLM_MAX_CONCURRENCY = 2
# Retry a call this many times on a transient status before giving up.
_LLM_MAX_RETRIES = 4
# Statuses worth retrying (rate limit + transient server errors). Any other
# non-2xx is raised immediately.
_LLM_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


class LLMController:
    """Builds prompts for the macro summaries and sends them to ModelRail.

    Public methods (breakdown_inst_positioning, global_breakdown,
    breakdown_by_country) each construct one prompt and hand it to `_chat`.
    `_chat` is where all the shared concerns live: the pooled HTTP client, the
    `_LLM_MAX_CONCURRENCY` semaphore, and retry-with-backoff on 429/5xx.
    """

    def __init__(self) -> None:
        # Doppler secret bundle. NOTE: get_doppler_env() catches its own errors
        # and returns None on failure, so a bad Doppler setup surfaces later as
        # an AttributeError on `self.secret.modelrail_key`.
        self.secret = get_doppler_env()
        # One pooled async client reused across every call, so gathered
        # requests share connections.
        self.client = httpx.AsyncClient(timeout=_LLM_TIMEOUT)
        # Gate concurrent requests so a fan-out (gather over N countries)
        # doesn't trip the provider's rate limit. Created here without a
        # running loop - asyncio.Semaphore binds lazily on first await.
        self._sem = asyncio.Semaphore(_LLM_MAX_CONCURRENCY)

    @staticmethod
    def _retry_delay(response: httpx.Response, attempt: int) -> float:
        """Seconds to wait before the next attempt: honour a Retry-After header
        if the server sent one, otherwise exponential backoff with jitter."""
        # Servers often send Retry-After on a 429 - prefer it when present and
        # numeric (seconds form; the HTTP-date form is not handled).
        header = response.headers.get("Retry-After")
        if header:
            try:
                return float(header)
            except ValueError:
                pass
        # Fallback: 1, 2, 4, 8, ... seconds (capped at 30) plus up to 1s of
        # jitter so concurrent retriers don't all wake at the same instant.
        return min(2 ** attempt, 30) + random.uniform(0, 1)

    async def _chat(self, prompt: str) -> str:
        """POST one single-message chat completion and return its cleaned text.

        Shared by every public method here - they differ only in the prompt
        they build. Concurrency is capped by `self._sem`; 429 / 5xx responses
        are retried up to `_LLM_MAX_RETRIES` times with backoff. Any other
        non-2xx is raised (raise_for_status), as is a malformed body
        (KeyError); callers log and re-raise.
        """
        headers = {"Authorization": f"Bearer {self.secret.modelrail_key}"}
        # Single-turn chat: the whole prompt goes in as one user message.
        payload = {"model": _LLM_MODEL, "messages": [{"role": "user", "content": prompt}]}

        # Hold a semaphore slot for the whole attempt cycle so retries count
        # against the concurrency budget too (a retrying call shouldn't let an
        # extra request slip in past the cap).
        async with self._sem:
            for attempt in range(_LLM_MAX_RETRIES):
                response = await self.client.post(_LLM_URL, headers=headers, json=payload)
                # Stop on success, on a non-retryable status, or once the last
                # attempt is used up - `response` then holds the final result.
                if response.status_code not in _LLM_RETRY_STATUS or attempt == _LLM_MAX_RETRIES - 1:
                    break
                delay = self._retry_delay(response, attempt)
                logging.warning(
                    f"LLM {response.status_code}; retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{_LLM_MAX_RETRIES})"
                )
                await asyncio.sleep(delay)

        # No-op on 2xx; raises for a final 429/5xx (retries exhausted) or any
        # other 4xx (auth, bad request). Callers log and re-raise.
        response.raise_for_status()
        # Body shape: {"choices": [{"message": {"content": "..."}}], ...} - a
        # KeyError here means an unexpected response and is treated as an error.
        content = response.json()["choices"][0]["message"]["content"]
        # Collapse runs of blank lines so the stored summary is tidy.
        return re.sub(r"\n[ \t]*\n+", "\n\n", content).strip()

    async def aclose(self) -> None:
        """Close the pooled HTTP client. Optional - the controller is a
        process-lifetime singleton, so in practice this is never called."""
        await self.client.aclose()

    async def breakdown_inst_positioning(self, data):
        """One-sentence read of a COT institutional-positioning payload.

        `data` is one instrument's {category: {net_pct_oi, percentile, score,
        z, mom_4w, label}} block from COTController._positioning_metrics.
        Returns a single plain-text sentence (the prompt bans headings and
        "Summary:" / "Conclusion:" scaffolding).
        """
        try:
            # Prompt = instructions + the raw metrics as JSON.
            prompt = (
                "Based on the commitment of trader position calculation, provide a "
                "max 20 word summary of what the data is suggesting.\n"
                "Draw a conclusion in the response as a full sentence and talk in most likely action.\n"
                "Response should be a sentence. Do not add these phrases and word like **Summary (20 words max):** and Conculsion in the response."
                f"This is the data in JSON format: {json.dumps(data)}"
            )
            return await self._chat(prompt)
        except Exception as e:
            logging.error(f"Error breaking down Instituitional positioning: {e}", exc_info=True)
            raise

    async def global_breakdown(self, data):
        """Narrative of the whole Phillips-curve cross-section snapshot
        (CrossSectionController.get_cross_section output: {"meta", "countries",
        ...}). Returns a <200-word multi-part summary.

        The prompt restates the scoring convention (higher = healthier, price
        HIGH = inflation contained) and gives its own regime definitions,
        telling the model to classify from the price/demand numbers rather than
        the stored `quadrant` field - which is mislabelled (see
        CrossSectionController.assign_quadrants).
        """
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
            return await self._chat(prompt)
        except Exception as e:
            logging.error(f"Error summarizing global breakdown: {e}", exc_info=True)
            raise

    async def breakdown_by_country(self, data, country: str | None = None):
        """Explain one economy's slot in the Phillips-curve cross-section.
        `data` is the per-country row from
        CrossSectionController.get_cross_section_by_country():
        {price, demand, composite, contrib_*, quadrant, price_median,
        demand_median}. `country` is the optional 3-letter code, for phrasing.
        Returns a 3-4 sentence summary.

        Same scoring/regime framing as global_breakdown, but for one economy:
        it compares that economy to the peer medians rather than surveying the
        whole group, and closes on that country's policy / currency.

        Callers parallelise across countries with
        asyncio.gather(*(self.llm.breakdown_by_country(row, code) for ...)).
        """
        try:
            # Name the economy in the prompt when we know it, else stay generic.
            who = country.upper() if country else "this economy"
            prompt = (
                f"You are a macro strategist. Explain where {who} sits in a "
                "cross-sectional Phillips-curve analysis of major economies. "
                "Every score is a 0-100 rank of this economy against the others "
                "in the same monthly snapshot; higher is always healthier.\n\n"
                "FIELDS\n"
                "- price: inflation pressure. HIGH = inflation contained (at or "
                "below target, soft producer prices); LOW = inflation running "
                "hot. Blends a CPI-vs-target score (55%) and a producer-price "
                "score (45%).\n"
                "- demand: demand strength. HIGH = tight labour market and "
                "accelerating real retail sales; LOW = labour-market slack and "
                "stalling retail. Blends an unemployment-gap score (60%) and a "
                "retail-momentum score (40%).\n"
                "- composite: overall health, all four factors weighted.\n"
                "- contrib_cpi / contrib_ppi / contrib_unemp / contrib_ret: "
                "each factor's weighted share of composite - the largest and "
                "smallest name what is helping and hurting this economy.\n"
                "- price_median / demand_median: the peer-group split lines. "
                "This economy is 'high' on an axis when its score is above the "
                "matching median, 'low' when below.\n\n"
                "REGIME (decide it from price vs price_median and demand vs "
                "demand_median, NOT from any 'quadrant' field, which may be "
                "mislabelled):\n"
                "- price high + demand high -> Goldilocks: firm demand, "
                "inflation under control.\n"
                "- price low + demand high -> Overheating: strong demand "
                "lifting inflation.\n"
                "- price low + demand low -> Stagflation: inflation pressure "
                "with weak demand.\n"
                "- price high + demand low -> Disinflation / soft patch: "
                "inflation cooling as demand fades.\n"
                "A null score means an indicator is missing this month - do not "
                "mention that in the response, but account for it in the "
                "analysis and do not guess the missing value.\n\n"
                f"DATA (JSON)\n{json.dumps(data)}\n\n"
                "TASK: 3-4 sentences. State the regime and how far this economy "
                "is from the peer medians on each axis; name the single factor "
                "most helping and the one most hurting, using the contrib_* "
                "values; close with the read-through for that country's central "
                "bank or currency. Be concrete and quantitative, cite the "
                "actual scores, no preamble or headings."
            )
            return await self._chat(prompt)
        except Exception as e:
            logging.error(f"Erros in summarizing the country {e}", exc_info=True)
            raise


# Manual smoke test - uncomment and run this file directly. Needs a valid
# Doppler modelrail_key and network access to the endpoint.
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

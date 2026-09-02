# TPFMacro

Backend for the TPFMacro dashboards. It ingests macro-economic and
futures-positioning data on a schedule, derives analytics from it, caches the
results in Redis, and serves them over a FastAPI HTTP/WebSocket API.

- **API** — `main:app` (FastAPI + Uvicorn), routers under `src/routes/`.
- **Worker** — `src.worker.WorkerSettings` (arq), scheduled cron jobs that keep
  the caches populated.
- **Store of record** — Postgres (Supabase). SQLModel / SQLAlchemy async.
- **Read cache** — Redis. Almost every API response is served from here.
- **Secrets** — Doppler (project `tpf_macro`, config `dev`), via a `DOPPLER_TOKEN`.
- **LLM narration** — ModelRail chat-completions, wrapped by `LLMController`.

The API and the worker never call each other. The worker writes to Redis /
Postgres; the API reads. Redis is the only thing they share.

---

## Repository layout

```
main.py                     FastAPI app + lifespan startup (repo root)
src/
  worker.py                 arq entry point — all cron jobs + their schedule
  cot.py                    CFTC COT ingest (Postgres <- CFTC weekly reports)
  routes/
    cot.py                  /v1/cot/*      COT positioning + net-position change
    macro.py                /v1/macro/*    cross-indicator reads, economic cycle
    symbol.py               /v1/symbol/*   quotes, snapshots, correlations, WS streams
  controller/               business logic — one class per domain
    cot.py                  COT positioning metrics, net-%-OI series, Redis cache
    cross_section.py        Phillips-curve quadrant snapshot ("Orthogonal View")
    macro.py                global averages + economic-cycle calculator
    lse_.py                 LSE economic-calendar sync (CPI/PPI/UNEMP/retail/inflation)
    economic_event.py       yfinance economic-calendar events (Redis only)
    news.py                 news-sentiment scoring (VADER)
    llm.py                  LLMController — prompt builders over a shared httpx client
  model/                    Postgres accessors (SQLModel queries)
  custom_types/             SQLModel table definitions + pydantic config types
  database/
    db.py                   async engine / session_scope
    redis_.py               RedisConnection — sync + async pooled clients
  config/config.py          get_doppler_env() — Doppler-backed secrets
sql/
  add_period_column.sql     migration: LSE tables keyed on (country_code, period)
Dockerfile                  builder + runtime image (Doppler CLI baked in)
docker-compose.yml          macro (API) · worker · redis · caddy · nats
```

Every module under `src/` puts `src/` on `sys.path` at import time, so imports
are bare (`from controller.cot import COTController`, `from model.cot import
CotModell`). A file directly in `src/` adds one `dirname`; a file in
`src/<subdir>/` adds two.

---

## Configuration

All secrets come from Doppler. Set a single environment variable:

```
DOPPLER_TOKEN=dp.st.dev.xxxxxxxx
```

`config/config.py::get_doppler_env()` (memoised) pulls the rest from project
`tpf_macro`, config `dev`: `SUPABASE_URL` / `SUPABASE_KEY`, `DB_*` (Postgres
DSN parts), `NEWS_*`, `LSE_KEY`, `MODELRAIL_KEY`, `TWITTER_TOKEN`, `PROXY_SERVER`.

Redis host is a plain env var, not a secret:

```
REDIS_HOST=localhost     # default; use "redis" inside docker-compose
```

---

## Running

### Docker (all services)

```bash
export DOPPLER_TOKEN=dp.st.dev.xxxx
docker compose up --build
```

Brings up `macro` (API on `:8000`), `worker` (arq), `redis`, `caddy`
(`:80`/`:443` reverse proxy), and `nats`. The API container runs
`doppler run -- uvicorn main:app --workers 2`; the worker container overrides
the command with `arq src.worker.WorkerSettings`.

### Local

```bash
python -m venv .venv && .venv/Scripts/activate      # Windows
pip install -r requirements.txt

export DOPPLER_TOKEN=dp.st.dev.xxxx
export REDIS_HOST=localhost                          # a local Redis must be up

# API
uvicorn main:app --reload --port 8000

# Worker (separate process, optional for local dev)
arq src.worker.WorkerSettings
```

Python 3.13. On Windows the worker forces
`WindowsSelectorEventLoopPolicy` (the default Proactor loop breaks some
psycopg/asyncpg internals).

### Database migration

One migration is checked in. Run it against the Postgres instance **before**
deploying code that depends on it:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/add_period_column.sql
```

It adds a `period` ("YYYY-MM" reference month) column to the LSE indicator
tables and switches their uniqueness key from `(country_code, report_date)` to
`(country_code, period)`, so a flash estimate, the final print and any revision
for the same month collapse onto one row instead of piling up. The file's
header block has the full pre-flight / verify / rollback checklist.

---

## Scheduled jobs (`src/worker.py`)

arq reads `WorkerSettings.cron_jobs`. Its own job-queue Redis is separate from
the app cache (`RedisSettings(host='redis')`).

| Schedule | Job | Effect |
|---|---|---|
| Wed 23:00 | `cot_update` | Sync new CFTC weekly COT reports into Postgres `cot_ttf` |
| Sat 00:30 | `curated_cot_positioning` | Rescore the curated shortlist → `cot_pos:_meta` + blobs (with LLM summaries) |
| Sun 01:00 | `full_cot_positioning` | Score every non-curated instrument → `cot_pos:_meta_all` + blobs |
| Daily 05:00 | `currency_snapshot` | Refresh currency market-overview cache |
| Sat 23:00 | `get_events` | Refresh yfinance economic-calendar events |
| 1/5/10/15/20/25/30 @ 23:00 | `get_lse` | Sync LSE economic indicators (CPI/PPI/UNEMP/retail/inflation) |
| Every 3h | `get_new_sentiment` | Re-score news sentiment per country |
| Sun 22:00 | `refresh_factor_stats` | Recompute trailing (μ, σ) per macro factor from Postgres |
| 1/10/15/20/25 @ 22:30 | `refresh_cross_section` | Re-run the Phillips-curve quadrant chain |

If you run only the API and not the worker, use `fastapi_utilities`'
`@repeat_every` inside `lifespan` for in-process scheduling instead. Keep the
heavy LLM fan-outs (`full_positioning`) on the worker regardless.

---

## Startup (`main.py` lifespan)

On boot the API warms the caches concurrently (failures are logged, not fatal):
`macro_ctrl.refresh_factor_stats`, `market_overview.get_currency`,
`lse_ctrl.get_event_cal`, `cot_ctrl.ensure_positioning`,
`cross_sec.update_quandrant`, then `macro_ctrl.get_global_cycle`.

`ensure_positioning` is staleness-guarded: if `cot_pos:_meta` exists and its
`updated` timestamp is under 4 days old it's a single `GET` and returns;
otherwise it does the full curated rebuild. So restarts and redeploys are cheap.

---

## Redis key map

| Key | Type | Written by | Read by |
|---|---|---|---|
| `cot_ttf:{market}:{name}:{date}` | hash | `COTController.insert_cot_redis` | every COT fetch helper |
| `cot_pos:{instrument}` | json str | `store_positioning`, `store_full_positioning` | `get_positioning`, `net_pct_oi_timeseries` |
| `cot_pos:_meta` | json str | `store_positioning` only | `get_positioning`, `store_full_positioning` (read-only) |
| `cot_pos:_meta_all` | json str | `store_full_positioning` only | `get_positioning(scope="all")` |
| `cot_status` | str | `setup_redis` | `setup_redis` |
| `cross_section:quadrant:{code}` / `:_meta` | json str | `CrossSectionController.store_quadrants` | `get_cross_section*` |
| `cross_section:breakdown*` | json str | `store_cross_section_breakdown` | `get_cross_section_breakdown*` |
| `{cpi\|ppi\|unemp\|retail\|inflation}:{code}` / `:avg` | json str | `LSEController.insert_redis` | `MacroController` |
| `{macro}:stats:{country}` | json str | `refresh_factor_stats` | `get_factor_stats` |
| `cycle:{country}:composite` | str | `MacroController` | economic-cycle reads |
| `sentiment_news:{country}` | str | `NewsSentimentController` | `MacroController` |
| `news:{country}:{event}` | hash | `EconomicEventController.store_economic_event` | `get_all_events`, `get_event_country` |

All keys carry a TTL; the producers refresh them well before expiry.

---

## API

### `/v1/cot` — Commitment of Traders

| Route | Returns |
|---|---|
| `GET /v1/cot/` | Net-position **pct change** (1/3/6/12-month) per instrument, grouped by asset class |
| `GET /v1/cot/cot_pos` | Cached **positioning snapshot** for the curated shortlist: `{meta, instruments: {asset: {category: {net_pct_oi, percentile, score, z, mom_4w, label}, summary}}}` |
| `GET /v1/cot/net_pct_oi?scope=tracked&weeks=52` | **Net % of open interest** as a weekly series, per trader category, for every instrument in the meta index. `scope` ∈ `tracked` \| `all`; `422` on bad params |
| `GET /v1/cot/asset_changes/{asset}?market=&weeks=52` | One instrument's last `weeks` reports: each trader group's **net-position series + trailing pct change**. `market` optional (looked up from Postgres); `404` if unknown |

### `/v1/macro`

| Route | Returns |
|---|---|
| `GET /v1/macro/global_avg` | `{macro: avg_value}` across all tracked countries |
| `GET /v1/macro/economies` | Economic-cycle summary for every tracked country |
| `GET /v1/macro/economy/{country}` | Latest reading per macro factor for one country |
| `GET /v1/macro/economy/timeseries/{country}` | Monthly factor series for one country (dates ISO-stringified) |
| `GET /v1/macro/cross_section` | Phillips-curve quadrant snapshot: `{meta, countries, summary}` |
| `GET /v1/macro/cross_section/{country}` | One country's quadrant row + narration |

### `/v1/symbol`

REST: `GET /v1/symbol/snapshot/{category}/{ticker}`,
`/corr/{category}/{ticker}`, `/technical/{category}/{ticker}`.
WebSocket: `/v1/symbol/ws/{category}/{pair}`, `/v1/symbol/event/{country}`,
`/v1/symbol/event` — each pushes a fresh payload every ~2 s.

`GET /health` — liveness.

---

## The COT module in depth (`src/controller/cot.py`)

`COTController` turns the CFTC's weekly *Traders in Financial Futures* report
into three things.

### Concepts

- **Trader groups** — the report splits reportable positions into buckets.
  `dealer` (sell-side counterparty), `asset_mgr` (institutions / "real money"),
  `lev_money` (hedge funds, CTAs / "fast money"), `other_rept`.
- **Net position** — `long − short` for a group. `insert_cot_redis` also stores
  derived `*_net` columns and `open_interest_all` on each weekly hash.
- **Net % of open interest** — `net / open_interest × 100`. Normalising by OI
  makes the number comparable across instruments and across time.
- **Positioning metrics** (`_positioning_metrics`) — for the most recent week,
  per group: `net_pct_oi`, `percentile` (rank within the trailing 52-week
  window), `score` (`2·percentile − 100`), `z` (population z-score over the
  window), `mom_4w` (4-week change), `label` (`_crowding_label`: crowded /
  stretched / leaning long ↔ short, on the 5/20/40/60/80/95 ladder).
- **Curated shortlist** (`COT_CURATED_ASSETS`) — ~11 macro-relevant contracts
  scored with an LLM summary each. The long tail is everything else.

### Data flow

```
CFTC weekly report
   └─ cot.py (src/) ──────────────────────────►  Postgres  cot_ttf
                                                    │
   COTController.insert_cot_redis ──────────────────┤ derive *_net, HSET
                                                    ▼
                              Redis  cot_ttf:{market}:{name}:{date}  (hashes, ~60wk TTL)
                                                    │
        _fetch_recent_weeks / _fetch_last_52 ───────┤ SCAN + pipelined HGETALL
                                                    ▼
   ┌────────────────────────┬───────────────────────┬──────────────────────────┐
   │ instituitional_pos     │ full_positioning      │ net_pct_oi_timeseries    │
   │ (curated)              │ (long tail)           │ asset_group_changes      │
   │   _positioning_metrics │   _positioning_metrics│   _net_pct_oi_history    │
   │   store_positioning    │   store_full_positioning│  (read only, no write) │
   └───────────┬────────────┴───────────┬───────────┴──────────────────────────┘
               ▼                        ▼
       Redis cot_pos:{asset}     Redis cot_pos:{asset}
            + cot_pos:_meta           + cot_pos:_meta_all
                        │
       get_positioning  │  net_pct_oi_timeseries  │  asset_group_changes
                        ▼
                  /v1/cot/*  →  frontend
```

### Key methods

| Method | Role |
|---|---|
| `ensure_positioning(max_age_hours=96)` | Startup loader — rebuild the curated snapshot only if missing / stale |
| `instituitional_pos()` | Curated flow: fetch → `_positioning_metrics` → `store_positioning` |
| `full_positioning(with_summary=True)` | Long-tail flow, same shape; `with_summary=False` skips the LLM |
| `_positioning_metrics(data, window=52, min_w=26)` | Vectorised percentile / z / momentum / label per group (pure CPU) |
| `_write_positioning(...)` | Shared writer — LLM fan-out + one pipeline of blobs + index |
| `store_positioning` / `store_full_positioning` | Build their own `meta` dict + key, delegate to `_write_positioning` |
| `get_positioning(scope="tracked")` | Read a snapshot back; `SCAN` fallback if the index expired |
| `net_pct_oi_timeseries(scope, weeks)` | Weekly net-%-OI series per group, for every instrument in the chosen index |
| `_net_pct_oi_history(by_date)` | One instrument's `{date: hash}` → `{group: [[date, value], ...]}` |
| `asset_group_changes(asset, market=None, weeks=52)` | One instrument: each group's net series + trailing pct change (1/3/6/12-mo) |
| `_fetch_recent_weeks(market, asset, weeks)` | SCAN + pipelined HGETALL for one instrument, oldest-first |

### Known issues

- Several `async def` methods on the legacy pct-change path (`get_cot_data`,
  `setup_redis`, …) call the **synchronous** `self.redis` client, which blocks
  the event loop. The positioning / time-series methods use `self.aioredis`
  throughout.
- `insert_cot_redis` — the outer batch loop iterates the whole frame each pass,
  so every hash is written `len(df) / batch_size` times.
- `setup_redis` — the `cot_status != 1` guard compares a string to an int, so
  the fast path never triggers.
- `interpret_pct_change`, `cot_asset_position`, `get_asset_year` are incomplete
  stubs.
- Method / field name typos are load-bearing (`instituitional_pos`,
  `new_covert_redis_dataframe`, `asset_mgr_Net`).

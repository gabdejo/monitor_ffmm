# fund-dashboard — Project Context for Claude Code

## Project purpose
A local web dashboard comparing the performance of 8–10 mutual funds against
their respective ETF benchmarks. Displays interactive wealth curves (index-100
rebased) and return tables (MTD, YTD, monthly breakdown). Also a learning
vehicle for JavaScript, HTML/CSS, and web deployment.

---

## Stack

| Layer | Choice |
|---|---|
| ETF data | `yfinance` |
| Fund data | `selenium` (regulator portal requires JS / login) |
| Processing | `pandas`, `numpy` |
| Visualization (dev/exploratory) | `plotly` (Python) |
| Storage | SQLite (`.sqlite` extension) |
| Backend | FastAPI + uvicorn |
| Frontend | Vanilla HTML + CSS + Plotly.js (via CDN) |
| Scheduling | `scheduler.py` using daily time trigger |
| Deployment | Render or small VPS |

---

## Virtual environment
- Folder: `.venv/` at project root
- Activate: `venv\Scripts\activate` (Windows)
- Install: `pip install -r requirements.txt`

### requirements.txt
```
yfinance
selenium
pandas
numpy
plotly
fastapi
uvicorn
pytest
python-dotenv
```

---

## Folder structure

```
fund-dashboard/
├── .venv/
├── db/
│   ├── schema.py          # CREATE TABLE IF NOT EXISTS — run once on setup
│   └── connection.py      # shared get_connection() helper, imported everywhere
├── data/
│   ├── raw/               # files as-downloaded from sources
│   ├── processed/         # cleaned, aligned CSVs ready for DB
│   └── fund_dashboard.sqlite
├── ingestion/
│   ├── etf_fetcher.py     # yfinance → fact_etf_prices
│   ├── fund_scraper.py    # selenium scraper → fact_fund_nav
│   └── scheduler.py       # daily trigger, calls run_ingestion.py
├── processing/
│   ├── returns.py         # MTD / YTD / monthly return calculations
│   └── rebase.py          # index-100 normalization for wealth curves
├── api/
│   ├── main.py            # FastAPI app instantiation
│   └── routes.py          # endpoint definitions
├── frontend/
│   └── index.html         # single page, imports Plotly.js via CDN
├── run.py                 # entry point: starts FastAPI server
├── run_ingestion.py       # entry point: runs full ingestion pipeline
├── CLAUDE.md
├── requirements.txt
├── .env
└── .gitignore
```

---

## Entry points

```bash
python db/schema.py        # first-time setup: creates DB and all tables
python run_ingestion.py    # manual or scheduled data refresh
python run.py              # starts FastAPI server
```

---

## Database schema (SQLite)

### `dim_funds`
| Column | Type | Notes |
|---|---|---|
| fund_id | TEXT PK | short internal slug e.g. `fondo_alpha` |
| fund_name | TEXT | full display name |
| currency | TEXT | default `PEN` |
| inception_date | DATE | |
| is_active | INTEGER | 1 = active |
| created_at | TIMESTAMP | |

### `dim_etfs`
| Column | Type | Notes |
|---|---|---|
| ticker | TEXT PK | Yahoo Finance symbol |
| etf_name | TEXT | |
| currency | TEXT | default `USD` |
| created_at | TIMESTAMP | |

### `dim_fund_benchmark`
One-to-one mapping between a fund and its ETF benchmark.
| Column | Type | Notes |
|---|---|---|
| fund_id | TEXT PK | FK → dim_funds |
| ticker | TEXT | FK → dim_etfs |
| effective_from | DATE | supports future benchmark changes |
| created_at | TIMESTAMP | |

### `fact_fund_nav`
Daily NAV data from the regulator portal.
| Column | Type | Notes |
|---|---|---|
| fund_id | TEXT | FK → dim_funds |
| date | DATE | reported date |
| nav | REAL | price per unit (valor cuota) |
| aum | REAL | patrimonio |
| units_outstanding | REAL | |
| ingested_at | TIMESTAMP | pipeline write time |
| PK | (fund_id, date) | prevents duplicates |

### `fact_etf_prices`
Daily adjusted close from Yahoo Finance.
| Column | Type | Notes |
|---|---|---|
| ticker | TEXT | FK → dim_etfs |
| date | DATE | reported date |
| adj_close | REAL | |
| ingested_at | TIMESTAMP | pipeline write time |
| PK | (ticker, date) | prevents duplicates |

### Key DB conventions
- `PRAGMA foreign_keys = ON` must be set on every connection — handled in `db/connection.py`
- `date` = source-reported date. `ingested_at` = pipeline audit timestamp. Never mix them.
- Use `INSERT OR IGNORE` to skip duplicates on re-ingestion, or `INSERT OR REPLACE` to overwrite with corrected values.

---

## .env variables

```bash
# Database
DB_PATH=data/fund_dashboard.sqlite

# Funds & benchmarks (comma-separated, order must match)
FUND_IDS="Fondo Alpha Renta Fija,Fondo Beta Acciones"
ETF_TICKERS=SPY,QQQ

# Regulator scraper
REGULATOR_URL=https://www.regulator.gob.pe/fondos
SCRAPER_HEADLESS=False
SCRAPER_TIMEOUT=30

# Scheduler
INGESTION_TIME=18:30

# API
API_HOST=127.0.0.1
API_PORT=8000

# Environment
ENV=development
```

### Loading in Python
```python
from dotenv import load_dotenv
import os

load_dotenv()

DB_PATH     = os.getenv("DB_PATH")
FUND_IDS    = os.getenv("FUND_IDS").split(",")
ETF_TICKERS = os.getenv("ETF_TICKERS").split(",")
```

- Fund/ETF pairing: `zip(FUND_IDS, ETF_TICKERS)` — index position defines the relationship
- No spaces around commas in env values to avoid leading/trailing whitespace bugs
- `SCRAPER_HEADLESS=False` while login/session handling is manual; flip to `True` once automated

---

## .gitignore conventions

```
# Virtual environment
.venv/

# Data & database (ignore contents, preserve folder structure)
data/*
*.sqlite
!data/.gitkeep
!data/raw/.gitkeep
!data/processed/.gitkeep

# Environment variables
.env

# Python cache
__pycache__/
*.pyc
*.pyo
```

---

## General conventions
- All DB access goes through `db/connection.py` — never hardcode `DB_PATH` in other modules
- Each ingestion module (`etf_fetcher.py`, `fund_scraper.py`) is responsible for writing its own data
- `run_ingestion.py` only orchestrates execution order — no business logic lives there
- Processing logic (returns, rebase) lives exclusively in `processing/` — ingestion modules do not compute returns
- API routes only read from DB — no writes, no processing logic in `api/`

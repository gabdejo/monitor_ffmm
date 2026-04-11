import pandas as pd
from fastapi import APIRouter, Query

from db.schema import get_connection
from processing.returns import compute_all
from processing.rebase import align_and_rebase

router = APIRouter()


# ---------------------------------------------------------------------------
# DB helpers — query raw data into pandas Series; no processing logic here
# ---------------------------------------------------------------------------

def _load_benchmark_map(conn) -> dict[str, str]:
    """Return {fund_id: ticker} for all mapped funds."""
    rows = conn.execute(
        "SELECT fund_id, ticker FROM dim_fund_benchmark"
    ).fetchall()
    return {row["fund_id"]: row["ticker"] for row in rows}


def _load_fund_nav(conn, fund_ids: list[str]) -> dict[str, pd.Series]:
    """Return {fund_id: Series(nav, DatetimeIndex)} for the given fund_ids."""
    if not fund_ids:
        return {}
    placeholders = ",".join("?" * len(fund_ids))
    df = pd.read_sql(
        f"SELECT fund_id, date, nav FROM fact_fund_nav"
        f" WHERE fund_id IN ({placeholders}) ORDER BY date",
        conn,
        params=fund_ids,
        parse_dates=["date"],
    )
    return {
        fund_id: group.set_index("date")["nav"]
        for fund_id, group in df.groupby("fund_id")
    }


def _load_etf_prices(conn, tickers: list[str]) -> dict[str, pd.Series]:
    """Return {ticker: Series(adj_close, DatetimeIndex)} for the given tickers."""
    if not tickers:
        return {}
    placeholders = ",".join("?" * len(tickers))
    df = pd.read_sql(
        f"SELECT ticker, date, adj_close FROM fact_etf_prices"
        f" WHERE ticker IN ({placeholders}) ORDER BY date",
        conn,
        params=tickers,
        parse_dates=["date"],
    )
    return {
        ticker: group.set_index("date")["adj_close"]
        for ticker, group in df.groupby("ticker")
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/funds")
def get_funds():
    """
    List all funds with their benchmark ETF ticker and basic metadata.
    """
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT f.fund_id, f.fund_name, f.currency, f.inception_date, b.ticker
            FROM dim_funds f
            LEFT JOIN dim_fund_benchmark b ON f.fund_id = b.fund_id
            ORDER BY f.fund_name
        """).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


@router.get("/wealth-curve")
def get_wealth_curve(start_date: str | None = Query(default=None)):
    """
    Rebased index-100 wealth curves for all funds and their ETF benchmarks.

    Query params:
        start_date: YYYY-MM-DD anchor date. Defaults to the latest common
                    first date across all series so every line starts at 100
                    on the same day.

    Response:
        {
            "dates": ["2024-01-02", ...],
            "series": {
                "<fund_id>": [100.0, 101.3, ...],
                "<ticker>":  [100.0,  99.8, ...]
            }
        }
    """
    conn = get_connection()
    try:
        benchmark_map = _load_benchmark_map(conn)
        fund_nav = _load_fund_nav(conn, list(benchmark_map.keys()))
        etf_prices = _load_etf_prices(conn, list(set(benchmark_map.values())))
    finally:
        conn.close()

    anchor = pd.Timestamp(start_date) if start_date else None
    df = align_and_rebase({**fund_nav, **etf_prices}, start_date=anchor)

    if df.empty:
        return {"dates": [], "series": {}}

    return {
        "dates": [str(d.date()) for d in df.index],
        "series": {col: df[col].round(4).tolist() for col in df.columns},
    }


@router.get("/returns")
def get_returns():
    """
    MTD, YTD, and monthly breakdown for every fund and its ETF benchmark.

    Response:
        [
            {
                "fund_id": "...",
                "fund_name": "...",
                "ticker": "...",
                "fund":      {"mtd": 0.012, "ytd": -0.04, "monthly": {...}},
                "benchmark": {"mtd": 0.015, "ytd": -0.03, "monthly": {...}}
            },
            ...
        ]
    """
    conn = get_connection()
    try:
        benchmark_map = _load_benchmark_map(conn)
        fund_nav = _load_fund_nav(conn, list(benchmark_map.keys()))
        etf_prices = _load_etf_prices(conn, list(set(benchmark_map.values())))
        fund_names = {
            row["fund_id"]: row["fund_name"]
            for row in conn.execute("SELECT fund_id, fund_name FROM dim_funds").fetchall()
        }
    finally:
        conn.close()

    return [
        {
            "fund_id": fund_id,
            "fund_name": fund_names.get(fund_id, fund_id),
            "ticker": ticker,
            "fund": compute_all(fund_nav.get(fund_id, pd.Series(dtype=float))),
            "benchmark": compute_all(etf_prices.get(ticker, pd.Series(dtype=float))),
        }
        for fund_id, ticker in benchmark_map.items()
    ]

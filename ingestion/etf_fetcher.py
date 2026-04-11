import yfinance as yf
from datetime import datetime, timezone

from db.schema import get_connection


def _upsert_dim_etf(conn, ticker: str):
    try:
        info = yf.Ticker(ticker).info
        etf_name = info.get("shortName", ticker)
        currency = info.get("currency", "USD")
    except Exception:
        etf_name = ticker
        currency = "USD"

    conn.execute(
        """
        INSERT OR IGNORE INTO dim_etfs (ticker, etf_name, currency)
        VALUES (?, ?, ?)
        """,
        (ticker, etf_name, currency),
    )


def fetch_etf_prices(tickers: list[str], start_date: str, end_date: str):
    """
    Fetch adjusted close prices for each ticker from yfinance
    and persist to fact_etf_prices.

    Args:
        tickers:    list of Yahoo Finance ticker symbols
        start_date: 'YYYY-MM-DD'
        end_date:   'YYYY-MM-DD'
    """
    conn = get_connection()

    try:
        for ticker in tickers:
            print(f"Fetching {ticker}...")

            df = yf.Ticker(ticker).history(
                start=start_date,
                end=end_date,
                auto_adjust=True,
            )

            if df.empty:
                print(f"  No data returned for {ticker}, skipping")
                continue

            _upsert_dim_etf(conn, ticker)

            ingested_at = datetime.now(timezone.utc).isoformat()
            rows = [
                (ticker, str(idx.date()), float(row["Close"]), ingested_at)
                for idx, row in df.iterrows()
            ]

            conn.executemany(
                """
                INSERT OR IGNORE INTO fact_etf_prices (ticker, date, adj_close, ingested_at)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            print(f"  {len(rows)} rows written for {ticker}")

    finally:
        conn.close()

import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "data/fund_dashboard.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def create_schema():
    conn = get_connection()
    cursor = conn.cursor()

    # ------------------------------------------------------------------
    # dim_funds
    # One row per mutual fund. Fund ID is a short internal slug
    # (e.g. "fondo_alpha") used as FK across all tables.
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_funds (
            fund_id         TEXT PRIMARY KEY,
            fund_name       TEXT NOT NULL,
            currency        TEXT NOT NULL DEFAULT 'USD',
            inception_date  DATE,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # dim_etfs
    # One row per ETF benchmark. Ticker is the Yahoo Finance symbol.
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_etfs (
            ticker          TEXT PRIMARY KEY,
            etf_name        TEXT,
            currency        TEXT NOT NULL DEFAULT 'USD',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # dim_fund_benchmark
    # One-to-one mapping between a fund and its ETF benchmark.
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_fund_benchmark (
            fund_id         TEXT PRIMARY KEY,
            ticker          TEXT NOT NULL,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (fund_id) REFERENCES dim_funds (fund_id),
            FOREIGN KEY (ticker)  REFERENCES dim_etfs  (ticker)
        )
    """)

    # ------------------------------------------------------------------
    # fact_fund_nav
    # Daily NAV, AUM and units outstanding per fund,
    # as reported by the regulator portal.
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_fund_nav (
            fund_id             TEXT    NOT NULL,
            date                DATE    NOT NULL,
            nav                 REAL    NOT NULL,
            aum                 REAL,
            units_outstanding   REAL,
            ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (fund_id, date),
            FOREIGN KEY (fund_id) REFERENCES dim_funds (fund_id)
        )
    """)

    # ------------------------------------------------------------------
    # fact_etf_prices
    # Daily adjusted close price per ETF ticker.
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_etf_prices (
            ticker          TEXT    NOT NULL,
            date            DATE    NOT NULL,
            adj_close       REAL    NOT NULL,
            ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, date),
            FOREIGN KEY (ticker) REFERENCES dim_etfs (ticker)
        )
    """)

    conn.commit()
    conn.close()
    print(f"Schema created successfully at: {DB_PATH}")


if __name__ == "__main__":
    create_schema()

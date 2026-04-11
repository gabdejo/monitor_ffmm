import sys
from dotenv import load_dotenv
from pathlib import Path
from datetime import date
import os

load_dotenv()

RAW_PATH = Path(os.getenv("RAW_PATH")).resolve()
FUND_IDS = os.getenv("FUND_IDS").split(",")
ETF_TICKERS = os.getenv("ETF_TICKERS").split(",")

from ingestion.scrape_smv import extract_vc, parse_and_store, dmy2ymd
from ingestion.etf_fetcher import fetch_etf_prices
from db.schema import create_schema, seed_fund_benchmark

SAF_NAME = "SCOTIA FONDOS SOCIEDAD ADMINISTRADORA DE FONDOS MUTUOS S.A."
START_DATE = "01/01/2024"  # DD/MM/YYYY

if __name__ == "__main__":
    end_date_dmy = date.today().strftime("%d/%m/%Y")
    start_date_ymd = dmy2ymd(START_DATE)
    end_date_ymd = date.today().strftime("%Y-%m-%d")

    # Ensure schema exists (idempotent)
    create_schema()

    failed = []

    # 1. Scrape and parse each fund NAV
    for fund_id in FUND_IDS:
        print(f"\n[Fund] {fund_id}")
        try:
            extract_vc(
                start_date=START_DATE,
                end_date=end_date_dmy,
                download_path=RAW_PATH,
                saf_name=SAF_NAME,
                fund_name=fund_id,
            )
            xls_path = RAW_PATH / f"{fund_id}_{start_date_ymd}_{end_date_ymd}_run{date.today()}.xls"
            parse_and_store(xls_path, fund_id)
        except Exception as e:
            print(f"  ERROR: {e}")
            failed.append(fund_id)

    # 2. Fetch ETF benchmark prices
    print("\n[ETF]")
    try:
        fetch_etf_prices(ETF_TICKERS, start_date=start_date_ymd, end_date=end_date_ymd)
    except Exception as e:
        print(f"  ERROR: {e}")
        failed.append("ETF fetch")

    # 3. Seed fund-benchmark mapping (INSERT OR IGNORE — safe to repeat)
    print("\n[Benchmark mapping]")
    try:
        seed_fund_benchmark(FUND_IDS, ETF_TICKERS)
    except Exception as e:
        print(f"  ERROR: {e}")
        failed.append("Benchmark mapping")

    # Summary
    total = len(FUND_IDS) + 2  # funds + ETF fetch + benchmark mapping
    print(f"\n{'='*40}")
    if failed:
        print(f"Completed with {len(failed)}/{total} failure(s): {failed}")
        sys.exit(1)
    else:
        print(f"All {total} steps completed successfully.")

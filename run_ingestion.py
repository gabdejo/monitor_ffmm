from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

DB_PATH = os.getenv("DB_PATH")
RAW_PATH = Path(os.getenv('RAW_PATH')).resolve()

FUND_IDS = os.getenv("FUND_IDS").split(",")
ETF_TICKERS = os.getenv("ETF_TICKERS").split(",")

from ingestion.scrape_smv import extract_vc
from datetime import date, timedelta

if __name__ == '__main__':
    
    for FUND_ID in FUND_IDS:

        extract_vc(
            start_date='01/01/2026',
            end_date=date.today().strftime('%d/%m/%Y'),
            download_path=RAW_PATH,
            saf_name='SCOTIA FONDOS SOCIEDAD ADMINISTRADORA DE FONDOS MUTUOS S.A.',
            fund_name=FUND_ID
        )
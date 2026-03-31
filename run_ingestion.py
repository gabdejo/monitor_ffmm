from dotenv import load_dotenv
import os

load_dotenv()

DB_PATH = os.getenv("DB_PATH")
FUND_IDS = os.getenv("FUND_IDS").split(",")
ETF_TICKERS = os.getenv("ETF_TICKERS").split(",")

from ingestion.scrape_smv import extract_vc
from datetime import date, timedelta

if __name__ == '__main__':
    

    extract_vc(
        start_date='01/01/2026',
        end_date=date.today().strftime('%d/%m/%Y'),
        download_path=r'C:\Users\CRISTINA\Documents\Gabriel\Desarrollo\Python\Proyectos\monitor_ffmm\data\raw',
        saf_name='SCOTIA FONDOS SOCIEDAD ADMINISTRADORA DE FONDOS MUTUOS S.A.'
    )

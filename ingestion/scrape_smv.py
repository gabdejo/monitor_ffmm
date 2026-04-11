from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from pathlib import Path
from io import StringIO
import os
import time
from datetime import datetime, timezone

import pandas as pd

from db.schema import get_connection

ID_SAF = "MainContent_cboDenominacionSocial"
ID_FONDO = "MainContent_cboFondo"

ID_FECHA_DESDE = "txtFechDesde"
ID_FECHA_HASTA = "txtFechHasta"

ID_BUSCAR = "MainContent_btnBuscar"

ID_LOADING_LABEL = "lblCargando"
ID_LOADING_MODAL = "myLoading"
ID_EXCEL = "MainContent_imgexcel"

WAIT_TIMEOUT = 15

def create_driver(download_path: Path) -> webdriver.Chrome:
    options = Options()
    prefs = {
        "download.default_directory": str(download_path),
        "download.prompt_for_download": False,
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)
    return driver

def safe_find(driver, by, value, timeout=60):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )

def retry(action, retries=3, delay=1):
    for _ in range(retries):
        try:
            return action()
        except StaleElementReferenceException:
            time.sleep(delay)
    raise

def get_saf_list(driver) -> list[str]:
    dropdown = safe_find(driver, By.ID, ID_SAF)
    select = Select(dropdown)

    return [
        opt.text
        for opt in select.options
        if opt.text != "Ingrese nombre de la empresa"
    ]

def select_saf(driver, saf: str):
    retry(lambda: Select(
        safe_find(driver, By.ID, ID_SAF)
    ).select_by_visible_text(saf))

def get_fondo_texts(driver):
    try:
        select = Select(safe_find(driver, By.ID, ID_FONDO))
        return [opt.text.strip() for opt in select.options]
    except StaleElementReferenceException:
        return []

def wait_for_fondo_ready(driver, timeout=180):
    WebDriverWait(driver, timeout).until(
        lambda d: (
            len(get_fondo_texts(d)) >= 3 and
            any(
                t not in ("--SELECCIONE FONDO--", "TODOS")
                for t in get_fondo_texts(d)
            )
        )
    )

def wait_for_fondo_refresh(driver, previous_options, timeout=180):
    WebDriverWait(driver, timeout).until(
        lambda d: get_fondo_texts(d) != previous_options
    )

def wait_for_fondo_state(driver, timeout=60):
    '''
    Wait for final state from fondo dropdown and return True if has "TODOS",
    and false if it has "--SIN DATO--"
    '''

    try:
        old_select = safe_find(driver, By.ID, ID_FONDO, timeout=5)
    except TimeoutException:
        old_select = None

    if old_select is not None:
        WebDriverWait(driver, timeout).until(
            EC.staleness_of(old_select)
        )

    new_select_el = safe_find(driver, By.ID, ID_FONDO, timeout=timeout)
    select = Select(new_select_el)

    options = [opt.text.strip() for opt in select.options]

    if options == ["--SIN DATO--"]:
        return False

    if (
        len(options) >= 2
        and options[0] in ['--SELECCIONE FONDO--', '---SELECCIONE TIPO FONDO---'] #== "--SELECCIONE FONDO--"
        and "TODOS" in options
    ):
        return True

    raise RuntimeError(
        f"Estado inesperado del dropdown fondo: {options}"
    )

def select_fund(driver, fund_name: str | None = None) -> bool:
    if not fund_name:
        fund_name = 'TODOS'
    
    try:
        wait_for_fondo_ready(driver)
        retry(lambda: Select(
            safe_find(driver, By.ID, ID_FONDO)
        ).select_by_visible_text(fund_name))
        return True
    except:
        return False

def run_search(driver, search_button_id):
    retry(lambda: safe_find(driver, By.ID, search_button_id).click())

    WebDriverWait(driver, 900).until_not(
        EC.visibility_of_element_located((By.ID, ID_LOADING_LABEL))
    )

    wait_until_modal_gone(driver)

def wait_for_results_update(driver, previous_html, timeout=600):
    WebDriverWait(driver, timeout).until(
        lambda d: d.page_source != previous_html
    )

def wait_for_search_result(driver, timeout=900):
    wait = WebDriverWait(driver, timeout)

    try:
        wait.until(
            lambda d: (
                len(d.find_elements(By.ID, ID_EXCEL)) > 0
                or "No hay fondos" in d.page_source
            )
        )
    except TimeoutException:
        raise TimeoutException("Search did not finish in expected time")
    
def wait_until_modal_gone(driver, timeout=900):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("""
            const m = document.getElementById('myLoading');
            if (!m) return true;
            const style = window.getComputedStyle(m);
            return (
                style.display === 'none' ||
                style.visibility === 'hidden' ||
                style.opacity === '0' ||
                style.pointerEvents === 'none'
            );
        """)
    )

    WebDriverWait(driver, 300).until(
        lambda d: len(d.find_elements(By.CLASS_NAME, "modal-backdrop")) == 0
    )

def outcome_present(driver):
    if driver.find_elements(By.ID, ID_EXCEL):
        return True #"HAS_RESULTS"

    # no_data_labels = driver.find_elements(
    #     By.XPATH,
    #     "//label[contains(text(), 'No se encontraron registros')]"
    # )
    # if no_data_labels:
    #     return "NO_RESULTS"

    return False  # keep waiting

def export_excel(driver) -> bool:
    try:
        btn = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, ID_EXCEL))
        )
        btn.click()
        return True
    except Exception:
        return False

def list_files(path):
    return set(os.listdir(path))

def snapshot_files(path):
    return {f.name for f in Path(path).glob("*")}

def wait_for_download(download_dir, before_files, timeout=600):
    start = time.time()
    print('Detecting crd')

    # wait for download to start (.crdownload appears)
    while time.time() - start < timeout:
        current = list_files(download_dir)
        if any(f.endswith(".crdownload") for f in current - before_files):
            print('crd found')
            break
        time.sleep(0.5)
    else:
        raise TimeoutError("Download did not start")

    # wait for download to finish (.crdownload disappears)
    while time.time() - start < timeout:
        current = list_files(download_dir)
        if not any(f.endswith(".crdownload") for f in current):
            print('crd disappeared')
            return
        time.sleep(1)

    raise TimeoutError("Download did not finish")

def wait_for_new_download(
    download_dir,
    before_snapshot,
    timeout=60,
    stable_secs=1.0
):
    """
    Waits until a new file appears and stops changing size.
    Returns the Path to the downloaded file.
    """

    download_dir = Path(download_dir)
    start = time.time()
    last_sizes = {}

    while time.time() - start < timeout:
        current_files = {f.name for f in download_dir.glob("*")}
        new_files = current_files - before_snapshot

        # Ignore temporary files
        new_files = {
            f for f in new_files
            if not f.endswith(".crdownload")
        }

        if new_files:
            file_path = download_dir / next(iter(new_files))

            size = file_path.stat().st_size
            prev_size = last_sizes.get(file_path.name)

            if prev_size == size:
                time.sleep(stable_secs)
                if file_path.stat().st_size == size:
                    return file_path

            last_sizes[file_path.name] = size

        time.sleep(0.2)

    raise TimeoutError("La descarga no apareció o no se estabilizó")

def rename_new_file(download_dir, before_files, new_name):
    
    new_file = list_files(download_dir) - before_files

    print(list(new_file)[0])

    if len(new_file) > 1:
        raise ValueError('More than one new file in path')
    
    os.rename(
        os.path.join(download_dir, list(new_file)[0]),
        os.path.join(download_dir, new_name)
    )
    print(new_name)

def set_date_range(driver, wait, start_date: str, end_date: str):
    desde = safe_find(driver, By.ID, "txtFechDesde")
    hasta = safe_find(driver, By.ID, "txtFechHasta")

    desde.clear()
    desde.send_keys(start_date + Keys.ENTER)

    hasta.clear()
    hasta.send_keys(end_date + Keys.ENTER)

def dmy2ymd(str_date_dmy):
    return str(datetime.strptime(str_date_dmy, '%d/%m/%Y').strftime('%Y-%m-%d'))

def _run_extraction(
    *,
    url: str,
    search_button_id: str,
    download_path: Path,
    start_date: str,
    end_date: str,
    saf_name: str | None = None,
    fund_name: str | None = None,
):
    driver = create_driver(download_path)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        driver.get(url)

        if saf_name:
            saf_list = [saf_name]
        else:
            saf_list = get_saf_list(driver)

        for saf in saf_list:
            if fund_name:
                print(f"Descargando: {fund_name}-{saf}")
            else:
                print(f"Descargando: TODOS-{saf}")

            select_saf(driver, saf)

            if not wait_for_fondo_state(driver):
                print("No hay fondos, pasando...")
                continue

            select_fund(driver, fund_name)

            set_date_range(driver, wait, start_date, end_date)
            file_name = f'{fund_name}_{dmy2ymd(start_date)}_{dmy2ymd(end_date)}_run{datetime.today().date()}.xls'

            run_search(driver, search_button_id)

            WebDriverWait(driver, 300).until(
                lambda d: len(d.find_elements(By.CLASS_NAME, "modal-backdrop")) == 0
                )
            
            if not outcome_present(driver):
                print('No hay datos, pasando...')
                continue

            excel_btn = WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.ID, ID_EXCEL))
                )
            
            prev_files = snapshot_files(download_path)

            excel_btn.click()
            
            wait_for_new_download(download_path, prev_files)
            
            rename_new_file(download_path, prev_files, file_name)

    finally:
        driver.quit()

def extract_vc(
    start_date: str,
    end_date: str,
    download_path: Path,
    saf_name: str | None = None,
    fund_name: str | None = None    
):
    _run_extraction(
        url="https://www.smv.gob.pe/SIMV/Frm_EVCP?data=5A959494701B26421F184C081CACF55BFA328E8EBC",
        search_button_id="MainContent_btnBuscar",
        start_date=start_date,
        end_date=end_date,
        download_path=download_path,
        saf_name=saf_name,
        fund_name=fund_name
    )


def parse_and_store(xls_path: Path, fund_id: str):
    """
    Parse a downloaded .xls (HTML-embedded) from SMV and write to fact_fund_nav.
    Also upserts the fund into dim_funds if not already present.

    Column positions in the SMV report:
        0  Fondo              — full display name
        1  Fecha Información  — date (DD/MM/YYYY)  → date
        2  Fecha Inicio       — inception date
        3  Tipo Fondo
        4  Valor Cuota        → nav
        5  Patrimonio         → aum
        6  Partícipes
        7  Nº Cuotas          → units_outstanding
        8  Tipo Cambio

    Args:
        xls_path: path to the downloaded .xls file
        fund_id:  identifier used as PK in dim_funds / FK in fact_fund_nav
    """
    with open(xls_path, encoding="latin-1") as f:
        content = f.read()

    df = pd.read_html(StringIO(content))[0]

    # Fund metadata from first row
    fund_display_name = df.iloc[0, 0]
    inception_str = df.iloc[0, 2]
    currency = "USD" if "DOLARES" in str(fund_display_name).upper() else "PEN"
    inception_date = datetime.strptime(inception_str, "%d/%m/%Y").strftime("%Y-%m-%d")

    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO dim_funds (fund_id, fund_name, currency, inception_date)
            VALUES (?, ?, ?, ?)
            """,
            (fund_id, fund_display_name, currency, inception_date),
        )

        ingested_at = datetime.now(timezone.utc).isoformat()
        rows = [
            (
                fund_id,
                datetime.strptime(str(row.iloc[1]), "%d/%m/%Y").strftime("%Y-%m-%d"),
                float(row.iloc[4]),
                float(row.iloc[5]),
                float(row.iloc[7]),
                ingested_at,
            )
            for _, row in df.iterrows()
        ]

        conn.executemany(
            """
            INSERT OR IGNORE INTO fact_fund_nav
                (fund_id, date, nav, aum, units_outstanding, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        print(f"  {len(rows)} rows written for {fund_id}")
    finally:
        conn.close()
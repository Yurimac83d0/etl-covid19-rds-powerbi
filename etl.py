#!/usr/bin/env python3
import os
import logging
from io import StringIO, BytesIO
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- Configurações ---
SRC_URL_GITHUB = "https://github.com/owid/covid-19-data/raw/master/public/data/owid-covid-data.csv"
SRC_URL_BRASILIO = "https://brasil.io/dataset/covid19/caso_full.csv.gz"
PAIS = "Brazil"
OUT_CSV = f"covid_{PAIS.lower()}_latest.csv"

# --- Configurações PostgreSQL RDS ---
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "Cassio2002")  # você pode usar variável de ambiente para segurança
PG_HOST = os.environ.get("PG_HOST", "database-1.crukqu8ay4l0.sa-east-1.rds.amazonaws.com")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DBNAME = os.environ.get("PG_DBNAME", "postgres")

# --- Helper engine ---
def make_engine(user, password, host, port, dbname):
    from urllib.parse import quote_plus
    pw = quote_plus(password)
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}"
    return create_engine(url, connect_args={"connect_timeout": 10})

# --- ETL functions ---
def extrair_csv_com_retry(urls, tentativas=3, espera=5, compress=None):
    for url in urls:
        logging.info("Tentando baixar: %s", url)
        for tentativa in range(1, tentativas+1):
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                if compress:
                    bio = BytesIO(resp.content)
                    df = pd.read_csv(bio, compression=compress, low_memory=False)
                else:
                    df = pd.read_csv(StringIO(resp.text), low_memory=False)
                logging.info("Download OK: %s (linhas=%d, cols=%d)", url, df.shape[0], df.shape[1])
                return df
            except Exception as e:
                logging.warning("Falha tentativa %d/%d para %s — %s", tentativa, tentativas, url, str(e))
                import time; time.sleep(espera)
        logging.info("Mudando para próximo URL (fallback).")
    raise ConnectionError("Falha ao baixar de todas as fontes disponíveis.")

def transformar(df, pais):
    pais_lower = pais.lower()
    if 'location' in df.columns:
        df_pais = df[df['location'].str.lower() == pais_lower].copy()
    elif 'country' in df.columns:
        df_pais = df[df['country'].str.lower() == pais_lower].copy()
    elif 'state' in df.columns and pais_lower in ['brazil', 'brasil']:
        df_pais = df[df['state'].isna()].copy()
    else:
        raise ValueError("Não foi possível identificar coluna de país.")
    cols = ['date','location','country','state','city','total_cases','new_cases','total_deaths',
            'new_deaths','population','new_tests','total_tests','people_vaccinated','people_fully_vaccinated',
            'icu_patients','hosp_patients','stringency_index']
    cols_existentes = [c for c in cols if c in df_pais.columns]
    df_pais = df_pais[cols_existentes]
    df_pais['date'] = pd.to_datetime(df_pais['date'], errors='coerce')
    df_pais = df_pais.sort_values('date').reset_index(drop=True)
    for c in ['new_cases','new_deaths','total_cases','total_deaths','new_tests','total_tests']:
        if c in df_pais.columns:
            df_pais[c] = pd.to_numeric(df_pais[c], errors='coerce').fillna(0)
    if 'population' in df_pais.columns and 'total_cases' in df_pais.columns:
        df_pais['cases_per_100k'] = (df_pais['total_cases'] / df_pais['population']) * 100_000
    else:
        df_pais['cases_per_100k'] = None
    return df_pais

def validar(df):
    problemas = []
    if df['date'].isna().any():
        problemas.append(f"{df['date'].isna().sum()} linhas com data inválida")
    for col in ['new_cases','new_deaths']:
        if col in df.columns and (df[col] < 0).any():
            problemas.append(f"Valores negativos em {col}")
    if df.duplicated(subset=['date']).any():
        problemas.append("Datas duplicadas detectadas")
    ultima_data = df['date'].max()
    logging.info(f"Última data disponível: {ultima_data.date() if pd.notnull(ultima_data) else 'N/A'}")
    return problemas

def etl_pipeline(pais=PAIS, save_csv=True, save_postgres=True):
    try:
        df_raw = extrair_csv_com_retry([SRC_URL_GITHUB], tentativas=3, espera=3, compress=None)
    except Exception:
        df_raw = extrair_csv_com_retry([SRC_URL_BRASILIO], tentativas=3, espera=3, compress='gzip')
    df = transformar(df_raw, pais)
    issues = validar(df)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_name_ts = f"covid_{pais.lower()}_{ts}.csv"
    if save_csv:
        df.to_csv(out_name_ts, index=False)
        df.to_csv(OUT_CSV, index=False)
        logging.info("CSVs salvos localmente.")
    if save_postgres:
        engine = make_engine(PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DBNAME)
        df.to_sql(f"covid_{pais.lower()}", engine, if_exists="replace", index=False, method="multi", chunksize=1000)
        logging.info("Dados gravados no PostgreSQL RDS.")
    return df, issues

# --- Execução ---
if __name__ == "__main__":
    df_final, issues = etl_pipeline()
    print("Problemas encontrados:", issues)

    # --- Plot (salva imagem local, útil se não tiver X11) ---
    plt.figure(figsize=(12,5))
    plt.plot(df_final['date'], df_final['new_cases'], linewidth=1, label='Novos casos')
    plt.plot(df_final['date'], df_final['new_cases'].rolling(7, min_periods=1).mean(), linewidth=2, label='Média móvel (7d)')
    plt.title(f"Evolução diária de casos - {PAIS}")
    plt.xlabel("Data")
    plt.ylabel("Casos")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("covid_plot.png")
    logging.info("Gráfico salvo como covid_plot.png")

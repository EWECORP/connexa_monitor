# db.py
import os
from functools import lru_cache
from dotenv import load_dotenv, find_dotenv
# Para SQL Server vía pyodbc/ODBC Driver 18
import urllib.parse
import psycopg2
import streamlit as st
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

dotenv_path = find_dotenv()
print(f"Usando archivo dotenv en: {dotenv_path}")


load_dotenv()
print (os.getenv("PG_HOST","no-pg-host"))
print (os.getenv("SQL_SERVER","no-SQL-host"))
print(os.getenv("PGP_HOST","no-pgp-host")   )
print(os.getenv("PGP_DB","no-pgp-db")   )

@lru_cache(maxsize=1)
def get_pg_engine():
    host = os.getenv("PG_HOST","186.158.182.54")
    port = os.getenv("PG_PORT","5432")
    db   = os.getenv("PG_DB","diarco_data")
    user = os.getenv("PG_USER","postgres")
    pw   = os.getenv("PG_PASSWORD","postgres")
    uri  = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(uri, pool_pre_ping=True)

@lru_cache(maxsize=1)
def get_pgp_engine():
    hostp = os.getenv("PGP_HOST","186.158.182.127")
    portp = os.getenv("PG_PPORT","5432")
    dbp   = os.getenv("PGP_DB","connexa_platform_ms")
    userp = os.getenv("PGP_USER","postgres")
    pwp  = os.getenv("PGP_PASSWORD","postgres")
    urip  = f"postgresql+psycopg2://{userp}:{pwp}@{hostp}:{portp}/{dbp}"
    print("Connecting to PGP:", urip)
    return create_engine(urip, pool_pre_ping=True)

@lru_cache(maxsize=1)
def get_conn_diarco_data():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )
    
# =========================
# Utilidades de conexión
# =========================

def _build_pg_url(host: str, port: str, db: str, user: str, pwd: str) -> str:
    return f"postgresql+psycopg2://{urllib.parse.quote_plus(user)}:{urllib.parse.quote_plus(pwd)}@{host}:{port}/{db}"

@st.cache_resource(show_spinner=False)
def get_connexa_engine() -> Engine:
    host = os.getenv("CONNEXA_PG_HOST", os.getenv("PGP_HOST"))
    port = os.getenv("CONNEXA_PG_PORT", os.getenv("PGP_PORT", "5432"))
    db   = os.getenv("CONNEXA_PG_DB",   os.getenv("PGP_DB"))
    usr  = os.getenv("CONNEXA_PG_USER", os.getenv("PGP_USER"))
    pwd  = os.getenv("CONNEXA_PG_PASSWORD", os.getenv("PGP_PASSWORD"))
    if not all([host, port, db, usr, pwd]):
        raise RuntimeError("Faltan variables de entorno para Postgres connexa_platform.")
    url = _build_pg_url(host, port, db, usr, pwd)
    return create_engine(url, pool_pre_ping=True)

@st.cache_resource(show_spinner=False)
def get_diarco_engine() -> Engine:
    host = os.getenv("DIARCO_PG_HOST", os.getenv("PG_HOST"))
    port = os.getenv("DIARCO_PG_PORT", os.getenv("PG_PORT", "5432"))
    db   = os.getenv("DIARCO_PG_DB",   os.getenv("PG_DB"))
    usr  = os.getenv("DIARCO_PG_USER", os.getenv("PG_USER"))
    pwd  = os.getenv("DIARCO_PG_PASSWORD", os.getenv("PG_PASSWORD"))
    if not all([host, port, db, usr, pwd]):
        raise RuntimeError("Faltan variables de entorno para Postgres diarco_data.")
    url = _build_pg_url(host, port, db, usr, pwd)
    return create_engine(url, pool_pre_ping=True)

@st.cache_resource(show_spinner=False)
def get_sqlserver_engine() -> Engine:
     # Opcional: habilitar solo si corresponde (Indicadores SGM)
    import urllib.parse
    host = os.getenv("SQL_SERVER")
    port = os.getenv("SQL_PORT", "1433")
    db   = os.getenv("SQL_DATABASE")
    user = os.getenv("SQL_USER")
    pw   = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER","ODBC Driver 18 for SQL Server")

    if not (host and db and user and pw):
        return None

    params = urllib.parse.quote_plus(
        f"DRIVER={driver};SERVER={host},{port};DATABASE={db};UID={user};PWD={pw};Encrypt=yes;TrustServerCertificate=yes;"
    )
    url = f"mssql+pyodbc:///?odbc_connect={params}"

    return create_engine(url, pool_pre_ping=True, fast_executemany=True)

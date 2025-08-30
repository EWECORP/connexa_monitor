
import os
from functools import lru_cache
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
print (os.getenv("PG_HOST","no-pg-host"))
print (os.getenv("SQL_SERVER","no-SQL-host"))

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
def get_sqlserver_engine():
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
    uri = f"mssql+pyodbc:///?odbc_connect={params}"
    return create_engine(uri, pool_pre_ping=True)

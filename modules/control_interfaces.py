# control_intefaces.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class InterfaceSpec:
    tabla: str
    campo_fecha: str
    schema: str = "src"


def _exists_column(engine: Engine, schema: str, table: str, column: str) -> bool:
    """Verifica existencia de columna en information_schema."""
    sql = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = :column
        LIMIT 1
    """)
    with engine.connect() as con:
        r = con.execute(sql, {"schema": schema, "table": table, "column": column}).fetchone()
        return r is not None


def _q_last_day_and_count(schema: str, table: str, col: str) -> str:
    """
    Devuelve un SQL que:
      - toma la última marca temporal (MAX(col)) como 'ultima_fecha'
      - cuenta registros del ÚLTIMO DÍA disponible (col::date = max(col::date))
    NOTA: Al aplicar '::date' sobre la COLUMNA (no parámetros) mantenemos compatibilidad y uso de índices.
    """
    return f"""
        SELECT
          MAX({col}) AS ultima_fecha,
          COUNT(*)   AS cantidad
        FROM {schema}.{table}
        WHERE ({col})::date = (SELECT MAX(({col})::date) FROM {schema}.{table})
    """


def _q_trend_last_n_days(schema: str, table: str, col: str, days: int) -> str:
    """
    Serie diaria completa (incluye días sin datos) para los últimos 'days' días
    respecto del último día con datos.
    """
    d = int(days)
    return f"""
        WITH last AS (
          SELECT MAX(({col})::date) AS dmax
          FROM {schema}.{table}
        ),
        days AS (
          SELECT generate_series(dmax - INTERVAL '{d-1} day', dmax, INTERVAL '1 day')::date AS fecha
          FROM last
        )
        SELECT
          days.fecha,
          COALESCE((
            SELECT COUNT(*) FROM {schema}.{table} t
            WHERE (t.{col})::date = days.fecha
          ), 0) AS cantidad
        FROM days
        ORDER BY days.fecha;
    """


def obtener_control_interfaces(
    engine: Engine,
    tablas_por_fecha: Dict[str, str],
    schema: str = "src",
) -> pd.DataFrame:
    """
    Ejecuta el control sobre múltiples tablas.
    Retorna DataFrame con: tabla, campo_fecha, ultima_fecha_extraccion (timestamp/date), cantidad_registros (Int64)
    Si una tabla/columna no existe o el SQL falla, deja valores nulos.
    """
    rows: List[dict] = []
    with engine.connect() as con:
        # Sin timeout para controles largos; si desean, ajusten con: con.exec_driver_sql("SET statement_timeout='0'")
        for table, col in tablas_por_fecha.items():
            ok = _exists_column(engine, schema, table, col)
            if not ok:
                rows.append({
                    "tabla": table,
                    "campo_fecha": col,
                    "ultima_fecha_extraccion": None,
                    "cantidad_registros": None,
                    "error": f"Columna {schema}.{table}.{col} inexistente"
                })
                continue
            sql_txt = text(_q_last_day_and_count(schema, table, col))
            try:
                rec = con.execute(sql_txt).mappings().first()
                rows.append({
                    "tabla": table,
                    "campo_fecha": col,
                    "ultima_fecha_extraccion": rec["ultima_fecha"],
                    "cantidad_registros": rec["cantidad"],
                    "error": None
                })
            except Exception as e:
                rows.append({
                    "tabla": table,
                    "campo_fecha": col,
                    "ultima_fecha_extraccion": None,
                    "cantidad_registros": None,
                    "error": str(e)
                })
    df = pd.DataFrame(rows, columns=["tabla", "campo_fecha", "ultima_fecha_extraccion", "cantidad_registros", "error"])
    # Tipos amigables
    if not df.empty:
        df["tabla"] = df["tabla"].astype(str)
        df["campo_fecha"] = df["campo_fecha"].astype(str)
        df["cantidad_registros"] = pd.to_numeric(df["cantidad_registros"], errors="coerce").astype("Int64")
        df["ultima_fecha_extraccion"] = pd.to_datetime(df["ultima_fecha_extraccion"], errors="coerce", utc=True).dt.tz_localize(None)
    return df


def obtener_trend_tabla(
    engine: Engine,
    tabla: str,
    campo_fecha: str,
    days: int = 14,
    schema: str = "src",
) -> pd.DataFrame:
    """Devuelve serie diaria (últimos 'days' días relativos al último día con datos)."""
    if not _exists_column(engine, schema, tabla, campo_fecha):
        return pd.DataFrame(columns=["fecha", "cantidad"])
    sql_txt = text(_q_trend_last_n_days(schema, tabla, campo_fecha, days))
    with engine.connect() as con:
        df = pd.read_sql(sql_txt, con)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").astype("Int64")
    return df


def sugerir_indices(tablas_por_fecha: Dict[str, str], schema: str = "src") -> List[str]:
    """
    Devuelve sentencias CREATE INDEX sugeridas sobre cada (schema.tabla, campo_fecha).
    Útil para pegarlas en la base (idempotentes si usan IF NOT EXISTS en PG >= 9.5).
    """
    stmts = []
    for table, col in tablas_por_fecha.items():
        idx = f"idx_{table}_{col}".replace(".", "_").lower()
        stmts.append(f"CREATE INDEX IF NOT EXISTS {idx} ON {schema}.{table} ({col});")
    return stmts

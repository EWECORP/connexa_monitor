# modules/queries/compradores.py

from datetime import date
from typing import Optional
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

# ============================================================
# Helpers
# ============================================================

# def _to_df(result) -> pd.DataFrame:
#     """
#     Convierte un resultado de SQLAlchemy en DataFrame.
#     Retorna DataFrame vacío si no hay filas o si no se pueden leer.
#     """
#     try:
#         rows = result.fetchall()
#     except Exception:
#         return pd.DataFrame()

#     if not rows:
#         return pd.DataFrame()

#     return pd.DataFrame(rows, columns=result.keys())


def _to_df(result) -> pd.DataFrame:
    return (
        pd.DataFrame(result.fetchall(), columns=result.keys())
        if result.returns_rows
        else pd.DataFrame()
    )

def _safe_sum(df: pd.DataFrame, col: str) -> float:
    """
    Suma segura: si la columna no existe, devuelve 0.
    """
    if df is None or df.empty or col not in df.columns:
        return 0.0
    serie = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return float(serie.sum())

# ============================================================
# SQL — Rankings de compradores (OC Connexa)
# ============================================================

# Ranking por código de comprador (uso interno / detallado)
SQL_RANKING_COMPRADORES = text("""
SELECT
  c_comprador,
  SUM(total_oc)::bigint    AS oc_total,
  SUM(total_bultos)::numeric AS bultos_total
FROM mon.v_oc_generadas_mensual
WHERE mes >= :desde AND mes <= :hasta
GROUP BY c_comprador
ORDER BY oc_total DESC NULLS LAST
LIMIT :topn;
""")

# Ranking con nombre de comprador (vista extendida)
SQL_RANKING_COMPRADORES_NOMBRE = text("""
SELECT
  COALESCE(NULLIF(TRIM(n_comprador), ''), CAST(c_comprador AS TEXT)) AS comprador,
  SUM(total_oc)::bigint    AS oc_total,
  SUM(total_bultos)::numeric AS bultos_total
FROM mon.v_oc_generadas_mensual_ext
WHERE mes >= :desde AND mes <= :hasta
GROUP BY COALESCE(NULLIF(TRIM(n_comprador), ''), CAST(c_comprador AS TEXT))
ORDER BY oc_total DESC NULLS LAST
LIMIT :topn;
""")


# ============================================================
# SQL — Productividad de compradores (Forecast → Propuesta)
# ============================================================

# Productividad mensual por comprador (Connexa)
SQL_FP_MENSUAL_COMPRADOR = text("""
WITH b AS (
  SELECT *
  FROM mon.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT
  date_trunc('month', (base_ts AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
  COALESCE(NULLIF(trim(user_name), ''), buyer_id::text, '- sin comprador -')         AS comprador,
  COUNT(DISTINCT pp_id)                                                              AS propuestas,
  SUM(pp_total_amount)                                                               AS monto_total,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY adjust_time_min)                       AS p50_ajuste_min,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY adjust_time_min)                       AS p90_ajuste_min,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY lead_open_min)                         AS p50_lead_min,
  AVG(exec_time_min)                                                                 AS avg_exec_min
FROM b
GROUP BY 1, 2
ORDER BY mes, comprador;
""")

# Ranking global de compradores (Forecast → Propuesta)
SQL_FP_RANKING_COMPRADOR = text("""
WITH b AS (
  SELECT *
  FROM mon.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT
  COALESCE(NULLIF(trim(user_name), ''), buyer_id::text, '- sin modificacion -') AS comprador,
  COUNT(DISTINCT pp_id)                                                         AS propuestas,
  SUM(pp_total_amount)                                                          AS monto_total,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY adjust_time_min)                  AS p50_ajuste_min,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY adjust_time_min)                  AS p90_ajuste_min
FROM b
GROUP BY 1
ORDER BY propuestas DESC, monto_total DESC
LIMIT :topn;
""")

# ---------------------------------------------------------
# Ranking “bruto” por comprador (Connexa → diarco_data)
# ---------------------------------------------------------
SQL_RANKING_COMPRADORES_RESUMEN = text("""
SELECT
    c_comprador,
    COUNT(DISTINCT c_compra_connexa)::bigint         AS oc_total_connexa,
    SUM(COALESCE(q_bultos_kilos_diarco,0))::numeric  AS bultos_total_connexa
FROM public.t080_oc_precarga_connexa
WHERE f_alta_sist >= :desde
  AND f_alta_sist <  (:hasta + INTERVAL '1 day')
GROUP BY c_comprador
ORDER BY oc_total_connexa DESC
LIMIT :topn;
""")



# ============================================================
# Funciones públicas — Rankings OC Connexa
# ============================================================

def get_ranking_compradores_pg(
    pg_engine: Engine,
    desde: date,
    hasta: date,
    topn: int = 10
) -> pd.DataFrame:
    """
    Ranking de compradores por OC y bultos desde Connexa
    (basado en mon.v_oc_generadas_mensual, por código de comprador).
    """
    if pg_engine is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta, "topn": topn}
    with pg_engine.connect() as con:
        res = con.execute(SQL_RANKING_COMPRADORES, params)
        df = _to_df(res)

    if df.empty:
        return df

    # Normalización básica de tipos
    if "c_comprador" in df.columns:
        df["c_comprador"] = pd.to_numeric(df["c_comprador"], errors="coerce")
    for col in ("oc_total", "bultos_total"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_ranking_compradores_nombre_pg(
    pg_engine: Engine,
    desde: date,
    hasta: date,
    topn: int = 10
) -> pd.DataFrame:
    """
    Ranking de compradores con nombre (mon.v_oc_generadas_mensual_ext).
    Columnas típicas:
      - comprador (str)
      - oc_total (int)
      - bultos_total (numeric)
    """
    if pg_engine is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta, "topn": topn}
    with pg_engine.connect() as con:
        res = con.execute(SQL_RANKING_COMPRADORES_NOMBRE, params)
        df = _to_df(res)

    if df.empty:
        return df

    if "comprador" in df.columns:
        df["comprador"] = df["comprador"].astype(str)

    for col in ("oc_total", "bultos_total"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_ranking_compradores_resumen(
    pg_engine: Engine,
    desde: date,
    hasta: date,
    topn: int = 50,
) -> pd.DataFrame:
    """
    Ranking de compradores por OC y bultos desde Connexa (directo desde t080_oc_precarga_connexa).

    Columnas devueltas:
      - c_comprador (Int64)
      - oc_total_connexa (Int64)
      - bultos_connexa (numeric)
    """
    params = {"desde": desde, "hasta": hasta, "topn": topn}

    with pg_engine.connect() as con:
        res = con.execute(SQL_RANKING_COMPRADORES_RESUMEN, params)
        df = _to_df(res)

    if df.empty:
        return df

    # Normalización de tipos
    df["c_comprador"] = pd.to_numeric(df["c_comprador"], errors="coerce").astype("Int64")
    df["oc_total_connexa"] = pd.to_numeric(df["oc_total_connexa"], errors="coerce").fillna(0).astype("Int64")

    # Renombramos bultos_total_connexa → bultos_connexa para que concuerde con el resto del código
    if "bultos_total_connexa" in df.columns:
        df.rename(columns={"bultos_total_connexa": "bultos_connexa"}, inplace=True)
    else:
        # fallback por si el alias cambia en la query
        if "bultos_connexa" not in df.columns:
            df["bultos_connexa"] = 0.0

    df["bultos_connexa"] = pd.to_numeric(df["bultos_connexa"], errors="coerce").fillna(0.0)

    return df


# ============================================================
# Funciones públicas — Productividad Forecast → Propuesta
# ============================================================

def get_productividad_comprador_mensual(
    pg_engine_connexa: Engine,
    desde: date,
    hasta: date
) -> pd.DataFrame:
    """
    Productividad por comprador (mensual):
      - propuestas
      - monto_total
      - P50/P90 tiempos de ajuste y lead
      - tiempo medio de ejecución
    Basado en mon.v_forecast_propuesta_base.
    """
    if pg_engine_connexa is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta}
    with pg_engine_connexa.connect() as con:
        res = con.execute(SQL_FP_MENSUAL_COMPRADOR, params)
        df = _to_df(res)

    if df.empty:
        return df

    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"])

    # Conversión numérica básica de métricas
    numeric_cols = [
        "propuestas",
        "monto_total",
        "p50_ajuste_min",
        "p90_ajuste_min",
        "p50_lead_min",
        "avg_exec_min",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_ranking_comprador_forecast(
    pg_engine_connexa: Engine,
    desde: date,
    hasta: date,
    topn: int = 10
) -> pd.DataFrame:
    """
    Ranking de compradores en función de:
      - #propuestas
      - monto_total
      - tiempos de ajuste (P50 / P90)
    Basado en mon.v_forecast_propuesta_base.
    """
    if pg_engine_connexa is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta, "topn": topn}
    with pg_engine_connexa.connect() as con:
        res = con.execute(SQL_FP_RANKING_COMPRADOR, params)
        df = _to_df(res)

    if df.empty:
        return df

    numeric_cols = [
        "propuestas",
        "monto_total",
        "p50_ajuste_min",
        "p90_ajuste_min",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "comprador" in df.columns:
        df["comprador"] = df["comprador"].astype(str)

    return df


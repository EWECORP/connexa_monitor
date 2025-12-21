# modules/queries/uso_general.py

from typing import Any, Dict
from datetime import date

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

# ============================================================
# Helpers
# ============================================================

def _to_df(result) -> pd.DataFrame:
    """
    Convierte un resultado de SQLAlchemy en DataFrame.
    Retorna DataFrame vacío si no hay filas.
    """
    try:
        rows = result.fetchall()
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=result.keys())


# ============================================================
# DDL / Vistas mon.* — OC generadas (Connexa → diarco_data)
# ============================================================

# En diarco_data:  La tabla t080_oc_precarga_connexa está poblada desde CONNEXA y no se eliminan registros. (Cambi Estado m_pulicado : true)

DDL_VIEW_OC_GENERADAS_BASE = """
CREATE SCHEMA IF NOT EXISTS mon;

CREATE OR REPLACE VIEW mon.v_oc_generadas_mensual
 AS
 WITH base AS (
         SELECT date_trunc('month'::text, (t080_oc_precarga_connexa.f_alta_sist AT TIME ZONE 'America/Argentina/Buenos_Aires'::text))::date AS mes,
            t080_oc_precarga_connexa.c_comprador,
            t080_oc_precarga_connexa.c_proveedor,
            t080_oc_precarga_connexa.c_compra_connexa,
            COALESCE(t080_oc_precarga_connexa.q_bultos_kilos_diarco, 0::numeric) AS q_bultos
           FROM t080_oc_precarga_connexa
          WHERE t080_oc_precarga_connexa.f_alta_sist IS NOT NULL
        )
 SELECT base.mes,
    base.c_comprador,
    base.c_proveedor,
    count(DISTINCT base.c_compra_connexa) AS total_oc,
    sum(base.q_bultos) AS total_bultos
   FROM base
  GROUP BY base.mes, base.c_comprador, base.c_proveedor
  ORDER BY base.mes, base.c_comprador, base.c_proveedor;
  
ALTER TABLE mon.v_oc_generadas_mensual
    OWNER TO postgres;
"""

DLL_VIEW_OC_GENERADAS_EXT = """
CREATE OR REPLACE VIEW mon.v_oc_generadas_mensual_ext
AS
SELECT v.mes,
  v.c_comprador,
  c.n_comprador,
  v.c_proveedor,
  v.total_oc,
  v.total_bultos
  FROM mon.v_oc_generadas_mensual v
    LEFT JOIN src.m_9_compradores c ON v.c_comprador = c.cod_comprador::numeric
    LEFT JOIN src.m_10_proveedores p ON v.c_proveedor = p.c_proveedor::bigint::numeric;
"""

DDL_VIEW_OC_GENERADAS_SUC_EXT = """
CREATE OR REPLACE VIEW mon.v_oc_generadas_mensual_sucursal_ext
AS
WITH base AS (
        SELECT date_trunc('month'::text, (t080_oc_precarga_kikker.f_alta_sist AT TIME ZONE 'America/Argentina/Buenos_Aires'::text))::date AS mes,
          t080_oc_precarga_kikker.c_comprador,
          t080_oc_precarga_kikker.c_proveedor,
          t080_oc_precarga_kikker.c_sucu_empr::character varying(10) AS id_tienda,
          t080_oc_precarga_kikker.c_compra_kikker,
          COALESCE(t080_oc_precarga_kikker.q_bultos_kilos_diarco, 0::numeric) AS q_bultos
          FROM t080_oc_precarga_kikker
        WHERE t080_oc_precarga_kikker.f_alta_sist IS NOT NULL
      )
SELECT b.mes,
  b.c_comprador,
  c.n_comprador,
  b.c_proveedor,
  TRIM(BOTH FROM p.n_proveedor) AS n_proveedor,
  b.id_tienda,
  s.suc_nombre,
  count(DISTINCT b.c_compra_kikker) AS total_oc,
  sum(b.q_bultos) AS total_bultos
  FROM base b
    LEFT JOIN src.m_9_compradores c ON b.c_comprador = COALESCE(NULLIF(c.cod_comprador::text, ''::text)::numeric, 0::numeric)
    LEFT JOIN src.m_10_proveedores p ON b.c_proveedor = p.c_proveedor::bigint::numeric
    LEFT JOIN src.m_91_sucursales s ON b.id_tienda::text = s.id_tienda
GROUP BY b.mes, b.c_comprador, c.n_comprador, b.c_proveedor, (TRIM(BOTH FROM p.n_proveedor)), b.id_tienda, s.suc_nombre
ORDER BY b.mes, b.id_tienda, b.c_comprador, b.c_proveedor;
"""

IDX_OC_GENERADAS = [
    "CREATE INDEX IF NOT EXISTS idx_t080_alta_sist     ON public.t080_oc_precarga_kikker (f_alta_sist)",
    "CREATE INDEX IF NOT EXISTS idx_t080_comprador     ON public.t080_oc_precarga_kikker (c_comprador)",
    "CREATE INDEX IF NOT EXISTS idx_t080_proveedor     ON public.t080_oc_precarga_kikker (c_proveedor)",
    "CREATE INDEX IF NOT EXISTS idx_t080_compra_kikker ON public.t080_oc_precarga_kikker (c_compra_kikker)"
]

IDX_DIM = [
    "CREATE INDEX IF NOT EXISTS idx_m9_cod_comprador ON src.m_9_compradores (cod_comprador)",
    "CREATE INDEX IF NOT EXISTS idx_m10_c_proveedor  ON src.m_10_proveedores (c_proveedor)",
    "CREATE INDEX IF NOT EXISTS idx_m91_id_tienda    ON src.m_91_sucursales (id_tienda)"
]


# ============================================================
# DDL / Vista mon.v_forecast_propuesta_base (Connexa)
# ============================================================
# Vista Generada en MODELO MICROSERVICIOS

DDL_V_FORECAST_PROPUESTA_BASE = """
CREATE SCHEMA IF NOT EXISTS mon;

CREATE OR REPLACE VIEW mon.v_forecast_propuesta_base
 AS
 SELECT fe.id AS fe_id,
    fe.start_execution AS fe_start,
    fe.end_execution AS fe_end,
    fe.last_execution AS fe_last,
    fe."timestamp" AS fe_ts,
    fe.supply_forecast_execution_id AS fe_exec_id,
    fe.supply_forecast_execution_schedule_id AS fe_sched_id,
    fe.ext_supplier_code,
    fe.monthly_net_margin_in_millions AS fe_net_margin_m,
    fe.monthly_purchases_in_millions AS fe_purchases_m,
    fe.monthly_sales_in_millions AS fe_sales_m,
    fe.sotck_days AS stock_days,
    fe.sotck_days_colors AS stock_days_colors,
    fe.supplier_id AS fe_supplier_id,
    fe.supply_forecast_execution_status_id AS fe_status_id,
    sfes.name AS fe_status_name,
    sfes.description AS fe_status_desc,
    fe.contains_breaks,
    fe.maximum_backorder_days AS max_backorder_days,
    fe.otif,
    fe.total_products AS fe_total_products,
    fe.total_units AS fe_total_units,
    fe.supply_purchase_proposal_id AS pp_id,
    pp.closed_at AS pp_closed_at,
    pp.comments AS pp_comments,
    pp.open_at AS pp_open_at,
    pp.proposal_number AS pp_number,
    pp.status AS pp_status,
    pp."timestamp" AS pp_ts,
    pp.total_amount AS pp_total_amount,
    pp.total_products AS pp_total_products,
    pp.total_sites AS pp_total_sites,
    pp.total_units AS pp_total_units,
    pp.buyer_id,
    pp.supplier_id AS pp_supplier_id,
    pp.user_id,
    pp.user_name,
    COALESCE(pp.open_at, fe.end_execution, fe.start_execution, fe."timestamp") AS base_ts,
    EXTRACT(epoch FROM fe.end_execution - fe.start_execution) / 60.0 AS exec_time_min,
    EXTRACT(epoch FROM pp.open_at - fe.end_execution) / 60.0 AS lead_open_min,
    EXTRACT(epoch FROM COALESCE(pp.closed_at, now()) - COALESCE(pp.open_at, COALESCE(pp."timestamp", fe.end_execution))) / 60.0 AS adjust_time_min
   FROM supply_planning.spl_supply_forecast_execution_execute fe
     LEFT JOIN supply_planning.spl_supply_purchase_proposal pp ON pp.id = fe.supply_purchase_proposal_id
     LEFT JOIN supply_planning.spl_supply_forecast_execution_status sfes ON sfes.id = fe.supply_forecast_execution_status_id;
"""


# ============================================================
# SQL básicos para Indicadores de uso general
# ============================================================

# Serie mensual de OC generadas (vista mon.v_oc_generadas_mensual)
SQL_OC_GENERADAS_RANGO = text("""
SELECT *
FROM mon.v_oc_generadas_mensual
WHERE mes >= :desde AND mes <= :hasta
ORDER BY mes, c_comprador, c_proveedor;
""")

# Forecast → Propuesta: serie mensual
SQL_FP_CONVERSION_MENSUAL = text("""
WITH b AS (
  SELECT *
  FROM mon.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
)
, fe_m AS (
  SELECT date_trunc('month', (COALESCE(fe_end, fe_start, fe_ts) AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
         COUNT(DISTINCT fe_id) AS ejecuciones
  FROM b
  WHERE fe_end IS NOT NULL
  GROUP BY 1
)
, pp_m AS (
  SELECT date_trunc('month', (COALESCE(pp_open_at, base_ts) AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
         COUNT(DISTINCT pp_id) AS propuestas
  FROM b
  WHERE pp_id IS NOT NULL
  GROUP BY 1
)
SELECT
  COALESCE(fe_m.mes, pp_m.mes) AS mes,
  COALESCE(fe_m.ejecuciones, 0) AS ejecuciones,
  COALESCE(pp_m.propuestas, 0)  AS propuestas,
  CASE WHEN COALESCE(fe_m.ejecuciones,0) = 0 THEN NULL
       ELSE 1.0 * COALESCE(pp_m.propuestas,0) / fe_m.ejecuciones
  END AS conversion
FROM fe_m
FULL OUTER JOIN pp_m USING (mes)
ORDER BY mes;
""")

# Embudo CONNEXA → SGM (PostgreSQL + SQL Server)
SQL_PG_KIKKER_MENSUAL = text("""
SELECT
  date_trunc('month', (f_alta_sist AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
  COUNT(DISTINCT c_compra_connexa)         AS kikker_distintos_pg,
  SUM(COALESCE(q_bultos_kilos_diarco,0))  AS total_bultos_pg
FROM public.t080_oc_precarga_connexa
WHERE f_alta_sist >= :desde 
  AND f_alta_sist  < (:hasta  + INTERVAL '1 day')
GROUP BY 1
ORDER BY 1;
""")

SQL_SGM_KIKKER_VS_OC_MENSUAL = text("""
WITH src AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST) AS f_alta_date,
    C_COMPRA_KIKKER,
    COALESCE(Q_BULTOS_KILOS_DIARCO, 0) AS q_bultos,
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-06-01' AS DATE) -- evitar conversión masiva
),
base AS (
  SELECT * FROM src
  WHERE f_alta_date >= :desde AND f_alta_date < DATEADD(day, 1, :hasta)
)
SELECT
  DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1) AS mes,
  COUNT(DISTINCT C_COMPRA_KIKKER) AS kikker_distintos,
  COUNT(DISTINCT oc_sgm)         AS oc_sgm_distintas,
  SUM(q_bultos)                  AS total_bultos
FROM base
GROUP BY DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1)
ORDER BY mes;
""")

# Proporción CI vs Total SGM (Indicador 3)
SQL_SGM_I3_MENSUAL = text("""
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                    AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                 AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                 AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                    AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                 AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                 AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm,
    C_OC
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE)
      AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
rango AS (
  SELECT :desde AS d, :hasta AS h
),
cabe_r AS (
  SELECT c.*
  FROM cabe c CROSS JOIN rango r
  WHERE c.f_alta_date >= r.d AND c.f_alta_date < DATEADD(day, 1, r.h)
),
t874_r AS (
  SELECT t.*
  FROM t874 t CROSS JOIN rango r
  WHERE t.f_alta_date >= r.d AND t.f_alta_date < DATEADD(day, 1, r.h)
)
SELECT
  DATEFROMPARTS(YEAR(c.f_alta_date), MONTH(c.f_alta_date), 1)           AS mes,
  COUNT(DISTINCT c.oc_sgm)                                              AS oc_totales_sgm,
  COUNT(DISTINCT CASE WHEN t.oc_sgm IS NOT NULL THEN c.oc_sgm END)      AS oc_desde_connexa,
  CAST(
    1.0 * COUNT(DISTINCT CASE WHEN t.oc_sgm IS NOT NULL THEN c.oc_sgm END) / NULLIF(COUNT(DISTINCT c.oc_sgm), 0)
    AS decimal(9,6)
  ) AS proporcion_ci
FROM cabe_r c
LEFT JOIN t874_r t
  ON t.u_prefijo_oc = c.u_prefijo_oc AND t.u_sufijo_oc = c.u_sufijo_oc
GROUP BY DATEFROMPARTS(YEAR(c.f_alta_date), MONTH(c.f_alta_date), 1)
ORDER BY mes;
""")


# ============================================================
# Funciones públicas
# ============================================================

def ensure_mon_objects(pg_engine: Engine) -> None:
    """
    Crea/actualiza vistas e índices mon.* asociados a OC generadas.
    Debe ejecutarse sobre diarco_data.
    """
    if pg_engine is None:
        return

    with pg_engine.begin() as con:
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_BASE)
        con.exec_driver_sql(DLL_VIEW_OC_GENERADAS_EXT)
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_SUC_EXT)
        for stmt in IDX_OC_GENERADAS + IDX_DIM:
            con.exec_driver_sql(stmt)


def ensure_forecast_views(pg_engine_connexa: Engine) -> None:
    """
    Crea/actualiza la vista mon.v_forecast_propuesta_base
    sobre el esquema supply_planning (Connexa).
    """
    if pg_engine_connexa is None:
        return

    with pg_engine_connexa.begin() as con:
        con.exec_driver_sql(DDL_V_FORECAST_PROPUESTA_BASE)


def get_oc_generadas_mensual(
    pg_engine: Engine,
    desde: date,
    hasta: date
) -> pd.DataFrame:
    """
    Devuelve la serie mensual de OC generadas desde Connexa,
    basada en mon.v_oc_generadas_mensual.
    """
    if pg_engine is None:
        return pd.DataFrame()

    with pg_engine.connect() as con:
        res = con.execute(SQL_OC_GENERADAS_RANGO, {"desde": desde, "hasta": hasta})
        return _to_df(res)


def get_forecast_propuesta_conversion_mensual(
    pg_engine_connexa: Engine,
    desde: date,
    hasta: date
) -> pd.DataFrame:
    """
    Devuelve serie mensual de:
      - ejecuciones de forecast completadas
      - propuestas generadas
      - tasa de conversión propuestas / ejecuciones
    a partir de mon.v_forecast_propuesta_base.
    """
    if pg_engine_connexa is None:
        return pd.DataFrame()

    with pg_engine_connexa.connect() as con:
        res = con.execute(SQL_FP_CONVERSION_MENSUAL, {"desde": desde, "hasta": hasta})
        df = _to_df(res)

    return df


def get_embudo_connexa_sgm_mensual(
    pg_engine_diarco: Engine,
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Devuelve el embudo mensual CONNEXA → SGM con columnas:
      - mes (datetime)
      - pedidos_connexa      (NIPs / compras Connexa)
      - oc_sgm               (OC SGM distintas)
      - bultos_connexa
      - bultos_sgm
    """
    if pg_engine_diarco is None or sqlserver_engine is None:
        return pd.DataFrame()

    # Pedidos CONNEXA (PostgreSQL)
    with pg_engine_diarco.connect() as con_pg:
        df_pg = pd.read_sql(
            SQL_PG_KIKKER_MENSUAL,
            con_pg,
            params={"desde": desde, "hasta": hasta},
        )

    if not df_pg.empty:
        df_pg["mes"] = pd.to_datetime(df_pg["mes"])
        df_pg.rename(
            columns={
                "kikker_distintos_pg": "pedidos_connexa",
                "total_bultos_pg": "bultos_connexa",
            },
            inplace=True,
        )
    else:
        df_pg = pd.DataFrame(columns=["mes", "pedidos_connexa", "bultos_connexa"])

    # OC SGM (SQL Server)
    with sqlserver_engine.connect() as con_ss:
        df_sgm = pd.read_sql(
            SQL_SGM_KIKKER_VS_OC_MENSUAL,
            con_ss,
            params={"desde": desde, "hasta": hasta},
        )

    if not df_sgm.empty:
        df_sgm["mes"] = pd.to_datetime(df_sgm["mes"])
        df_sgm.rename(
            columns={
                "kikker_distintos": "nips_sgm",
                "oc_sgm_distintas": "oc_sgm",
                "total_bultos": "bultos_sgm",
            },
            inplace=True,
        )
    else:
        df_sgm = pd.DataFrame(columns=["mes", "nips_sgm", "oc_sgm", "bultos_sgm"])

    # Merge y normalización
    df = pd.merge(df_pg, df_sgm, on="mes", how="outer").sort_values("mes")

    for c in ("pedidos_connexa", "oc_sgm", "bultos_connexa", "bultos_sgm"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # Tasa de conversión pedidos → OC (en %)
    if {"pedidos_connexa", "oc_sgm"}.issubset(df.columns):
        df["tasa_conv_pedidos_oc"] = df.apply(
            lambda r: (r["oc_sgm"] / r["pedidos_connexa"] * 100.0)
            if r.get("pedidos_connexa", 0) > 0
            else 0.0,
            axis=1,
        )
    else:
        df["tasa_conv_pedidos_oc"] = 0.0

    return df


def get_proporcion_ci_vs_sgm_mensual(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Devuelve la proporción mensual de OC SGM originadas en CONNEXA.
    Columnas esperadas:
      - mes
      - oc_totales_sgm
      - oc_desde_connexa
      - proporcion_ci (0..1)
    """
    if sqlserver_engine is None:
        return pd.DataFrame()

    with sqlserver_engine.connect() as con_ss:
        df = pd.read_sql(
            SQL_SGM_I3_MENSUAL,
            con_ss,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    df["mes"] = pd.to_datetime(df["mes"])

    for c in ("oc_totales_sgm", "oc_desde_connexa"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    if "proporcion_ci" in df.columns:
        df["proporcion_ci"] = pd.to_numeric(
            df["proporcion_ci"], errors="coerce"
        ).fillna(0.0)
    else:
        if {"oc_totales_sgm", "oc_desde_connexa"}.issubset(df.columns):
            df["proporcion_ci"] = df.apply(
                lambda r: (r["oc_desde_connexa"] / r["oc_totales_sgm"])
                if r.get("oc_totales_sgm", 0) > 0
                else 0.0,
                axis=1,
            )
        else:
            df["proporcion_ci"] = 0.0

    return df

# ============================================================
# NUEVAS QUERIES — Estados y detalle de propuestas
# ============================================================

SQL_FP_ESTADOS_PROP = text("""
WITH b AS (
  SELECT *
  FROM mon.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT pp_status, COUNT(*) AS propuestas
FROM b
GROUP BY pp_status
ORDER BY propuestas DESC NULLS LAST;
""")

SQL_FP_DETALLE = text("""
SELECT
  base_ts,
  fe_id, fe_start, fe_end, fe_status_name,
  pp_id, pp_number, pp_status, pp_open_at, pp_closed_at,
  buyer_id, user_name,
  pp_total_amount, pp_total_units, pp_total_products,
  exec_time_min, lead_open_min, adjust_time_min,
  ext_supplier_code, fe_supplier_id
FROM mon.v_forecast_propuesta_base
WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
ORDER BY base_ts DESC, pp_id NULLS LAST;
""")


def get_forecast_propuesta_estados(
    pg_engine_connexa: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Estados de propuestas en el rango:
      - pp_status
      - propuestas
    """
    if pg_engine_connexa is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta}
    with pg_engine_connexa.connect() as con:
        res = con.execute(SQL_FP_ESTADOS_PROP, params)
        df = _to_df(res)  # se asume que _to_df ya está definido en este módulo

    if df.empty:
        return df

    df["propuestas"] = pd.to_numeric(df["propuestas"], errors="coerce").fillna(0).astype("Int64")

    if "pp_status" in df.columns:
        df["pp_status"] = df["pp_status"].astype(str)

    return df


def get_forecast_propuesta_detalle(
    pg_engine_connexa: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Detalle de pipeline de propuestas en el rango:
      - base_ts, fe_id, fe_status_name
      - pp_id, pp_number, pp_status, pp_open_at, pp_closed_at
      - buyer_id, user_name
      - pp_total_amount, pp_total_units, pp_total_products
      - exec_time_min, lead_open_min, adjust_time_min
    """
    if pg_engine_connexa is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta}
    with pg_engine_connexa.connect() as con:
        df = pd.read_sql(SQL_FP_DETALLE, con, params=params)

    if df.empty:
        return df

    # Normalización de fechas
    for col in ("base_ts", "fe_start", "fe_end", "pp_open_at", "pp_closed_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Normalización numérica
    for col in (
        "pp_total_amount",
        "pp_total_units",
        "pp_total_products",
        "exec_time_min",
        "lead_open_min",
        "adjust_time_min",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Campos de identificación
    if "buyer_id" in df.columns:
        df["buyer_id"] = pd.to_numeric(df["buyer_id"], errors="coerce")

    if "user_name" in df.columns:
        df["user_name"] = df["user_name"].astype(str)

    if "pp_status" in df.columns:
        df["pp_status"] = df["pp_status"].astype(str)

    return df

# ============================================================
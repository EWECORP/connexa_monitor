# modules/queries/proveedores.py

from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text


# ============================================================
# Helpers
# ============================================================

def _to_df(result) -> pd.DataFrame:
    """
    Convierte un resultado de SQLAlchemy en DataFrame.
    Retorna DataFrame vacío si no hay filas o si no se pueden leer.
    """
    try:
        rows = result.fetchall()
    except Exception:
        return pd.DataFrame()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=result.keys())


# ============================================================
# SQL — Rankings de proveedores (OC Connexa en diarco_data)
# ============================================================

# Ranking de proveedores a partir de mon.v_oc_generadas_mensual (lado diarco_data).
# Se enriquece con nombre de proveedor desde src.m_10_proveedores.
SQL_RANKING_PROVEEDORES_PG = text("""
SELECT
  COALESCE(NULLIF(TRIM(p.n_proveedor), ''), CAST(v.c_proveedor AS TEXT)) AS proveedor,
  SUM(v.total_oc)::bigint      AS oc_total,
  SUM(v.total_bultos)::numeric AS bultos_total
FROM mon.v_oc_generadas_mensual v
LEFT JOIN src.m_10_proveedores p
       ON v.c_proveedor = p.c_proveedor::numeric
WHERE v.mes >= :desde AND v.mes <= :hasta
GROUP BY COALESCE(NULLIF(TRIM(p.n_proveedor), ''), CAST(v.c_proveedor AS TEXT))
ORDER BY oc_total DESC NULLS LAST
LIMIT :topn;
""")


# ============================================================
# SQL — Proveedores CI vs SGM (Indicador 4)
# ============================================================

# Proporción mensual de proveedores gestionados vía CI sobre el total SGM
SQL_SGM_I4_PROV_MENSUAL = text("""
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                           AS c_proveedor
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-11-01' AS DATE)
        AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                           AS c_proveedor
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-11-01' AS DATE)
      AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
rango AS (SELECT :desde AS d, :hasta AS h),
t874_r AS (
  SELECT t.* FROM t874 t CROSS JOIN rango r
  WHERE t.f_alta_date >= r.d AND t.f_alta_date < DATEADD(day, 1, r.h)
),
cabe_r AS (
  SELECT c.* FROM cabe c CROSS JOIN rango r
  WHERE c.f_alta_date >= r.d AND c.f_alta_date < DATEADD(day, 1, r.h)
),
sgm_tot AS (
  SELECT
    DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1) AS mes,
    COUNT(DISTINCT c_proveedor) AS prov_totales_sgm
  FROM cabe_r
  GROUP BY DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1)
),
ci_sgm AS (
  SELECT
    DATEFROMPARTS(YEAR(c.f_alta_date), MONTH(c.f_alta_date), 1) AS mes,
    COUNT(DISTINCT c.c_proveedor)                              AS prov_desde_ci
  FROM cabe_r c
  JOIN t874_r t
    ON t.u_prefijo_oc = c.u_prefijo_oc AND t.u_sufijo_oc = c.u_sufijo_oc
  GROUP BY DATEFROMPARTS(YEAR(c.f_alta_date), MONTH(c.f_alta_date), 1)
)
SELECT
  tot.mes,
  tot.prov_totales_sgm,
  COALESCE(ci.prov_desde_ci, 0) AS prov_desde_ci,
  CAST(1.0 * COALESCE(ci.prov_desde_ci, 0) / NULLIF(tot.prov_totales_sgm, 0) AS decimal(9,6)) AS proporcion_ci_prov
FROM sgm_tot tot
LEFT JOIN ci_sgm ci ON ci.mes = tot.mes
ORDER BY tot.mes;
""")

# Detalle de proveedores CI → SGM (para ranking / drill-down)
SQL_SGM_I4_PROV_DETALLE = text("""
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, T.F_ALTA_SIST)                                        AS f_alta_date_ci,
    CAST(T.U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(T.U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    T.C_PROVEEDOR                                                           AS c_proveedor_ci,
	P.N_PROVEEDOR														  AS n_proveedor,
    COALESCE(Q_BULTOS_KILOS_DIARCO,0)                                     AS q_bultos_ci
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST] T
  LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T020_PROVEEDOR] P
	ON T.C_PROVEEDOR = P.C_PROVEEDOR
  WHERE T.F_ALTA_SIST >= CAST('2025-11-01' AS DATE)
        AND ISNULL(T.U_PREFIJO_OC, 0) <> 0 AND ISNULL(T.U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date_sgm,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                           AS c_proveedor_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-11-01' AS DATE)
        AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
rango AS (SELECT :desde AS d, :hasta AS h),
t874_r AS (
  SELECT * FROM t874 CROSS JOIN rango r
  WHERE f_alta_date_ci >= r.d AND f_alta_date_ci < DATEADD(day, 1, r.h)
),
cabe_r AS (
  SELECT * FROM cabe CROSS JOIN rango r
  WHERE f_alta_date_sgm >= r.d AND f_alta_date_sgm < DATEADD(day, 1, r.h)
)
SELECT
  c.f_alta_date_sgm AS f_alta_sgm,
  c.c_proveedor_sgm  AS c_proveedor,
  t.n_proveedor AS n_proveedor,
  CONCAT(c.u_prefijo_oc, '-', c.u_sufijo_oc) AS oc_sgm,
  t.q_bultos_ci
FROM cabe_r c
JOIN t874_r t
  ON t.u_prefijo_oc = c.u_prefijo_oc AND t.u_sufijo_oc = c.u_sufijo_oc
ORDER BY c.f_alta_date_sgm DESC, c.c_proveedor_sgm;
""")

# Proveedores en T874 sin cabecera en T080 (anomalías / controles)
SQL_SGM_I4_PROV_SIN_CABE = text("""
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                           AS c_proveedor
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-11-01' AS DATE)
      AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-11-01' AS DATE)
        AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
rango AS (SELECT :desde AS d, :hasta AS h),
t874_r AS (
  SELECT * FROM t874 CROSS JOIN rango r
  WHERE f_alta_date >= r.d AND f_alta_date < DATEADD(day, 1, r.h)
),
cabe_r AS (
  SELECT * FROM cabe CROSS JOIN rango r
  WHERE f_alta_date >= r.d AND f_alta_date < DATEADD(day, 1, r.h)
)
SELECT DISTINCT t.c_proveedor, t.u_prefijo_oc, t.u_sufijo_oc, t.f_alta_date
FROM t874_r t
LEFT JOIN cabe_r c
  ON c.u_prefijo_oc = t.u_prefijo_oc AND c.u_sufijo_oc = t.u_sufijo_oc
WHERE c.u_prefijo_oc IS NULL
ORDER BY t.f_alta_date DESC, t.c_proveedor;
""")


# ============================================================
# SQL — Resumen Connexa vs SGM por proveedor
# ============================================================

SQL_PG_CONNEXA_PROV = text("""
SELECT
    O.c_proveedor,
    P.n_proveedor,
    COUNT(DISTINCT O.c_compra_connexa) AS pedidos_connexa, -- cuenta únicos
    SUM(COALESCE(O.q_bultos_kilos_diarco, 0)) AS bultos_connexa -- suma segura
FROM public.t080_oc_precarga_connexa O
LEFT JOIN src.t020_proveedor P
       ON O.c_proveedor = P.c_proveedor
WHERE f_alta_sist >= :desde 
  AND f_alta_sist <  (:hasta)
GROUP BY O.c_proveedor, P.n_proveedor;
""")

SQL_SGM_CONNEXA_PROV = text("""
SELECT 
    base.c_proveedor,
	P.n_proveedor,
    COUNT(DISTINCT oc_sgm) AS oc_sgm_generadas,
    SUM(q_bultos_ci)       AS bultos_sgm
FROM (
    SELECT 
        C_PROVEEDOR AS c_proveedor,
        CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm,
        COALESCE(Q_BULTOS_KILOS_DIARCO,0) AS q_bultos_ci
    FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
		
    WHERE F_ALTA_SIST >= :desde
      AND F_ALTA_SIST <  :hasta
) AS base
LEFT JOIN [repl].[T020_PROVEEDOR] P ON base.c_proveedor = P.c_proveedor
	
GROUP BY base.c_proveedor, P.n_proveedor;
""")


# ============================================================
# SQL — Ventas por proveedor (PostgreSQL)
# ============================================================

SQL_VENTAS_PROVEEDOR = text("""
SELECT 
    v.fecha,
    v.codigo_articulo,
    v.sucursal AS codigo_sucursal,
    s.suc_nombre,
    p.c_proveedor_primario AS c_proveedor,
    pr.n_proveedor,
    SUM(v.unidades) AS unidades
FROM src.base_ventas_extendida v
JOIN src.base_productos_vigentes p 
      ON p.c_articulo = v.codigo_articulo
JOIN src.m_91_sucursales s
      ON s.id_tienda = v.sucursal::text
LEFT JOIN src.m_10_proveedores pr
      ON pr.c_proveedor = p.c_proveedor_primario
WHERE v.fecha >= :desde
  AND v.fecha <= :hasta
  AND p.c_proveedor_primario = :proveedor
GROUP BY 
    v.fecha, v.codigo_articulo, v.sucursal,
    s.suc_nombre, p.c_proveedor_primario, pr.n_proveedor
ORDER BY v.fecha, v.sucursal, v.codigo_articulo;
""")


# ============================================================
# Funciones públicas — Rankings de proveedores
# ============================================================

def get_ranking_proveedores_pg(
    pg_engine_diarco: Engine,
    desde: date,
    hasta: date,
    topn: int = 10
) -> pd.DataFrame:
    """
    Ranking de proveedores por OC y bultos desde Connexa (lado diarco_data),
    usando mon.v_oc_generadas_mensual + dimensión de proveedores.
    """
    if pg_engine_diarco is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta, "topn": topn}
    with pg_engine_diarco.connect() as con:
        res = con.execute(SQL_RANKING_PROVEEDORES_PG, params)
        df = _to_df(res)

    if df.empty:
        return df

    df["proveedor"] = df["proveedor"].astype(str)

    for col in ("oc_total", "bultos_total"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "oc_total" in df.columns:
        df = df.sort_values("oc_total", ascending=False)

    return df


def get_ranking_proveedores_desde_ci(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
    topn: int = 10
) -> pd.DataFrame:
    """
    Ranking de proveedores por bultos y OC SGM originadas en CONNEXA,
    a partir de T874 + T080 (SQL_SGM_I4_PROV_DETALLE).
    """
    if sqlserver_engine is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta}
    with sqlserver_engine.connect() as con:
        df_det = pd.read_sql(SQL_SGM_I4_PROV_DETALLE, con, params=params)

    if df_det.empty:
        return df_det

    if "c_proveedor" in df_det.columns:
        df_det["c_proveedor"] = pd.to_numeric(df_det["c_proveedor"], errors="coerce")

    if "q_bultos_ci" in df_det.columns:
        df_det["q_bultos_ci"] = pd.to_numeric(df_det["q_bultos_ci"], errors="coerce").fillna(0.0)

    rk = (
        df_det.groupby(["c_proveedor", "n_proveedor"], dropna=False)
              .agg(
                  oc_distintas=("oc_sgm", "nunique"),
                  bultos_total=("q_bultos_ci", "sum"),
              )
              .reset_index()
    )

    rk = rk.sort_values("bultos_total", ascending=False).head(topn)
    rk["label"] = rk["n_proveedor"].astype(str)

    return rk


def get_ranking_proveedores_resumen(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
    topn: int = 5
) -> pd.DataFrame:
    """
    Función de alto nivel orientada a la Portada:
    devuelve el Top de proveedores abastecidos vía CI → SGM.
    """
    return get_ranking_proveedores_desde_ci(
        sqlserver_engine=sqlserver_engine,
        desde=desde,
        hasta=hasta,
        topn=topn,
    )


# ============================================================
# Funciones públicas — Proveedores CI vs SGM (proporciones)
# ============================================================

def get_proveedores_ci_vs_sgm_mensual(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Devuelve la proporción mensual de proveedores gestionados vía CONNEXA
    sobre el total de proveedores con OC en SGM.
    """
    if sqlserver_engine is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta}
    with sqlserver_engine.connect() as con:
        df = pd.read_sql(SQL_SGM_I4_PROV_MENSUAL, con, params=params)

    if df.empty:
        return df

    df["mes"] = pd.to_datetime(df["mes"])

    for col in ("prov_totales_sgm", "prov_desde_ci"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    if "proporcion_ci_prov" in df.columns:
        df["proporcion_ci_prov"] = pd.to_numeric(
            df["proporcion_ci_prov"], errors="coerce"
        ).fillna(0.0)

    return df


def get_proveedores_ci_sin_cabecera(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Devuelve el listado de proveedores presentes en T874 (CI)
    que no tienen cabecera T080 asociada en el rango.
    """
    if sqlserver_engine is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta}
    with sqlserver_engine.connect() as con:
        df = pd.read_sql(SQL_SGM_I4_PROV_SIN_CABE, con, params=params)

    if df.empty:
        return df

    if "c_proveedor" in df.columns:
        df["c_proveedor"] = pd.to_numeric(df["c_proveedor"], errors="coerce")

    return df


# ============================================================
# Funciones públicas — Resumen Connexa vs SGM por proveedor
# ============================================================

def get_resumen_proveedores_connexa(
    pg_engine_diarco: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Resumen por proveedor desde Connexa (PostgreSQL, diarco_data).

    Columnas:
      - c_proveedor
      - pedidos_connexa
      - bultos_connexa
    """
    if pg_engine_diarco is None:
        return pd.DataFrame()

    params_pg = {"desde": desde, "hasta": hasta}
    with pg_engine_diarco.connect() as con_pg:
        df_pg = pd.read_sql(SQL_PG_CONNEXA_PROV, con_pg, params=params_pg)

    return df_pg


def get_resumen_proveedores_sgm_desde_ci(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Resumen por proveedor de las OC SGM generadas desde CI,
    a partir de T874 (SQL Server).

    Columnas:
      - c_proveedor
      - oc_sgm_generadas
      - bultos_sgm
    """
    if sqlserver_engine is None:
        return pd.DataFrame()

    params_sgm = {"desde": desde, "hasta": hasta}
    with sqlserver_engine.connect() as con_ss:
        df_sgm = pd.read_sql(SQL_SGM_CONNEXA_PROV, con_ss, params=params_sgm)

    return df_sgm


def get_resumen_proveedor_connexa_vs_sgm(
    pg_engine_diarco: Engine,
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Resumen combinado Connexa + SGM por proveedor.
    """
    if pg_engine_diarco is None or sqlserver_engine is None:
        return pd.DataFrame()

    df_pg = get_resumen_proveedores_connexa(pg_engine_diarco, desde, hasta)
    df_sgm = get_resumen_proveedores_sgm_desde_ci(sqlserver_engine, desde, hasta)

    if df_pg.empty and df_sgm.empty:
        return pd.DataFrame()

    for df in (df_pg, df_sgm):
        if not df.empty and "c_proveedor" in df.columns:
            df["c_proveedor"] = pd.to_numeric(df["c_proveedor"], errors="coerce")

    df = pd.merge(
        df_pg,
        df_sgm,
        on="c_proveedor",
        how="outer",
        suffixes=("_pg", "_sgm"),
    ).fillna(0)

    if "pedidos_connexa_pg" in df.columns and "pedidos_connexa" not in df.columns:
        df.rename(columns={"pedidos_connexa_pg": "pedidos_connexa"}, inplace=True)
    if "bultos_connexa_pg" in df.columns and "bultos_connexa" not in df.columns:
        df.rename(columns={"bultos_connexa_pg": "bultos_connexa"}, inplace=True)

    for col in ("pedidos_connexa", "oc_sgm_generadas", "bultos_connexa", "bultos_sgm"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if "oc_sgm_generadas" in df.columns:
        df["prop_oc_connexa_sobre_sgm"] = df.apply(
            lambda r: (r["pedidos_connexa"] / r["oc_sgm_generadas"])
            if r.get("oc_sgm_generadas", 0) > 0
            else 0.0,
            axis=1,
        )
    else:
        df["prop_oc_connexa_sobre_sgm"] = 0.0

    return df


# ============================================================
# Función pública — alias para proporción mensual (naming consistente)
# ============================================================

def get_proporcion_proveedores_ci_mensual(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Alias legible para la capa de páginas:
    proporción mensual de proveedores CI vs SGM.
    """
    return get_proveedores_ci_vs_sgm_mensual(sqlserver_engine, desde, hasta)


# ============================================================
# Función pública — Detalle de proveedores CI (drill-down)
# ============================================================

def get_detalle_proveedores_ci(
    sqlserver_engine: Engine,
    desde: date,
    hasta: date,
) -> pd.DataFrame:
    """
    Devuelve el detalle de proveedores CI → SGM (T874 + T080),
    basado en SQL_SGM_I4_PROV_DETALLE.
    """
    if sqlserver_engine is None:
        return pd.DataFrame()

    params = {"desde": desde, "hasta": hasta}
    with sqlserver_engine.connect() as con:
        df = pd.read_sql(SQL_SGM_I4_PROV_DETALLE, con, params=params)

    return df


# ============================================================
# Función pública — Ventas por proveedor
# ============================================================

def get_ventas_proveedor(
    pg_engine_diarco: Engine,
    desde: date,
    hasta: date,
    proveedor: int,
    articulo: Optional[int] = None,
    sucursal: Optional[str] = None,
) -> pd.DataFrame:
    """
    Devuelve el detalle de ventas por proveedor en el rango.
    """
    if pg_engine_diarco is None:
        return pd.DataFrame()

    params = {
        "desde": desde,
        "hasta": hasta,
        "proveedor": proveedor,
    }

    with pg_engine_diarco.connect() as con:
        df = pd.read_sql(SQL_VENTAS_PROVEEDOR, con, params=params)

    if df.empty:
        return df

    if articulo is not None and "codigo_articulo" in df.columns:
        df = df[df["codigo_articulo"] == articulo]

    if sucursal is not None and "codigo_sucursal" in df.columns:
        df = df[df["codigo_sucursal"].astype(str) == str(sucursal)]

    df["fecha"] = pd.to_datetime(df["fecha"])

    if "unidades" in df.columns:
        df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce").fillna(0.0)

    return df
# ============================================================
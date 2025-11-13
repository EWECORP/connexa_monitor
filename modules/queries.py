
from sqlalchemy import text


# ------------------------------------------------
#  MODULO STOCK
#--------------------------------------------------
QRY_STOCK_SUCURSAL = """
SELECT ...
FROM src.base_stock_sucursal;
"""

QRY_PRODUCTOS_VIGENTES = """
SELECT ...
FROM src.base_productos_vigentes;
"""

QRY_VENTAS_30D = """
SELECT
    codigo_articulo,
    sucursal AS codigo_sucursal,
    SUM(unidades) AS unidades_30d,
    COUNT(DISTINCT fecha) AS dias_con_venta,
    SUM(unidades) / 30.0 AS venta_promedio_diaria_30d
FROM src.base_ventas_extendida
WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
    codigo_articulo,
    sucursal;
"""


# --- DDL de vistas y soportes (idempotente) ---
DDL_VIEW_OC_GENERADAS_BASE = """
CREATE SCHEMA IF NOT EXISTS mon;

CREATE OR REPLACE VIEW mon.v_oc_generadas_mensual
AS
WITH base AS (
        SELECT date_trunc('month'::text, (t080_oc_precarga_kikker.f_alta_sist AT TIME ZONE 'America/Argentina/Buenos_Aires'::text))::date AS mes,
          t080_oc_precarga_kikker.c_comprador,
          t080_oc_precarga_kikker.c_proveedor,
          t080_oc_precarga_kikker.c_compra_kikker,
          COALESCE(t080_oc_precarga_kikker.q_bultos_kilos_diarco, 0::numeric) AS q_bultos
          FROM t080_oc_precarga_kikker
        WHERE t080_oc_precarga_kikker.f_alta_sist IS NOT NULL
      )
SELECT base.mes,
  base.c_comprador,
  base.c_proveedor,
  count(DISTINCT base.c_compra_kikker) AS total_oc,
  sum(base.q_bultos) AS total_bultos
  FROM base
GROUP BY base.mes, base.c_comprador, base.c_proveedor
ORDER BY base.mes, base.c_comprador, base.c_proveedor;
"""

# Vista extendida (nombres de comprador/proveedor)
DDL_VIEW_OC_GENERADAS_EXT = """
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

# Vista extendida por sucursal (para drill-down futuro)
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

def ensure_mon_objects(pg_engine):
    # Asegura vistas e índices (idempotente). Requiere permisos CREATE VIEW / CREATE INDEX.
    with pg_engine.begin() as con:
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_BASE)
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_EXT)
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_SUC_EXT)
        for stmt in IDX_OC_GENERADAS + IDX_DIM:
            con.exec_driver_sql(stmt)

# ---------- Consultas usadas por la Página 1 ----------
SQL_OC_GENERADAS_RANGO = text("""
SELECT *
FROM mon.v_oc_generadas_mensual
WHERE mes >= :desde AND mes <= :hasta
ORDER BY mes, c_comprador, c_proveedor;
""")

# Con nombres (comprador y proveedor)
SQL_OC_GENERADAS_RANGO_EXT = text("""
SELECT *
FROM mon.v_oc_generadas_mensual_sucursal_ext
WHERE mes >= :desde AND mes <= :hasta
ORDER BY mes, n_comprador NULLS LAST, n_proveedor NULLS LAST, c_proveedor;
""")

SQL_RANKING_COMPRADORES = text("""
SELECT
  c_comprador,
  SUM(total_oc)::bigint AS oc_total,
  SUM(total_bultos)::numeric AS bultos_total
FROM mon.v_oc_generadas_mensual
WHERE mes >= :desde AND mes <= :hasta
GROUP BY c_comprador
ORDER BY oc_total DESC NULLS LAST
LIMIT :topn;
""")

SQL_RANKING_COMPRADORES_NOMBRE = text("""
SELECT
  COALESCE((n_comprador, CAST(c_comprador AS TEXT))) AS comprador,
  SUM(total_oc)::bigint AS oc_total,
  SUM(total_bultos)::numeric AS bultos_total
FROM mon.v_oc_generadas_mensual_ext
WHERE mes >= :desde AND mes <= :hasta
GROUP BY COALESCE((n_comprador, CAST(c_comprador AS TEXT)))
ORDER BY oc_total DESC NULLS LAST
LIMIT :topn;
""")

# PostgreSQL (CONNEXA)
SQL_PG_KIKKER_MENSUAL = text("""
SELECT
  date_trunc('month', (f_alta_sist AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
  COUNT(DISTINCT c_compra_kikker)         AS kikker_distintos_pg,
  SUM(COALESCE(q_bultos_kilos_diarco,0))  AS total_bultos_pg
FROM public.t080_oc_precarga_kikker
WHERE f_alta_sist >= :desde 
  AND f_alta_sist  < (:hasta  + INTERVAL '1 day')
GROUP BY 1
ORDER BY 1;
""")

# SQL Server (SGM) — mensual dual (KIKKER vs OC SGM)
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

# SQL Server (detalle para rankings/export)
SQL_SGM_KIKKER_DETALLE = text("""
WITH src AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST) AS f_alta_date,
    C_COMPRA_KIKKER,
    C_COMPRADOR, C_PROVEEDOR, C_ARTICULO, C_SUCU_EMPR,
    COALESCE(Q_BULTOS_KILOS_DIARCO, 0) AS q_bultos,
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-06-01' AS DATE) -- evitar conversión masiva
)
SELECT *
FROM src
WHERE f_alta_date >= :desde AND f_alta_date < DATEADD(day, 1, :hasta)
ORDER BY f_alta_date DESC, C_COMPRA_KIKKER, oc_sgm;
""")

# SQL Server (diagnóstico duplicaciones KIKKER→OC SGM)
SQL_SGM_KIKKER_DUP = text("""
WITH src AS (
  SELECT
    C_COMPRA_KIKKER,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST)<  DATEADD(day, 1, :hasta)
)
SELECT C_COMPRA_KIKKER,
      COUNT(DISTINCT oc_sgm) AS oc_sgm_unicas
FROM src
GROUP BY C_COMPRA_KIKKER
HAVING COUNT(DISTINCT oc_sgm) > 1
ORDER BY oc_sgm_unicas DESC;
""")

# SQL Server (diagnóstico duplicaciones KIKKER→OC SGM)
SQL_C_COMPRA_KIKKERP_GEN = text("""
SELECT DISTINCT c_compra_kikker AS kikker
FROM public.t080_oc_precarga_kikker
WHERE f_alta_sist >= :desde
  AND f_alta_sist  < (:hasta + INTERVAL '1 day')
""")

# set de NRO C_COMPRA_KIKKER en SGM
SQL_C_COMPRA_KIKKER_GEN = text("""
    SELECT DISTINCT c_compra_kikker AS kikker
    FROM public.t080_oc_precarga_kikker
    WHERE f_alta_sist >= :desde AND f_alta_sist < :hasta_mas_1
""")

SQL_PROPORCION_CI_VS_SGM = text("""
WITH pg AS (
  SELECT
    date_trunc('month', (f_alta_sist AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
    COUNT(DISTINCT c_compra_kikker)         AS oc_desde_connexa,
    SUM(COALESCE(q_bultos_kilos_diarco,0))  AS total_bultos_pg
  FROM public.t080_oc_precarga_kikker
  WHERE f_alta_sist >= :desde 
    AND f_alta_sist  < (:hasta  + INTERVAL '1 day')
  GROUP BY 1
), sgm AS (
  SELECT
    DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1) AS mes,
    COUNT(DISTINCT oc_sgm)         AS oc_totales_sgm,
    SUM(q_bultos)                  AS total_bultos_sgm
  FROM (  
        SELECT
          TRY_CONVERT(date, F_ALTA_SIST) AS f_alta_date,
          C_COMPRA_KIKKER,
          COALESCE(Q_BULTOS_KILOS_DIARCO, 0) AS q_bultos,
          CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
          CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc,
          CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
        FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
        WHERE F_ALTA_SIST >= CAST('2025-06-01' AS DATE) -- evitar conversión masiva
       ) AS base
  WHERE f_alta_date >= :desde AND f_alta_date < DATEADD(day, 1, :hasta)
  GROUP BY DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1)
)
SELECT
  COALESCE(pg.mes, sgm.mes) AS mes,
  COALESCE(oc_desde_connexa, 0) AS oc_desde_connexa,
  COALESCE(oc_totales_sgm, 0)   AS oc_totales_sgm,
  COALESCE(total_bultos_pg, 0)  AS total_bultos_pg,
  COALESCE(total_bultos_sgm, 0) AS  total_bultos_sgm
FROM pg
FULL OUTER JOIN sgm ON pg.mes = sgm.mes
ORDER BY mes;
""")


# ==================== Indicador 3 — SQL Server (Proporción CI vs Total SGM) ====================
# Fuentes:
#   - [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]  (OC originadas por CI, con prefijo/sufijo SGM)
#   - [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]                   (Cabecera de OC en SGM)
#
# Criterios:
#   - Fecha desde/hasta: se compara contra fecha robusta (f_alta_date) usando TRY_CONVERT/CAST.
#   - Solo cuentan OC SGM válidas cuando U_PREFIJO_OC y U_SUFIJO_OC <> 0.
#   - Identificador de OC SGM: CONCAT(U_PREFIJO_OC, '-', U_SUFIJO_OC).
#   - El mes se alinea al F_ALTA_SIST de SGM (cabecera), para que numerador/denominador miren el mismo calendario.

SQL_SGM_I3_MENSUAL = text("""
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                   AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                    AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm,
    C_OC
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
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
  COUNT(DISTINCT CASE WHEN t.oc_sgm IS NOT NULL THEN c.oc_sgm END)      AS oc_desde_ci,
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

SQL_SGM_I3_SIN_CABE = text("""
-- KIKKER con prefijo/sufijo no-cero en T874 que NO tienen cabecera en T080 en el rango
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                    AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm,
    C_COMPRA_KIKKER
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
      AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST) AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
rango AS (SELECT :desde AS d, :hasta AS h),
t874_r AS (
  SELECT t.* FROM t874 t CROSS JOIN rango r
  WHERE t.f_alta_date >= r.d AND t.f_alta_date < DATEADD(day, 1, r.h)
),
cabe_r AS (
  SELECT c.* FROM cabe c CROSS JOIN rango r
  WHERE c.f_alta_date >= r.d AND c.f_alta_date < DATEADD(day, 1, r.h)
)
SELECT t.C_COMPRA_KIKKER, t.u_prefijo_oc, t.u_sufijo_oc, t.oc_sgm, t.f_alta_date
FROM t874_r t
LEFT JOIN cabe_r c
  ON c.u_prefijo_oc = t.u_prefijo_oc AND c.u_sufijo_oc = t.u_sufijo_oc
WHERE c.oc_sgm IS NULL
ORDER BY t.f_alta_date DESC;
""")

# ==================== Indicador 4 — Proveedores CI (SQL Server) ====================
# Fuentes:
#   - [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]  (origen CI)
#   - [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]                   (cabecera SGM)
#
# Criterios:
#   - Solo consideran casos con U_PREFIJO_OC y U_SUFIJO_OC <> 0 (circuito cerrado).
#   - Mes de referencia: F_ALTA_SIST de SGM (cabecera), para alinear numerador/denominador.

SQL_SGM_I4_PROV_MENSUAL = text("""
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                             AS c_proveedor
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
        AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                             AS c_proveedor
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
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

SQL_SGM_I4_PROV_DETALLE = text("""
-- Detalle de proveedores CI → SGM en el rango (para ranking/drill-down)
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date_ci,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                           AS c_proveedor_ci,
    COALESCE(Q_BULTOS_KILOS_DIARCO,0)                                     AS q_bultos_ci
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
        AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date_sgm,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                           AS c_proveedor_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
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
  CONCAT(c.u_prefijo_oc, '-', c.u_sufijo_oc) AS oc_sgm,
  t.q_bultos_ci
FROM cabe_r c
JOIN t874_r t
  ON t.u_prefijo_oc = c.u_prefijo_oc AND t.u_sufijo_oc = c.u_sufijo_oc
ORDER BY c.f_alta_date_sgm DESC, c.c_proveedor_sgm;
""")

SQL_SGM_I4_PROV_SIN_CABE = text("""
-- Proveedores presentes en T874 (con prefijo/sufijo ≠ 0) sin cabecera en T080 en el rango
WITH t874 AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc,
    C_PROVEEDOR                                                           AS c_proveedor
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
      AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST)                                        AS f_alta_date,
    CAST(U_PREFIJO_OC AS varchar(32))                                     AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32))                                     AS u_sufijo_oc
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE F_ALTA_SIST >= CAST('2025-08-01' AS DATE) -- evitar conversión masiva
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


########### INDICES qeu ayudan al Control de Interfaces ###########
"""
-- Crear una vez por tabla controlada (idempotente)
CREATE INDEX IF NOT EXISTS idx_base_productos_vigentes_fecha_extraccion ON src.base_productos_vigentes (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_base_stock_sucursal_fecha_extraccion     ON src.base_stock_sucursal (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t020_proveedor_fecha_extraccion          ON src.t020_proveedor (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_m_3_articulos_fecha_extraccion           ON src.m_3_articulos (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t050_articulos_fecha_extraccion          ON src.t050_articulos (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t051_articulos_sucursal_fecha_extraccion ON src.t051_articulos_sucursal (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t060_stock_fecha_extraccion              ON src.t060_stock (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t080_oc_cabe_fecha_extraccion            ON src.t080_oc_cabe (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t081_oc_deta_fecha_extraccion            ON src.t081_oc_deta (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t100_empresa_suc_fecha_extraccion        ON src.t100_empresa_suc (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t052_articulos_proveedor_fecha_extraccion ON src.t052_articulos_proveedor (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t710_estadis_oferta_folder_fecha_extraccion ON src.t710_estadis_oferta_folder (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t710_estadis_reposicion_fecha_extraccion ON src.t710_estadis_reposicion (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t117_compradores_fecha_extraccion        ON src.t117_compradores (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_t114_rubros_fecha_extraccion             ON src.t114_rubros (fecha_extraccion);

CREATE INDEX IF NOT EXISTS idx_m_91_sucursales_f_proc ON src.m_91_sucursales (f_proc);
CREATE INDEX IF NOT EXISTS idx_m_92_depositos_f_proc  ON src.m_92_depositos (f_proc);
CREATE INDEX IF NOT EXISTS idx_m_93_sustitutos_f_proc ON src.m_93_sustitutos (f_proc);
CREATE INDEX IF NOT EXISTS idx_m_94_alternativos_f_proc ON src.m_94_alternativos (f_proc);
CREATE INDEX IF NOT EXISTS idx_m_95_sensibles_f_proc  ON src.m_95_sensibles (f_proc);
CREATE INDEX IF NOT EXISTS idx_m_96_stock_seguridad_f_proc ON src.m_96_stock_seguridad (f_proc);

CREATE INDEX IF NOT EXISTS idx_t702_est_vtas_por_articulo_f_venta           ON src.t702_est_vtas_por_articulo (f_venta);
CREATE INDEX IF NOT EXISTS idx_t702_est_vtas_por_articulo_dbarrio_f_venta   ON src.t702_est_vtas_por_articulo_dbarrio (f_venta);

CREATE INDEX IF NOT EXISTS idx_base_forecast_oc_demoradas_f_alta_sist ON src.base_forecast_oc_demoradas (f_alta_sist);


"""

####---------------- NUEVOS INDICADORES PRODUCTIVIDAD ----------------####
# ===================== Forecast → Propuesta (PostgreSQL) =====================


def ensure_forecast_views(pg_engine):
    """Crea/actualiza la vista base para Forecast→Propuesta."""
    with pg_engine.begin() as con:
        con.exec_driver_sql(DDL_V_FORECAST_PROPUESTA_BASE)

# Vista base: “aplana” FE + PP + estado y calcula métricas de tiempo
DDL_V_FORECAST_PROPUESTA_BASE = """
CREATE SCHEMA IF NOT EXISTS views;

CREATE OR REPLACE VIEW views.v_forecast_propuesta_base AS
SELECT
  fe.id                                        AS fe_id,
  fe.start_execution                           AS fe_start,
  fe.end_execution                             AS fe_end,
  fe.last_execution                            AS fe_last,
  fe."timestamp"                               AS fe_ts,
  fe.supply_forecast_execution_id              AS fe_exec_id,
  fe.supply_forecast_execution_schedule_id     AS fe_sched_id,
  fe.ext_supplier_code                         AS ext_supplier_code,
  fe.monthly_net_margin_in_millions            AS fe_net_margin_m,
  fe.monthly_purchases_in_millions             AS fe_purchases_m,
  fe.monthly_sales_in_millions                 AS fe_sales_m,
  fe.sotck_days                                AS stock_days,               -- nota: 'sotck' tal como viene
  fe.sotck_days_colors                         AS stock_days_colors,
  fe.supplier_id                               AS fe_supplier_id,
  fe.supply_forecast_execution_status_id       AS fe_status_id,
  sfes.name                                    AS fe_status_name,
  sfes.description                             AS fe_status_desc,
  fe.contains_breaks                           AS contains_breaks,
  fe.maximum_backorder_days                    AS max_backorder_days,
  fe.otif                                      AS otif,
  fe.total_products                            AS fe_total_products,
  fe.total_units                               AS fe_total_units,
  fe.supply_purchase_proposal_id               AS pp_id,
  pp.closed_at                                 AS pp_closed_at,
  pp.comments                                  AS pp_comments,
  pp.open_at                                   AS pp_open_at,
  pp.proposal_number                           AS pp_number,
  pp.status                                    AS pp_status,
  pp."timestamp"                               AS pp_ts,
  pp.total_amount                              AS pp_total_amount,
  pp.total_products                            AS pp_total_products,
  pp.total_sites                               AS pp_total_sites,
  pp.total_units                               AS pp_total_units,
  pp.buyer_id                                  AS buyer_id,
  pp.supplier_id                               AS pp_supplier_id,
  pp.user_id                                   AS user_id,
  pp.user_name                                 AS user_name,

  -- Timestamp “base” para series (prioriza evento de propuesta, si existe)
  COALESCE(pp.open_at, fe.end_execution, fe.start_execution, fe."timestamp")          AS base_ts,

  -- Métricas de tiempo (minutos)
  EXTRACT(EPOCH FROM (fe.end_execution - fe.start_execution))/60.0                    AS exec_time_min,        -- duración del forecast
  EXTRACT(EPOCH FROM (pp.open_at - fe.end_execution))/60.0                            AS lead_open_min,        -- fin forecast → apertura propuesta
  EXTRACT(EPOCH FROM (COALESCE(pp.closed_at, now()) - COALESCE(pp.open_at, COALESCE(pp."timestamp", fe.end_execution))))/60.0
                                                                                      AS adjust_time_min       -- apertura → cierre (o hasta ahora)
FROM public.spl_supply_forecast_execution_execute fe
LEFT JOIN public.spl_supply_purchase_proposal pp
  ON pp.id = fe.supply_purchase_proposal_id
LEFT JOIN public.spl_supply_forecast_execution_status sfes
  ON sfes.id = fe.supply_forecast_execution_status_id
;
"""


# --- Consultas parametrizadas (tomen 'desde'/'hasta' como date/datetime de Python) ---

# Serie mensual de conversión: ejecuciones de forecast (terminadas) vs propuestas generadas
SQL_FP_CONVERSION_MENSUAL = text("""
WITH b AS (
  SELECT *
  FROM views.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
)
, fe_m AS (
  SELECT date_trunc('month', (COALESCE(fe_end, fe_start, fe_ts) AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
         COUNT(DISTINCT fe_id) AS ejecuciones
  FROM b
  WHERE fe_end IS NOT NULL  -- consideramos forecasts completados
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

# Productividad por comprador (mensual): #propuestas, monto, tiempos P50/P90
SQL_FP_MENSUAL_COMPRADOR = text("""
WITH b AS (
  SELECT *
  FROM views.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT
  date_trunc('month', (base_ts AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
  COALESCE(NULLIF(trim(user_name), ''), buyer_id::text, '- sin comprador -')         AS comprador,
  COUNT(DISTINCT pp_id)                                                               AS propuestas,
  SUM(pp_total_amount)                                                                AS monto_total,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY adjust_time_min)                        AS p50_ajuste_min,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY adjust_time_min)                        AS p90_ajuste_min,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY lead_open_min)                          AS p50_lead_min,
  AVG(exec_time_min)                                                                  AS avg_exec_min
FROM b
GROUP BY 1, 2
ORDER BY mes, comprador;
""")

# Ranking de compradores (rango): por #propuestas y tiempos
SQL_FP_RANKING_COMPRADOR = text("""
WITH b AS (
  SELECT *
  FROM views.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT
  COALESCE(NULLIF(trim(user_name), ''), buyer_id::text, '- sin comprador -') AS comprador,
  COUNT(DISTINCT pp_id)                                                      AS propuestas,
  SUM(pp_total_amount)                                                       AS monto_total,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY adjust_time_min)               AS p50_ajuste_min,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY adjust_time_min)               AS p90_ajuste_min
FROM b
GROUP BY 1
ORDER BY propuestas DESC, monto_total DESC
LIMIT :topn;
""")

# Estados de propuestas en el rango
SQL_FP_ESTADOS_PROP = text("""
WITH b AS (
  SELECT *
  FROM views.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT pp_status, COUNT(*) AS propuestas
FROM b
GROUP BY pp_status
ORDER BY propuestas DESC NULLS LAST;
""")

# Detalle para exportar (rango)
SQL_FP_DETALLE = text("""
SELECT
  base_ts, fe_id, fe_start, fe_end, fe_status_name,
  pp_id, pp_number, pp_status, pp_open_at, pp_closed_at,
  buyer_id, user_name, pp_total_amount, pp_total_units, pp_total_products,
  exec_time_min, lead_open_min, adjust_time_min, ext_supplier_code, fe_supplier_id
FROM views.v_forecast_propuesta_base
WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
ORDER BY base_ts DESC, pp_id NULLS LAST;
""")

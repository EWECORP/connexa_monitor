# ============================================================
# STOCK POR SUCURSAL
# ============================================================

QRY_COMPPRADORES = """
SELECT
    cod_comprador::int     AS cod_comprador,
    n_comprador::text      AS n_comprador
FROM src.m_9_compradores
ORDER BY cod_comprador;
"""

QRY_PROVEEDORES = """
SELECT
    c_proveedor::int     AS c_proveedor,
    n_proveedor::text    AS n_proveedor
FROM src.m_10_proveedores
ORDER BY c_proveedor;
"""

QRY_MV_STOCK_CARTERA_30D = """
SELECT *
FROM datamart.mv_stock_cartera_30d;
"""




# /* ============================================================
#    RECREAR MV: datamart.mv_stock_cartera_30d  (CORREGIDA)
#    Motivo corrección:
#      - Existen códigos alfanuméricos tipo "82CD" => NO se puede castear a INT.
#    Decisión:
#      - codigo_sucursal y cod_cd se manejan como TEXT (identificadores).
#      - codigo_articulo y codigo_proveedor se mantienen como INT (siempre que sean numéricos).
#    Incluye:
#      - DROP + CREATE MV (WITH NO DATA)
#      - Índices (incluye único para REFRESH CONCURRENTLY)
#      - REFRESH final
#    ============================================================ */

QRY_CREAR_MV_STOCK_CARTERA_30D = """
-- 0) Esquema
CREATE SCHEMA IF NOT EXISTS datamart;

-- 1) Borrar MV si existe
DROP MATERIALIZED VIEW IF EXISTS datamart.mv_stock_cartera_30d;

-- 2) Crear MV
CREATE MATERIALIZED VIEW datamart.mv_stock_cartera_30d
AS
WITH vtas AS (
    SELECT
        codigo_articulo::int                    AS codigo_articulo,
        sucursal::text                          AS codigo_sucursal,
        SUM(unidades)::numeric                  AS unidades_30d,
        COUNT(DISTINCT fecha)                   AS dias_con_venta,
        (SUM(unidades)::numeric / 30.0)         AS venta_promedio_diaria_30d,
        MAX(fecha)::date                        AS fecha_ultima_venta_30d
    FROM src.base_ventas_extendida
    WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2
),
stk AS (
    SELECT
        bs.codigo_articulo::int                 AS codigo_articulo,
        bs.codigo_sucursal::text                AS codigo_sucursal,
        bs.codigo_proveedor::int                AS codigo_proveedor,
        bs.precio_costo::numeric                AS precio_costo,
        bs.precio_venta::numeric                AS precio_venta,

        COALESCE(bs.stock, 0)::numeric          AS stock,
        COALESCE(bs.stock_reserva, 0)::numeric  AS stock_reserva,

        bs.fecha_ultimo_ingreso::date           AS fecha_ultimo_ingreso,
        bs.fecha_ultima_venta::date             AS fecha_ultima_venta,
        bs.ultimo_ingreso                       AS ultimo_ingreso,

        bs.dias_stock::numeric                  AS dias_stock,
        bs.q_dias_stock::numeric                AS q_dias_stock,
        bs.q_dias_sobre_stock::numeric          AS q_dias_sobre_stock,

        -- próximos ingresos (si existen)
        bs.pedido_pendiente::numeric            AS pedido_pendiente,
        bs.transfer_pendiente::numeric          AS transfer_pendiente,
        bs.pedido_sgm::numeric                  AS pedido_sgm
    FROM src.base_stock_sucursal bs
),
vig AS (
    SELECT
        c_articulo::int                         AS codigo_articulo,
        c_sucu_empr::text                       AS codigo_sucursal,
        c_proveedor_primario::int               AS codigo_proveedor_primario,

        COALESCE(stock_minimo, 0)::numeric      AS stock_minimo,
        cod_comprador::int                      AS cod_comprador,

        active_for_purchase::boolean            AS active_for_purchase,
        active_for_sale::boolean                AS active_for_sale,
        active_on_mix::boolean                  AS active_on_mix,

        habilitado::boolean                     AS habilitado,
        abastecimiento::text                    AS abastecimiento,

        -- ⚠️ cod_cd puede venir como "82CD"
        cod_cd::text                            AS cod_cd
    FROM src.base_productos_vigentes
),
transit AS (
    SELECT
        t.c_articulo::int                       AS codigo_articulo,
        t.c_sucu_dest::text                     AS codigo_sucursal,
        SUM(t.q_unid_transito)::numeric         AS en_transito
    FROM src.base_productos_en_transito t
    GROUP BY 1, 2
),
transf AS (
    SELECT
        tr.c_articulo::int                      AS codigo_articulo,
        tr.c_sucu_dest::text                    AS codigo_sucursal,
        SUM(tr.q_pendiente)::numeric            AS transferencias_pendientes
    FROM src.base_transferencias_pendientes tr
    GROUP BY 1, 2
)
SELECT
    s.codigo_articulo,
    s.codigo_sucursal,

    -- Proveedor: priorizar el de stock; si no existe, usar primario
    COALESCE(s.codigo_proveedor, v.codigo_proveedor_primario) AS codigo_proveedor,

    -- Precios
    s.precio_costo,
    s.precio_venta,

    -- Stock
    s.stock,
    s.stock_reserva,
    (s.stock + s.stock_reserva) AS stock_total,

    -- Ventas 30d
    COALESCE(vt.unidades_30d, 0)                AS unidades_30d,
    COALESCE(vt.dias_con_venta, 0)              AS dias_con_venta,
    COALESCE(vt.venta_promedio_diaria_30d, 0)   AS venta_promedio_diaria_30d,

    -- Días stock: si viene 0, recalcular
    COALESCE(
        NULLIF(s.dias_stock, 0),
        CASE
            WHEN COALESCE(vt.venta_promedio_diaria_30d, 0) > 0
                THEN (s.stock + s.stock_reserva) / vt.venta_promedio_diaria_30d
            ELSE NULL
        END
    ) AS dias_stock,

    -- Parámetros / vigencia
    v.stock_minimo,
    v.cod_comprador,
    v.active_for_purchase,
    v.active_for_sale,
    v.active_on_mix,
    v.habilitado,
    v.abastecimiento,
    v.cod_cd,

    -- Parámetros desde stock (si existen)
    s.q_dias_stock,
    s.q_dias_sobre_stock,

    -- Fechas
    s.fecha_ultimo_ingreso,
    COALESCE(s.fecha_ultima_venta, vt.fecha_ultima_venta_30d) AS fecha_ultima_venta,
    s.ultimo_ingreso,

    -- Próximos ingresos / pendientes
    COALESCE(s.pedido_pendiente, 0)             AS pedido_pendiente,
    COALESCE(s.transfer_pendiente, 0)           AS transfer_pendiente,
    COALESCE(s.pedido_sgm, 0)                   AS pedido_sgm,

    -- En tránsito / transferencias pendientes
    COALESCE(t.en_transito, 0)                  AS en_transito,
    COALESCE(tr.transferencias_pendientes, 0)   AS transferencias_pendientes

FROM stk s
LEFT JOIN vig v
  ON v.codigo_articulo = s.codigo_articulo
 AND v.codigo_sucursal = s.codigo_sucursal
LEFT JOIN vtas vt
  ON vt.codigo_articulo = s.codigo_articulo
 AND vt.codigo_sucursal = s.codigo_sucursal
LEFT JOIN transit t
  ON t.codigo_articulo = s.codigo_articulo
 AND t.codigo_sucursal = s.codigo_sucursal
LEFT JOIN transf tr
  ON tr.codigo_articulo = s.codigo_articulo
 AND tr.codigo_sucursal = s.codigo_sucursal
WITH NO DATA;

-- 3) Índices
-- Único (requisito para REFRESH CONCURRENTLY)
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_stock_cartera_30d
ON datamart.mv_stock_cartera_30d (codigo_articulo, codigo_sucursal);

-- Auxiliares para filtros del tablero
CREATE INDEX IF NOT EXISTS ix_mv_stock_cartera_30d_suc
ON datamart.mv_stock_cartera_30d (codigo_sucursal);

CREATE INDEX IF NOT EXISTS ix_mv_stock_cartera_30d_prov
ON datamart.mv_stock_cartera_30d (codigo_proveedor);

CREATE INDEX IF NOT EXISTS ix_mv_stock_cartera_30d_comp
ON datamart.mv_stock_cartera_30d (cod_comprador);

CREATE INDEX IF NOT EXISTS ix_mv_stock_cartera_30d_cd
ON datamart.mv_stock_cartera_30d (cod_cd);

"""
QRY_REFRESH_MV_STOCK_CARTERA_30D = """
-- 4) Cargar datos
REFRESH MATERIALIZED VIEW datamart.mv_stock_cartera_30d;

-- Alternativa futura (sin bloquear lecturas):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY datamart.mv_stock_cartera_30d;

---  CREAMOS FUNCION
CREATE OR REPLACE FUNCTION datamart.refresh_mv_stock_cartera_30d()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF to_regclass('datamart.mv_stock_cartera_30d') IS NULL THEN
        RAISE EXCEPTION 'La materialized view datamart.mv_stock_cartera_30d no existe';
    END IF;

    REFRESH MATERIALIZED VIEW CONCURRENTLY datamart.mv_stock_cartera_30d;
END;
--- USAMOS FUNCION
SELECT datamart.refresh_mv_stock_cartera_30d();
"""
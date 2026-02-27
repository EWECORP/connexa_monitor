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
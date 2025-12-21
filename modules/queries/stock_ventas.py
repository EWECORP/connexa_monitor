#stock_ventas.py
from sqlalchemy import text

"""
Agrupar:

- QRY_STOCK_SUCURSAL
- QRY_PRODUCTOS_VIGENTES
- QRY_VENTAS_30D
- SQL_VENTAS_PROVEEDOR

con funciones:
get_stock_sucursal(pg_engine)
get_productos_vigentes(pg_engine)
get_ventas_30d(pg_engine)
get_ventas_proveedor(pg_engine, desde, hasta, proveedor)
"""

# ------------------------------------------------
#  MODULO STOCK
#--------------------------------------------------
QRY_STOCK_SUCURSAL = """
  SELECT codigo_articulo, codigo_sucursal, codigo_proveedor, precio_venta, precio_costo, factor_venta, ultimo_ingreso, fecha_ultimo_ingreso, 
      fecha_ultima_venta, m_vende_por_peso, venta_unidades_1q, venta_unidades_2q, venta_mes_unidades, venta_mes_valorizada, dias_stock, 
      fecha_stock, stock, transfer_pendiente, pedido_pendiente, promocion, lote, validez_lote, stock_reserva, validez_promocion, 
      q_dias_stock, q_dias_sobre_stock, i_lista_calculado, pedido_sgm, importe_minimo, bultos_minimo, dias_preparacion, 
      fuente_origen, fecha_extraccion, estado_sincronizacion
    FROM src.base_stock_sucursal;
"""

QRY_PRODUCTOS_VIGENTES = """
  SELECT 
      c_sucu_empr,
      c_articulo,
      c_proveedor_primario,
      abastecimiento,
      cod_cd,
      habilitado,
      fecha_registro,
      fecha_baja,
      m_vende_por_peso,
      unid_transferencia,
      q_unid_transferencia,
      pedido_min,
      frente_lineal,
      capacid_gondola,
      stock_minimo,
      cod_comprador,
      promocion,
      active_for_purchase,
      active_for_sale,
      active_on_mix,
      delivered_id,
      product_base_id,
      own_production,
      q_factor_compra,
      full_capacity_pallet,
      number_of_layers,
      number_of_boxes_per_layer,
      fuente_origen,
      fecha_extraccion,
      estado_sincronizacion
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

QRY_MV_STOCK_CARTERA_30D = """
    SELECT *
    FROM mon.mv_stock_cartera_30d;
    """

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

QRY_COMPPRADORES = """
    SELECT cod_comprador, n_comprador
    FROM src.m_9_compradores;
    """

QRY_PROVEEDORES = """
    SELECT c_proveedor, n_proveedor
    FROM src.m_10_proveedores;
    """
    
# ============================================================
# Funciones públicas — STOCK_VENTAS
# ============================================================

def get_stock_sucursal(pg_engine):
    with pg_engine.connect() as conn:
        result = conn.execute(QRY_STOCK_SUCURSAL)
        return result.fetchall()
    
def get_productos_vigentes(pg_engine):
    with pg_engine.connect() as conn:
        result = conn.execute(QRY_PRODUCTOS_VIGENTES)
        return result.fetchall()    
    
def get_ventas_30d(pg_engine):
    with pg_engine.connect() as conn:
        result = conn.execute(QRY_VENTAS_30D)
        return result.fetchall()

def get_compradores(pg_engine):
    with pg_engine.connect() as conn:
        result = conn.execute(QRY_COMPPRADORES)
        return result.fetchall()
    
def get_proveedores(pg_engine):
    with pg_engine.connect() as conn:
        result = conn.execute(QRY_PROVEEDORES)
        return result.fetchall()
    




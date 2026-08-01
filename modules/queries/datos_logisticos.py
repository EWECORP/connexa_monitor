"""Consultas del reporte exportable de parametros logisticos."""

from __future__ import annotations

from sqlalchemy import bindparam, text


SQL_CATALOGO_FILTROS = text("""
WITH compradores AS (
    SELECT c_comprador,
           MAX(NULLIF(TRIM(n_comprador), '')) AS n_comprador
    FROM src.t117_compradores
    WHERE COALESCE(m_baja, 'N') = 'N'
    GROUP BY c_comprador
)
SELECT DISTINCT
    p.cod_comprador::int AS cod_comprador,
    COALESCE(c.n_comprador, p.cod_comprador::text) AS n_comprador
FROM src.base_productos_vigentes p
LEFT JOIN compradores c ON c.c_comprador = p.cod_comprador
ORDER BY n_comprador, cod_comprador;
""")


SQL_CATALOGO_PROVEEDORES = text("""
SELECT DISTINCT
    p.c_proveedor_primario::int AS c_proveedor,
    COALESCE(NULLIF(TRIM(pr.n_proveedor), ''), p.c_proveedor_primario::text) AS n_proveedor
FROM src.base_productos_vigentes p
LEFT JOIN src.t020_proveedor pr ON pr.c_proveedor = p.c_proveedor_primario
ORDER BY n_proveedor, c_proveedor;
""")


SQL_GRUPOS_SUCURSALES = text("""
SELECT
    g.id::text AS grupo_id,
    g.name::text AS grupo,
    s.code::text AS codigo_sucursal
FROM supply_planning.spl_site_group g
LEFT JOIN supply_planning.spl_site_group_relation r
  ON r.site_group_id = g.id
LEFT JOIN supply_planning.spl_site s
  ON s.id = r.site_id
ORDER BY g.name, s.code;
""")


SQL_DATOS_LOGISTICOS = """
SELECT
    p.cod_comprador,
    p.c_proveedor_primario,
    a.c_articulo,
    a.n_articulo_fact,
    p.c_sucu_empr,
    p.abastecimiento,
    p.cod_cd,
    p.habilitado,
    p.fecha_registro,
    p.fecha_baja,
    p.q_peso_unit_art,
    p.m_vende_por_peso,
    p.unid_transferencia,
    p.q_unid_transferencia,
    p.pedido_min,
    p.frente_lineal,
    p.capacid_gondola,
    p.stock_minimo,
    p.promocion,
    p.active_for_purchase,
    p.active_for_sale,
    p.active_on_mix,
    p.delivered_id,
    p.product_base_id,
    p.own_production,
    p.q_factor_compra,
    p.full_capacity_pallet,
    p.number_of_layers,
    p.number_of_boxes_per_layer,
    bs.q_dias_stock,
    bs.q_dias_sobre_stock,
    bs.dias_preparacion,
    bs.importe_minimo,
    bs.bultos_minimo
FROM src.base_productos_vigentes p
JOIN src.t050_articulos a ON p.c_articulo = a.c_articulo
LEFT JOIN src.base_stock_sucursal bs
  ON bs.codigo_articulo = p.c_articulo
 AND bs.codigo_sucursal = p.c_sucu_empr
 AND bs.codigo_proveedor = p.c_proveedor_primario
WHERE 1 = 1
{filtros}
ORDER BY 1, 2, 3, 4;
"""


def construir_consulta_datos_logisticos(
    compradores: tuple[int, ...],
    proveedores: tuple[int, ...],
    sucursales: tuple[int, ...],
    abastecimiento: int | None,
):
    """Construye una consulta parametrizada para los filtros opcionales."""
    condiciones: list[str] = []
    params: dict[str, object] = {}
    expanding: list[str] = []

    if compradores:
        condiciones.append("  AND p.cod_comprador IN :compradores")
        params["compradores"] = compradores
        expanding.append("compradores")
    if proveedores:
        condiciones.append("  AND p.c_proveedor_primario IN :proveedores")
        params["proveedores"] = proveedores
        expanding.append("proveedores")
    if abastecimiento is not None:
        condiciones.append("  AND p.abastecimiento = :abastecimiento")
        params["abastecimiento"] = abastecimiento
    if sucursales:
        condiciones.append("  AND p.c_sucu_empr IN :sucursales")
        params["sucursales"] = sucursales
        expanding.append("sucursales")

    consulta = text(SQL_DATOS_LOGISTICOS.format(filtros="\n".join(condiciones)))
    if expanding:
        consulta = consulta.bindparams(
            *(bindparam(nombre, expanding=True) for nombre in expanding)
        )
    return consulta, params

"""Consultas del reporte exportable de parametros logisticos."""

from __future__ import annotations

from sqlalchemy import bindparam, text


PROVEEDORES_HABILITADOS = (
    275, 327, 415, 1074, 1120, 1156, 1465, 2343, 2676, 2724, 2746,
    2834, 3835, 4201, 4524, 4647, 4772, 4872, 5259, 5568, 5757,
    6298, 6500, 6507, 7256, 8449, 9236, 10588, 11450, 11895,
    13656, 30873, 30956, 31117, 31285, 33135, 34981, 35203, 35521,
    35536, 35702,
)


SQL_CATALOGO_FILTROS = text("""
WITH compradores AS (
    SELECT c_comprador,
           MAX(NULLIF(TRIM(n_comprador), '')) AS n_comprador
    FROM src.t117_compradores
    WHERE COALESCE(m_baja, 'N') = 'N'
    GROUP BY c_comprador
),
proveedores AS (
    SELECT c_proveedor,
           MAX(NULLIF(TRIM(n_proveedor), '')) AS n_proveedor
    FROM src.t020_proveedor
    WHERE COALESCE(m_baja, 'N') = 'N'
      AND COALESCE(m_activo, 'S') = 'S'
    GROUP BY c_proveedor
)
SELECT DISTINCT
    p.cod_comprador::int AS cod_comprador,
    COALESCE(c.n_comprador, p.cod_comprador::text) AS n_comprador,
    p.c_proveedor_primario::int AS c_proveedor,
    COALESCE(pr.n_proveedor, p.c_proveedor_primario::text) AS n_proveedor
FROM src.base_productos_vigentes p
LEFT JOIN compradores c ON c.c_comprador = p.cod_comprador
LEFT JOIN proveedores pr ON pr.c_proveedor = p.c_proveedor_primario
WHERE p.c_proveedor_primario IN :proveedores_habilitados
  AND p.abastecimiento = 3
  AND p.c_sucu_empr >= 300
ORDER BY n_comprador, cod_comprador, n_proveedor, c_proveedor;
""").bindparams(bindparam("proveedores_habilitados", expanding=True))


SQL_CATALOGO_PROVEEDORES = text("""
SELECT
    c_proveedor::int AS c_proveedor,
    COALESCE(NULLIF(TRIM(n_proveedor), ''), c_proveedor::text) AS n_proveedor
FROM src.t020_proveedor
WHERE c_proveedor IN :proveedores_habilitados
  AND COALESCE(m_baja, 'N') = 'N'
  AND COALESCE(m_activo, 'S') = 'S'
ORDER BY n_proveedor, c_proveedor;
""").bindparams(bindparam("proveedores_habilitados", expanding=True))


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
    p.number_of_boxes_per_layer
FROM src.base_productos_vigentes p
JOIN src.t050_articulos a ON p.c_articulo = a.c_articulo
WHERE p.c_proveedor_primario IN :proveedores_habilitados
  AND p.abastecimiento = 3
  AND p.c_sucu_empr >= 300
{filtros}
ORDER BY 1, 2, 3, 4;
"""


def construir_consulta_datos_logisticos(
    compradores: tuple[int, ...],
    proveedores: tuple[int, ...],
    sucursales: tuple[int, ...],
):
    """Construye una consulta parametrizada para los filtros opcionales."""
    condiciones: list[str] = []
    params: dict[str, tuple[int, ...]] = {
        "proveedores_habilitados": PROVEEDORES_HABILITADOS,
    }
    expanding = ["proveedores_habilitados"]

    if compradores:
        condiciones.append("  AND p.cod_comprador IN :compradores")
        params["compradores"] = compradores
        expanding.append("compradores")
    if proveedores:
        condiciones.append("  AND p.c_proveedor_primario IN :proveedores")
        params["proveedores"] = proveedores
        expanding.append("proveedores")
    if sucursales:
        condiciones.append("  AND p.c_sucu_empr IN :sucursales")
        params["sucursales"] = sucursales
        expanding.append("sucursales")

    consulta = text(
        SQL_DATOS_LOGISTICOS.format(filtros="\n".join(condiciones))
    ).bindparams(*(bindparam(nombre, expanding=True) for nombre in expanding))
    return consulta, params

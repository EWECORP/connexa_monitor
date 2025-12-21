# modules/queries/__init__.py
"""
Fachada de alto nivel para los módulos de consultas del CONNEXA Monitor.

Se organiza en tres ejes:
- uso_general:    uso global del sistema, embudos, proporciones CI vs SGM.
- compradores:    gestión y productividad de compradores.
- proveedores:    gestión de proveedores, cobertura CI vs SGM y ventas.

Las páginas de Streamlit deberían, en lo posible, importar directamente
de cada submódulo (uso_general, compradores, proveedores). Este __init__
se mantiene como una capa de conveniencia y para reducir acoples.
"""

# ==============================
# Uso general del sistema
# ==============================
from .uso_general import (  # type: ignore[F401]
    # Objetos de soporte (vistas mon.*)
    ensure_mon_objects,
    ensure_forecast_views,
    # Series y embudos globales
    get_oc_generadas_mensual,
    get_forecast_propuesta_conversion_mensual,
    get_embudo_connexa_sgm_mensual,
    get_proporcion_ci_vs_sgm_mensual,
)

# ==============================
# Gestión de compradores
# ==============================
from .compradores import (  # type: ignore[F401]
    get_ranking_compradores_pg,
    get_ranking_compradores_nombre_pg,
    get_productividad_comprador_mensual,
    get_ranking_comprador_forecast,
    get_ranking_compradores_resumen,
)

# ==============================
# Gestión de proveedores
# ==============================
from .proveedores import (  # type: ignore[F401]
    get_ranking_proveedores_pg,
    get_ranking_proveedores_resumen,
    get_proveedores_ci_vs_sgm_mensual,
    get_proveedores_ci_sin_cabecera,
    get_resumen_proveedores_connexa,
    get_resumen_proveedores_sgm_desde_ci,
    get_proporcion_proveedores_ci_mensual,
    get_detalle_proveedores_ci,
    get_ranking_proveedores_desde_ci,
    get_resumen_proveedor_connexa_vs_sgm,
    get_ventas_proveedor,
)

# ==============================
# Gestión de proveedores
# ==============================
from .efectividad import (  # type: ignore[F401]
    get_estados_propuestas,
    get_detalle_forecast_propuesta,
)

# ==============================
# SGM Bridge
# ==============================
from .sgm_bridge import (  # type: ignore[F401] 
    get_kikker_vs_oc_mensual,
    get_kikker_detalle,
    get_kikker_duplicadas,
    get_proporcion_ci_vs_sgm,
    get_indicador3_mensual,
)

# ==============================
# STOCK VENTAS
# ==============================

from .stock_ventas import (  # type: ignore[F401]
    get_stock_sucursal,
    get_productos_vigentes,
    get_ventas_30d,
    get_compradores,
    get_proveedores,
)
    


__all__ = [
    # uso_general
    "ensure_mon_objects",
    "ensure_forecast_views",
    "get_oc_generadas_mensual",
    "get_forecast_propuesta_conversion_mensual",
    "get_embudo_connexa_sgm_mensual",
    "get_proporcion_ci_vs_sgm_mensual",
    "get_ranking_compradores_resumen",
    # compradores
    "get_ranking_compradores_pg",
    "get_ranking_compradores_nombre_pg",
    "get_productividad_comprador_mensual",
    "get_ranking_comprador_forecast",
    # proveedores
    "get_ranking_proveedores_pg",
    "get_ranking_proveedores_resumen",
    "get_proveedores_ci_vs_sgm_mensual",
    "get_proveedores_ci_sin_cabecera",
    "get_resumen_proveedor_connexa_vs_sgm",
    "get_ventas_proveedor",
    "get_resumen_proveedores_connexa",
    "get_resumen_proveedores_sgm_desde_ci",
    "get_proporcion_proveedores_ci_mensual",
    "get_detalle_proveedores_ci",
    "get_ranking_proveedores_desde_ci",
    # efectividad
    "get_estados_propuestas",
    "get_detalle_forecast_propuesta",
    # sgm_bridge
    "get_kikker_vs_oc_mensual",
    "get_kikker_detalle",
    "get_kikker_duplicadas",
    "get_proporcion_ci_vs_sgm",
    "get_indicador3_mensual",
    # stock_ventas
    "get_stock_sucursal",
    "get_productos_vigentes",
    "get_ventas_30d",
    "get_compradores",
    "get_proveedores",
    
]

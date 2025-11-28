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
)

# ==============================
# Gestión de proveedores
# ==============================
from .proveedores import (  # type: ignore[F401]
    get_ranking_proveedores_pg,
    get_ranking_proveedores_resumen,
    get_proveedores_ci_vs_sgm_mensual,
    get_proveedores_ci_sin_cabecera,
    get_resumen_proveedor_connexa_vs_sgm,
    get_ventas_proveedor,
)

__all__ = [
    # uso_general
    "ensure_mon_objects",
    "ensure_forecast_views",
    "get_oc_generadas_mensual",
    "get_forecast_propuesta_conversion_mensual",
    "get_embudo_connexa_sgm_mensual",
    "get_proporcion_ci_vs_sgm_mensual",
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
]

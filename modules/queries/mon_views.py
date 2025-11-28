# modules/queries/mon_views.py
from sqlalchemy.engine import Engine

from sqlalchemy import text

# --- DDL existentes, copiados desde queries.py -----------------

DDL_VIEW_OC_GENERADAS_BASE = """..."""
DDL_VIEW_OC_GENERADAS_EXT = """..."""
DDL_VIEW_OC_GENERADAS_SUC_EXT = """..."""

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

DDL_V_FORECAST_PROPUESTA_BASE = """..."""  # igual al que ya tienen

def ensure_mon_objects(pg_engine: Engine) -> None:
    """Crea/actualiza vistas e índices mon.* necesarios para los indicadores."""
    with pg_engine.begin() as con:
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_BASE)
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_EXT)
        con.exec_driver_sql(DDL_VIEW_OC_GENERADAS_SUC_EXT)
        for stmt in IDX_OC_GENERADAS + IDX_DIM:
            con.exec_driver_sql(stmt)

def ensure_forecast_views(pg_engine: Engine) -> None:
    """Crea/actualiza la vista mon.v_forecast_propuesta_base."""
    with pg_engine.begin() as con:
        con.exec_driver_sql(DDL_V_FORECAST_PROPUESTA_BASE)

# app.py
import os
import streamlit as st

from modules.ui import render_header
from modules.db import (
    get_pg_engine,        # diarco_data (PostgreSQL)
    get_connexa_engine,   # connexa_platform_ms (PostgreSQL)
)
from modules.queries.uso_general import (
    ensure_mon_objects,   # vistas/índices mon.* en diarco_data
    ensure_forecast_views # vista mon.v_forecast_propuesta_base en connexa
)

# -------------------------------------------------------
# Configuración general de la app raíz
# -------------------------------------------------------
st.set_page_config(
    page_title="CONNEXA Monitor",
    page_icon="📊",
    layout="wide",
)

render_header(
    "CONNEXA Tool Kit — Tablero de Control · Monitoreo de Uso y Efectividad"
)

st.sidebar.success("Usen el menú de la izquierda para navegar por los indicadores.")

# -------------------------------------------------------
# Inicialización de objetos de soporte (vistas mon.*)
# -------------------------------------------------------

# 1) Objetos mon.* en diarco_data (OC generadas, etc.)
try:
    eng_diarco = get_pg_engine()
    if eng_diarco is None:
        st.warning("No se pudo obtener el engine de diarco_data (PostgreSQL).")
    else:
        ensure_mon_objects(eng_diarco)
except Exception as e:
    st.warning(
        f"No fue posible crear/actualizar objetos de soporte en diarco_data (mon.* OC): {e}"
    )

# 2) Vista mon.v_forecast_propuesta_base en connexa_platform_ms
try:
    eng_connexa = get_connexa_engine()
    if eng_connexa is None:
        st.warning("No se pudo obtener el engine de connexa_platform_ms.")
    else:
        ensure_forecast_views(eng_connexa)
except Exception as e:
    st.warning(
        f"No fue posible crear/actualizar la vista mon.v_forecast_propuesta_base en connexa: {e}"
    )

# Texto de bienvenida simple en la portada raíz
st.markdown("---")
st.markdown(
    """
### Bienvenidos al CONNEXA Monitor

Este tablero permite analizar, desde una perspectiva gerencial:

- El uso general del sistema (Forecast → Propuesta, OC Connexa vs OC SGM).
- La gestión de los compradores.
- La incorporación de proveedores al circuito Connexa.
- La efectividad de la herramienta y el pipeline de propuestas.

Seleccionen un indicador en el menú lateral para comenzar el análisis.
"""
)


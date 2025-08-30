
import os
import streamlit as st
from modules.ui import render_header, make_date_filters
from modules.db import get_pg_engine
from modules.queries import ensure_mon_objects

st.set_page_config(page_title="CONNEXA Monitor", page_icon="📊", layout="wide")

render_header("CONNEXA Monitor — Tablero de Control")

st.sidebar.success("Usen el menú de la izquierda para navegar por los indicadores.")

# Aseguramos vistas/objetos necesarios del lado CONNEXA
try:
    ensure_mon_objects(get_pg_engine())
except Exception as e:
    st.warning(f"No fue posible crear/actualizar objetos de soporte (mon.*): {e}")

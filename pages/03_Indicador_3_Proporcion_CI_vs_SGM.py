
import os
from datetime import date
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from modules.db import get_sqlserver_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_SGM_I3_MENSUAL,
    SQL_SGM_I3_SIN_CABE,
)

st.set_page_config(page_title="Indicador 3 — Proporción CI vs Total SGM", page_icon="🧮", layout="wide")
render_header("Indicador 3 — Proporción de OC (CI vs Total SGM)")

# Filtros
desde, hasta = make_date_filters()
# Clampear la fecha mínima a 2025-06-01 (inicio del CI)
min_ci = date(2025, 6, 1)
if desde < min_ci: # type: ignore
    desde = min_ci

ttl = int(os.getenv("CACHE_TTL_SECONDS","300"))

@st.cache_data(ttl=ttl)
def fetch_mensual(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_I3_MENSUAL, con, params={"desde": desde, "hasta": hasta})
    return df

@st.cache_data(ttl=ttl)
def fetch_sin_cabe(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_I3_SIN_CABE, con, params={"desde": desde, "hasta": hasta})
    return df

df = fetch_mensual(desde, hasta)

if df.empty:
    st.info("Configuren SQL Server en .env y verifiquen acceso a T874_OC_PRECARGA_KIKKER_HIST y T080_OC_CABE.")
else:
    # KPIs de rango
    total_sgm = int(df["oc_totales_sgm"].sum())
    total_ci  = int(df["oc_desde_ci"].sum())
    ratio     = (total_ci / total_sgm) if total_sgm else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("OC totales SGM (rango)", total_sgm)
    c2.metric("OC desde CI (rango)", total_ci)
    c3.metric("Proporción CI/SGM (rango)", f"{ratio:.1%}")

    # Serie mensual
    df_plot = df.copy()
    df_plot["mes"] = pd.to_datetime(df_plot["mes"])
    col1, col2 = st.columns(2)
    with col1:
        fig = go.Figure()
        fig.add_bar(x=df_plot["mes"], y=df_plot["oc_totales_sgm"], name="OC totales SGM")
        fig.add_bar(x=df_plot["mes"], y=df_plot["oc_desde_ci"],   name="OC desde CI")
        fig.update_layout(title="OC por Mes (SGM total vs Originadas en CI)", barmode="group")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig2 = px.line(df_plot, x="mes", y="proporcion_ci", title="Proporción CI vs Total SGM")
        st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    with st.expander("CI con prefijo/sufijo sin cabecera en SGM (posibles pendientes) — detalle"):
        df_miss = fetch_sin_cabe(desde, hasta)
        if df_miss.empty:
            st.success("No se detectaron KIKKER con prefijo/sufijo sin cabecera en SGM en el rango.")
        else:
            st.dataframe(df_miss, use_container_width=True, hide_index=True)
            st.download_button(
                "Descargar CSV (CI sin cabecera)",
                data=df_miss.to_csv(index=False).encode("utf-8"),
                file_name="ci_sin_cabecera.csv",
                mime="text/csv",
            )


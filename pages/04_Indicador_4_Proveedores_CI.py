
import os
from datetime import date
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from modules.db import get_sqlserver_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_SGM_I4_PROV_MENSUAL,
    SQL_SGM_I4_PROV_DETALLE,
    SQL_SGM_I4_PROV_SIN_CABE,
)

st.set_page_config(page_title="Indicador 4 — Proveedores CI", page_icon="🏭", layout="wide")
render_header("Indicador 4 — Proveedores reabastecidos con Comprador Inteligente")

# Filtros
desde, hasta = make_date_filters()
min_ci = date(2025, 6, 1)  # inicio del CI
if desde < min_ci: # type: ignore
    desde = min_ci

ttl = int(os.getenv("CACHE_TTL_SECONDS","300"))

@st.cache_data(ttl=ttl)
def fetch_mensual(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None: return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_I4_PROV_MENSUAL, con, params={"desde": desde, "hasta": hasta})
    return df

@st.cache_data(ttl=ttl)
def fetch_detalle(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None: return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_I4_PROV_DETALLE, con, params={"desde": desde, "hasta": hasta})
    return df

@st.cache_data(ttl=ttl)
def fetch_sin_cabe(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None: return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_I4_PROV_SIN_CABE, con, params={"desde": desde, "hasta": hasta})
    return df

df = fetch_mensual(desde, hasta)

if df.empty:
    st.info("Configuren SQL Server en .env y verifiquen acceso a T874_OC_PRECARGA_KIKKER_HIST y T080_OC_CABE.")
else:
    # KPIs de rango (distintos proveedores)
    dfd = fetch_detalle(desde, hasta)
    prov_ci = dfd["c_proveedor"].dropna().nunique() if not dfd.empty else 0

    # Para el denominador de rango (proveedores totales SGM) sumamos por mes,
    # pero evitamos doble conteo acumulando en un set desde el detalle si se requiere precisión.
    # Aquí mostramos un KPI simple basado en la suma mensual para referencia visual.
    total_ci_mes = int(df["prov_desde_ci"].sum())
    total_sgm_mes = int(df["prov_totales_sgm"].sum())

    colk1, colk2, colk3 = st.columns(3)
    colk1.metric("Proveedores CI → SGM (rango, distintos)", prov_ci)
    colk2.metric("Suma mensual — Proveedores CI", total_ci_mes)
    colk3.metric("Suma mensual — Proveedores totales SGM", total_sgm_mes)

    # Serie mensual: barras y línea de proporción
    df_plot = df.copy()
    df_plot["mes"] = pd.to_datetime(df_plot["mes"])
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure()
        fig.add_bar(x=df_plot["mes"], y=df_plot["prov_totales_sgm"], name="Proveedores totales SGM")
        fig.add_bar(x=df_plot["mes"], y=df_plot["prov_desde_ci"],   name="Proveedores CI → SGM")
        fig.update_layout(title="Proveedores por Mes (SGM total vs desde CI)", barmode="group")
        st.plotly_chart(fig, width="stretch")
    with c2:
        fig2 = px.line(df_plot, x="mes", y="proporcion_ci_prov", title="Proporción de Proveedores CI vs Totales SGM")
        st.plotly_chart(fig2, width="stretch")

    st.dataframe(df, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Ranking de Proveedores CI (por #OC y Bultos)")
    if dfd.empty:
        st.warning("Sin detalle para calcular ranking en el rango seleccionado.")
    else:
        rk = (
            dfd.groupby("c_proveedor", dropna=False)
                .agg(oc_distintas=("oc_sgm","nunique"), bultos_total=("q_bultos_ci","sum"))
                .sort_values(["oc_distintas","bultos_total"], ascending=[False, False])
                .reset_index()
        )
        colr1, colr2 = st.columns([2,1])
        with colr1:
            figr = px.bar(rk.head(20).sort_values("oc_distintas"),
                            x="oc_distintas", y="c_proveedor", orientation="h",
                            title="Top Proveedores por #OC CI → SGM")
            st.plotly_chart(figr, width="stretch")
        with colr2:
            st.dataframe(rk.head(20), width="stretch", hide_index=True)
            st.download_button(
                "Descargar Ranking (CSV)",
                data=rk.to_csv(index=False).encode("utf-8"),
                file_name="ranking_proveedores_ci.csv",
                mime="text/csv",
            )

    st.divider()
    with st.expander("Proveedores CI con prefijo/sufijo sin cabecera en SGM (posibles pendientes)"):
        miss = fetch_sin_cabe(desde, hasta)
        if miss.empty:
            st.success("No se detectaron proveedores CI sin cabecera en SGM en el rango.")
        else:
            st.dataframe(miss, width="stretch", hide_index=True)
            st.download_button(
                "Descargar CSV (pendientes)",
                data=miss.to_csv(index=False).encode("utf-8"),
                file_name="proveedores_ci_sin_cabecera.csv",
                mime="text/csv",
            )

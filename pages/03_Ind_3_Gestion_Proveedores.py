# pages/03_Indicador_Gestion_Proveedores.py
# Indicador 3 — Gestión de Proveedores (Connexa ↔ SGM)

import os
from datetime import date
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import (
    get_pg_engine,        # diarco_data
    get_sqlserver_engine  # SGM
)
from modules.ui import render_header, make_date_filters

from modules.queries.proveedores import (
    get_ranking_proveedores_pg,
    get_ranking_proveedores_resumen,
    get_proporcion_proveedores_ci_mensual,
    get_detalle_proveedores_ci,
    get_proveedores_ci_sin_cabecera,
    get_resumen_proveedor_connexa_vs_sgm,
)

# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 3 — Gestión de Proveedores",
    page_icon="🏭",
    layout="wide",
)

render_header("Indicador 3 — Gestión de Proveedores")

# Filtros de fecha comunes
desde, hasta = make_date_filters()

ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# Motores de base de datos
pg_diarco = get_pg_engine()
eng_sgm   = get_sqlserver_engine()

# =======================================================
# Funciones auxiliares / caché
# =======================================================

@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_proporcion_proveedores_ci(desde: date, hasta: date) -> pd.DataFrame:
    if eng_sgm is None:
        return pd.DataFrame()
    return get_proporcion_proveedores_ci_mensual(eng_sgm, desde, hasta)


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_resumen_proveedores_connexa_vs_sgm(desde: date, hasta: date) -> pd.DataFrame:
    if pg_diarco is None or eng_sgm is None:
        return pd.DataFrame()
    return get_resumen_proveedor_connexa_vs_sgm(pg_diarco, eng_sgm, desde, hasta)


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_ranking_proveedores_connexa(desde: date, hasta: date, topn: int = 10) -> pd.DataFrame:
    if pg_diarco is None:
        return pd.DataFrame()
    return get_ranking_proveedores_pg(pg_diarco, desde, hasta, topn=topn)


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_ranking_proveedores_ci(desde: date, hasta: date, topn: int = 10) -> pd.DataFrame:
    if eng_sgm is None:
        return pd.DataFrame()
    return get_ranking_proveedores_resumen(eng_sgm, desde, hasta, topn=topn)


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_detalle_proveedores_ci(desde: date, hasta: date) -> pd.DataFrame:
    if eng_sgm is None:
        return pd.DataFrame()
    return get_detalle_proveedores_ci(eng_sgm, desde, hasta)


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_proveedores_ci_sin_cabecera(desde: date, hasta: date) -> pd.DataFrame:
    if eng_sgm is None:
        return pd.DataFrame()
    return get_proveedores_ci_sin_cabecera(eng_sgm, desde, hasta)


# =======================================================
# 1. Resumen ejecutivo de gestión de proveedores
# =======================================================

st.markdown("## 1. Resumen ejecutivo de gestión de proveedores")

df_prop = _fetch_proporcion_proveedores_ci(desde, hasta)
df_res  = _fetch_resumen_proveedores_connexa_vs_sgm(desde, hasta)

if df_prop.empty and df_res.empty:
    st.info("No se encontraron datos de proveedores para el rango seleccionado.")
else:
    # KPIs globales a partir de df_prop (proporción mensual) y df_res (resumen por proveedor)
    total_prov_sgm = int(df_prop["prov_totales_sgm"].sum()) if not df_prop.empty else 0
    total_prov_ci  = int(df_prop["prov_desde_ci"].sum()) if not df_prop.empty else 0
    cobertura_ci   = (
        (total_prov_ci / total_prov_sgm) * 100
        if total_prov_sgm > 0 else 0.0
    )

    total_bultos_connexa = 0.0
    total_bultos_sgm     = 0.0
    if not df_res.empty:
        if "bultos_connexa" in df_res.columns:
            total_bultos_connexa = float(df_res["bultos_connexa"].sum())
        if "bultos_sgm" in df_res.columns:
            total_bultos_sgm = float(df_res["bultos_sgm"].sum())

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Proveedores con OC en SGM (rango)", value=total_prov_sgm)
    with col2:
        st.metric("Proveedores gestionados vía CONNEXA (rango)", value=total_prov_ci)
    with col3:
        st.metric("Cobertura proveedores vía CONNEXA", value=f"{cobertura_ci:,.1f} %")
    with col4:
        st.metric(
            "Bultos Connexa vs SGM (rango)",
            value=f"{total_bultos_connexa:,.0f} / {total_bultos_sgm:,.0f}"
        )

    # Serie mensual de proporción de proveedores CI sobre total SGM
    if not df_prop.empty:
        df_prop_plot = df_prop.copy()
        df_prop_plot["mes"] = pd.to_datetime(df_prop_plot["mes"])
        df_prop_plot["proporcion_pct"] = df_prop_plot["proporcion_ci_prov"] * 100

        fig_prop = px.line(
            df_prop_plot,
            x="mes",
            y="proporcion_pct",
            markers=True,
            title="% de proveedores con OC SGM gestionados vía CONNEXA (mensual)",
        )
        fig_prop.update_layout(
            xaxis_title="Mes",
            yaxis_title="Proporción de proveedores (%)",
        )
        st.plotly_chart(fig_prop, use_container_width=True)

        with st.expander("Detalle mensual de proporción de proveedores CI vs SGM"):
            st.dataframe(df_prop_plot, use_container_width=True, hide_index=True)

# =======================================================
# 2. Ranking de proveedores (volumen y adopción)
# =======================================================

st.markdown("---")
st.markdown("## 2. Ranking de proveedores (Connexa y CI→SGM)")

col_left, col_right = st.columns(2)

with col_left:
    st.markdown("### 2.1 Top proveedores por OC y bultos desde Connexa (PostgreSQL)")
    df_rk_pg = _fetch_ranking_proveedores_connexa(desde, hasta, topn=10)

    if df_rk_pg.empty:
        st.info("Sin datos de ranking de proveedores Connexa para el rango seleccionado.")
    else:
        df_plot = df_rk_pg.copy()
        df_plot = df_plot.sort_values("bultos_total", ascending=False)

        fig_pg = px.bar(
            df_plot,
            x="bultos_total",
            y="proveedor",
            orientation="h",
            title="Top 10 proveedores por bultos desde Connexa",
            text="bultos_total",
        )
        fig_pg.update_layout(
            xaxis_title="Bultos Connexa",
            yaxis_title="Proveedor (Connexa)",
        )
        st.plotly_chart(fig_pg, use_container_width=True)

        with st.expander("Ver tabla completa — Ranking Connexa"):
            st.dataframe(df_plot, use_container_width=True, hide_index=True)

with col_right:
    st.markdown("### 2.2 Top proveedores por bultos en OC SGM originadas en CONNEXA (CI)")
    df_rk_ci = _fetch_ranking_proveedores_ci(desde, hasta, topn=10)

    if df_rk_ci.empty:
        st.info("Sin datos de proveedores CI → SGM para el rango seleccionado.")
    else:
        df_plot_ci = df_rk_ci.copy()
        df_plot_ci = df_plot_ci.sort_values("bultos_total", ascending=False)

        fig_ci = px.bar(
            df_plot_ci,
            x="bultos_total",
            y="label",
            orientation="h",
            title="Top 10 proveedores por bultos en OC SGM desde CI",
            text="bultos_total",
        )
        fig_ci.update_layout(
            xaxis_title="Bultos en OC SGM (desde CI)",
            yaxis_title="Proveedor (c_proveedor)",
        )
        st.plotly_chart(fig_ci, use_container_width=True)

        with st.expander("Ver tabla completa — Ranking proveedores CI → SGM"):
            st.dataframe(df_plot_ci, use_container_width=True, hide_index=True)

# =======================================================
# 3. Resumen Connexa vs SGM por proveedor
# =======================================================

st.markdown("---")
st.markdown("## 3. Resumen Connexa vs SGM por proveedor")

if df_res.empty:
    st.info("No se encontró información consolidada Connexa vs SGM para el rango seleccionado.")
else:
    df_res_plot = df_res.copy()

    # Métrica simple: top proveedores según bultos_totales (Connexa + SGM)
    df_res_plot["bultos_totales"] = df_res_plot.get("bultos_connexa", 0) + df_res_plot.get("bultos_sgm", 0)
    df_res_plot = df_res_plot.sort_values("bultos_totales", ascending=False)

    # Para no saturar el gráfico, se limita a top 15
    df_top = df_res_plot.head(15)

    # Se arma etiqueta proveedor por código
    df_top["label"] = df_top["c_proveedor"].astype("Int64").astype(str)

    fig_stack = px.bar(
        df_top,
        x="label",
        y=["bultos_connexa", "bultos_sgm"],
        barmode="group",
        title="Connexa vs SGM — Bultos por proveedor (Top 15)",
    )
    fig_stack.update_layout(
        xaxis_title="Proveedor (c_proveedor)",
        yaxis_title="Bultos",
        legend_title="Origen",
    )
    st.plotly_chart(fig_stack, use_container_width=True)

    with st.expander("Ver detalle completo Connexa vs SGM por proveedor"):
        st.dataframe(df_res_plot, use_container_width=True, hide_index=True)

# =======================================================
# 4. Controles e inconsistencias (diagnóstico)
# =======================================================

st.markdown("---")
st.markdown("## 4. Controles e inconsistencias (integración CI ↔ SGM)")

col_a, col_b = st.columns(2)

with col_a:
    st.markdown("### 4.1 Proveedores presentes en CI sin cabecera en SGM")
    df_sin_cab = _fetch_proveedores_ci_sin_cabecera(desde, hasta)

    if df_sin_cab.empty:
        st.success("No se detectaron proveedores en CI sin cabecera T080 en el rango seleccionado.")
    else:
        st.warning("Se detectaron proveedores en CI sin cabecera T080 asociada en SGM.")
        st.dataframe(df_sin_cab, use_container_width=True, hide_index=True)

with col_b:
    st.markdown("### 4.2 Detalle de movimientos CI → SGM por proveedor")
    df_det_ci = _fetch_detalle_proveedores_ci(desde, hasta)

    if df_det_ci.empty:
        st.info("No se encontraron movimientos CI → SGM para el rango seleccionado.")
    else:
        st.dataframe(df_det_ci, use_container_width=True, hide_index=True)
# =======================================================
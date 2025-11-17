# -*- coding: utf-8 -*-
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from modules.db import get_connexa_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_FP_CONVERSION_MENSUAL,
    SQL_FP_MENSUAL_COMPRADOR,
    SQL_FP_RANKING_COMPRADOR,
    SQL_FP_ESTADOS_PROP,
    SQL_FP_DETALLE,
    ensure_forecast_views,
)

st.set_page_config(
    page_title="Indicador 6 — Forecast → Propuesta",
    page_icon="🧠",
    layout="wide",
)

render_header("Indicador 6 — Forecast → Propuesta (productividad por comprador)")

# -------------------------------------------------------
# Filtros
# -------------------------------------------------------
desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


@st.cache_data(ttl=ttl)
def bootstrap_and_fetch_conv(desde, hasta) -> pd.DataFrame:
    """
    Serie mensual de ejecuciones de forecast vs propuestas generadas.
    Se apoya en mon.v_forecast_propuesta_base (ensure_forecast_views).
    """
    eng = get_connexa_engine()
    ensure_forecast_views(eng)  # crea/actualiza la vista en connexa_platform_ms
    with eng.connect() as con:
        conv = pd.read_sql(
            SQL_FP_CONVERSION_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    return conv


@st.cache_data(ttl=ttl)
def fetch_mensual_comprador(desde, hasta) -> pd.DataFrame:
    """
    Productividad mensual por comprador:
    #propuestas, monto, P50/P90 de tiempos, etc.
    """
    eng = get_connexa_engine()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_FP_MENSUAL_COMPRADOR,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    return df


@st.cache_data(ttl=ttl)
def fetch_ranking(desde, hasta, topn: int = 15) -> pd.DataFrame:
    """
    Ranking de compradores (rango) por #propuestas y tiempos.
    """
    eng = get_connexa_engine()
    with eng.connect() as con:
        rk = pd.read_sql(
            SQL_FP_RANKING_COMPRADOR,
            con,
            params={"desde": desde, "hasta": hasta, "topn": topn},
        )
    return rk


@st.cache_data(ttl=ttl)
def fetch_estados(desde, hasta) -> pd.DataFrame:
    """
    Distribución de estados de propuestas en el rango.
    """
    eng = get_connexa_engine()
    with eng.connect() as con:
        est = pd.read_sql(
            SQL_FP_ESTADOS_PROP,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    return est


@st.cache_data(ttl=ttl)
def fetch_detalle(desde, hasta) -> pd.DataFrame:
    """
    Detalle granular para exportación / análisis fino.
    """
    eng = get_connexa_engine()
    with eng.connect() as con:
        det = pd.read_sql(
            SQL_FP_DETALLE,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    return det


# -------------------------------------------------------
# Dataframes base
# -------------------------------------------------------
conv = bootstrap_and_fetch_conv(desde, hasta)
mensual = fetch_mensual_comprador(desde, hasta)
rk = fetch_ranking(desde, hasta)
est = fetch_estados(desde, hasta)
det = fetch_detalle(desde, hasta)

# -------------------------------------------------------
# KPIs del rango
# -------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)

total_ejec = int(conv["ejecuciones"].sum()) if not conv.empty else 0
total_prop = int(conv["propuestas"].sum()) if not conv.empty else 0
conversion = (total_prop / total_ejec) if total_ejec else 0.0

c1.metric("Forecasts completados (rango)", total_ejec)
c2.metric("Propuestas generadas (rango)", total_prop)
c3.metric("Conversión (rango)", f"{conversion:.1%}")

if not mensual.empty:
    p50_adj = mensual["p50_ajuste_min"].median()
    p50_lead = mensual["p50_lead_min"].median()
else:
    p50_adj = 0
    p50_lead = 0

c4.metric("Mediana ajuste (min)", f"{p50_adj:.0f}")

# -------------------------------------------------------
# Serie mensual: ejecuciones, propuestas, conversión
# -------------------------------------------------------
st.subheader("Serie mensual — Ejecuciones, Propuestas y Conversión")

if conv.empty:
    st.info("Sin datos en el rango seleccionado.")
else:
    conv_plot = conv.copy()
    conv_plot["mes"] = pd.to_datetime(conv_plot["mes"])

    fig = go.Figure()
    fig.add_bar(
        x=conv_plot["mes"],
        y=conv_plot["ejecuciones"],
        name="Forecasts completados",
    )
    fig.add_bar(
        x=conv_plot["mes"],
        y=conv_plot["propuestas"],
        name="Propuestas",
    )

    fig.update_layout(
        barmode="group",
        xaxis_title="Mes",
        yaxis_title="Cantidad",
        legend_title="Serie",
        title="Ejecuciones vs Propuestas (mensual)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Conversión como línea sobre un eje secundario
    fig2 = go.Figure()
    fig2.add_bar(
        x=conv_plot["mes"],
        y=conv_plot["ejecuciones"],
        name="Forecasts completados",
        yaxis="y1",
    )
    fig2.add_bar(
        x=conv_plot["mes"],
        y=conv_plot["propuestas"],
        name="Propuestas",
        yaxis="y1",
    )
    fig2.add_trace(
        go.Scatter(
            x=conv_plot["mes"],
            y=conv_plot["conversion"] * 100,
            name="Conversión (%)",
            mode="lines+markers",
            yaxis="y2",
        )
    )

    fig2.update_layout(
        title="Conversión Forecast → Propuesta (mensual)",
        xaxis=dict(title="Mes"),
        yaxis=dict(title="Cantidad", side="left"),
        yaxis2=dict(
            title="Conversión (%)",
            side="right",
            overlaying="y",
        ),
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# -------------------------------------------------------
# Productividad por comprador (mensual)
# -------------------------------------------------------
st.subheader("Productividad por comprador (mensual)")

if mensual.empty:
    st.info("Sin propuestas asociadas a comprador en el rango.")
else:
    mensual["mes"] = pd.to_datetime(mensual["mes"])

    col_a, col_b = st.columns(2)

    with col_a:
        fig_c = px.bar(
            mensual,
            x="mes",
            y="propuestas",
            color="comprador",
            title="#Propuestas por comprador y mes",
            barmode="stack",
        )
        st.plotly_chart(fig_c, use_container_width=True)

    with col_b:
        fig_t = px.box(
            mensual,
            x="comprador",
            y="p50_ajuste_min",
            title="Dispersión P50 de ajuste (min) por comprador",
        )
        st.plotly_chart(fig_t, use_container_width=True)

st.divider()

# -------------------------------------------------------
# Ranking de compradores
# -------------------------------------------------------
st.subheader("Ranking de compradores (rango)")

topn = st.slider("Top N compradores", min_value=5, max_value=30, value=15, step=5)
rk = fetch_ranking(desde, hasta, topn=topn)

if rk.empty:
    st.info("Sin datos para ranking en el rango.")
else:
    fig_r = px.bar(
        rk.sort_values("propuestas"),
        x="propuestas",
        y="comprador",
        orientation="h",
        title=f"Top {topn} por #Propuestas",
    )
    st.plotly_chart(fig_r, use_container_width=True)

    st.dataframe(rk, use_container_width=True, hide_index=True)
    st.download_button(
        "Descargar ranking (CSV)",
        rk.to_csv(index=False).encode("utf-8"),
        file_name="ranking_compradores_forecast_propuesta.csv",
        mime="text/csv",
    )

st.divider()

# -------------------------------------------------------
# Estados de propuestas
# -------------------------------------------------------
st.subheader("Estados de propuestas (rango)")

if est.empty:
    st.info("Sin estados para mostrar.")
else:
    fig_e = px.pie(
        est,
        names="pp_status",
        values="propuestas",
        title="Distribución de estados",
    )
    st.plotly_chart(fig_e, use_container_width=True)
    st.dataframe(est, use_container_width=True, hide_index=True)

st.divider()

# -------------------------------------------------------
# Detalle y exportación
# -------------------------------------------------------
with st.expander("Detalle (exportable)"):
    st.dataframe(det, use_container_width=True, hide_index=True)
    st.download_button(
        "Descargar detalle (CSV)",
        det.to_csv(index=False).encode("utf-8"),
        file_name="detalle_forecast_propuesta.csv",
        mime="text/csv",
    )

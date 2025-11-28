# pages/04_Indicador_Efectividad.py
# Indicador 4 — Efectividad Forecast → Propuesta
# Este Indicador 4 organiza la efectividad de la herramienta en torno a tres preguntas gerenciales:

# ¿Cuánto se está usando el motor de forecast y cuántas propuestas se generan efectivamente?
# ¿Cómo evoluciona en el tiempo la tasa de conversión Forecast → Propuesta?
# ¿Qué compradores son más activos y cómo son sus tiempos de trabajo sobre las propuestas?

from datetime import date
import os

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import (
    get_connexa_engine,   # connexa_platform_ms (motor supply_planning)
)
from modules.ui import render_header, make_date_filters

from modules.queries.uso_general import (
    get_forecast_propuesta_conversion_mensual,
)

from modules.queries.compradores import (
    get_productividad_comprador_mensual,
    get_ranking_comprador_forecast,
)

# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 4 — Efectividad Forecast → Propuesta",
    page_icon="📈",
    layout="wide",
)

render_header("Indicador 4 — Efectividad Forecast → Propuesta")

# Filtros de fecha comunes
desde, hasta = make_date_filters()

ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# Motor Connexa (supply_planning.*)
eng_connexa = get_connexa_engine()

# =======================================================
# Helpers de caché
# =======================================================

@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_fp_conversion(desde: date, hasta: date) -> pd.DataFrame:
    """
    Serie mensual de ejecuciones de forecast vs propuestas y tasa de conversión.
    """
    if eng_connexa is None:
        return pd.DataFrame()
    df = get_forecast_propuesta_conversion_mensual(eng_connexa, desde, hasta)
    if df.empty:
        return df

    # Normalización ligera
    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"])

    for c in ("ejecuciones", "propuestas"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("Int64")

    if "conversion" in df.columns:
        df["conversion"] = pd.to_numeric(df["conversion"], errors="coerce").fillna(0.0)

    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_productividad_mensual(desde: date, hasta: date) -> pd.DataFrame:
    """
    Productividad por comprador (mensual) a partir de mon.v_forecast_propuesta_base.
    """
    if eng_connexa is None:
        return pd.DataFrame()
    df = get_productividad_comprador_mensual(eng_connexa, desde, hasta)
    if df.empty:
        return df

    # Limpieza de tipos
    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"])

    for col in ("propuestas", "monto_total"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    for col in ("p50_ajuste_min", "p90_ajuste_min", "p50_lead_min", "avg_exec_min"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_ranking_comprador_fp(desde: date, hasta: date, topn: int = 10) -> pd.DataFrame:
    """
    Ranking de compradores por propuestas y tiempos de ajuste.
    """
    if eng_connexa is None:
        return pd.DataFrame()
    df = get_ranking_comprador_forecast(eng_connexa, desde, hasta, topn=topn)
    if df.empty:
        return df

    for col in ("propuestas", "monto_total", "p50_ajuste_min", "p90_ajuste_min"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =======================================================
# 1. Resumen ejecutivo de efectividad
# =======================================================

st.markdown("## 1. Resumen ejecutivo de efectividad Forecast → Propuesta")

df_conv = _fetch_fp_conversion(desde, hasta)
df_prod = _fetch_productividad_mensual(desde, hasta)

if df_conv.empty:
    st.info("No se encontraron ejecuciones de forecast ni propuestas en el rango seleccionado.")
else:
    total_ejec = int(df_conv["ejecuciones"].sum())
    total_prop = int(df_conv["propuestas"].sum())
    tasa_global = (total_prop / total_ejec) * 100 if total_ejec > 0 else 0.0

    # Tiempos globales (si hay datos de productividad)
    if not df_prod.empty:
        # Se calculan sobre todas las filas del rango
        p50_ajuste_glob = df_prod["p50_ajuste_min"].median(skipna=True) if "p50_ajuste_min" in df_prod.columns else None
        p90_ajuste_glob = df_prod["p90_ajuste_min"].median(skipna=True) if "p90_ajuste_min" in df_prod.columns else None
        p50_lead_glob   = df_prod["p50_lead_min"].median(skipna=True) if "p50_lead_min" in df_prod.columns else None
        avg_exec_glob   = df_prod["avg_exec_min"].mean(skipna=True)    if "avg_exec_min"   in df_prod.columns else None
    else:
        p50_ajuste_glob = p90_ajuste_glob = p50_lead_glob = avg_exec_glob = None

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Ejecuciones de forecast (rango)", value=total_ejec)
    with col2:
        st.metric("Propuestas generadas (rango)", value=total_prop)
    with col3:
        st.metric("Tasa global Forecast → Propuesta", value=f"{tasa_global:,.1f} %")

    col4, col5, col6, col7 = st.columns(4)
    with col4:
        st.metric(
            "P50 tiempo de ajuste (min)",
            value=f"{p50_ajuste_glob:,.1f}" if p50_ajuste_glob is not None else "—"
        )
    with col5:
        st.metric(
            "P90 tiempo de ajuste (min)",
            value=f"{p90_ajuste_glob:,.1f}" if p90_ajuste_glob is not None else "—"
        )
    with col6:
        st.metric(
            "P50 lead time apertura propuesta (min)",
            value=f"{p50_lead_glob:,.1f}" if p50_lead_glob is not None else "—"
        )
    with col7:
        st.metric(
            "Tiempo promedio de ejecución forecast (min)",
            value=f"{avg_exec_glob:,.1f}" if avg_exec_glob is not None else "—"
        )

# =======================================================
# 2. Evolución mensual de conversión
# =======================================================

st.markdown("---")
st.markdown("## 2. Evolución mensual de ejecuciones, propuestas y tasa de conversión")

if df_conv.empty:
    st.info("Sin datos de conversión para graficar en el rango seleccionado.")
else:
    df_plot = df_conv.copy()
    df_plot["mes"] = pd.to_datetime(df_plot["mes"])

    # Gráfico 1: ejecuciones vs propuestas
    fig_counts = px.bar(
        df_plot,
        x="mes",
        y=["ejecuciones", "propuestas"],
        barmode="group",
        title="Ejecuciones de forecast vs Propuestas generadas (mensual)",
    )
    fig_counts.update_layout(
        xaxis_title="Mes",
        yaxis_title="Cantidad",
        legend_title="Serie",
    )
    st.plotly_chart(fig_counts, use_container_width=True)

    # Gráfico 2: tasa de conversión
    df_plot["conversion_pct"] = df_plot["conversion"] * 100.0

    fig_conv = px.line(
        df_plot,
        x="mes",
        y="conversion_pct",
        markers=True,
        title="Tasa de conversión Forecast → Propuesta (mensual, %)",
    )
    fig_conv.update_layout(
        xaxis_title="Mes",
        yaxis_title="Conversión (%)",
    )
    st.plotly_chart(fig_conv, use_container_width=True)

    with st.expander("Detalle mensual de conversión Forecast → Propuesta"):
        st.dataframe(df_plot, use_container_width=True, hide_index=True)

# =======================================================
# 3. Productividad por comprador (mensual)
# =======================================================

st.markdown("---")
st.markdown("## 3. Productividad por comprador (mensual)")

if df_prod.empty:
    st.info("No se encontraron propuestas asociadas a compradores en el rango seleccionado.")
else:
    # Filtro opcional por comprador
    compradores_disponibles = sorted(df_prod["comprador"].dropna().astype(str).unique())
    comprador_sel = st.selectbox(
        "Filtrar por comprador (opcional)",
        options=["(Todos)"] + compradores_disponibles,
        index=0,
    )

    df_prod_plot = df_prod.copy()
    if comprador_sel != "(Todos)":
        df_prod_plot = df_prod_plot[df_prod_plot["comprador"].astype(str) == comprador_sel]

    # Gráfico: propuestas mensuales por comprador
    fig_prop_comp = px.bar(
        df_prod_plot,
        x="mes",
        y="propuestas",
        color="comprador",
        title="Propuestas generadas por comprador (mensual)",
    )
    fig_prop_comp.update_layout(
        xaxis_title="Mes",
        yaxis_title="# Propuestas",
        legend_title="Comprador",
    )
    st.plotly_chart(fig_prop_comp, use_container_width=True)

    # Gráfico: tiempos P50/P90 de ajuste por comprador/mes
    if {"p50_ajuste_min", "p90_ajuste_min"}.issubset(df_prod_plot.columns):
        fig_tiempos = px.line(
            df_prod_plot,
            x="mes",
            y=["p50_ajuste_min", "p90_ajuste_min"],
            color_discrete_sequence=px.colors.qualitative.Set1,
            title="Tiempos de ajuste P50 / P90 (por mes)",
        )
        fig_tiempos.update_layout(
            xaxis_title="Mes",
            yaxis_title="Minutos",
            legend_title="Percentil",
        )
        st.plotly_chart(fig_tiempos, use_container_width=True)

    with st.expander("Ver tabla completa de productividad por comprador (mensual)"):
        st.dataframe(df_prod_plot, use_container_width=True, hide_index=True)

# =======================================================
# 4. Ranking de compradores por efectividad
# =======================================================

st.markdown("---")
st.markdown("## 4. Ranking de compradores por efectividad")

df_rk = _fetch_ranking_comprador_fp(desde, hasta, topn=10)

if df_rk.empty:
    st.info("No se encontraron datos para el ranking de compradores en el rango seleccionado.")
else:
    df_rk_plot = df_rk.copy()

    # Orden por #propuestas y monto total
    if "propuestas" in df_rk_plot.columns:
        df_rk_plot = df_rk_plot.sort_values(
            ["propuestas", "monto_total"],
            ascending=[False, False]
        )

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("### 4.1 Top compradores por #propuestas")
        fig_rk_prop = px.bar(
            df_rk_plot,
            x="propuestas",
            y="comprador",
            orientation="h",
            title="Top compradores por propuestas generadas",
            text="propuestas",
        )
        fig_rk_prop.update_layout(
            xaxis_title="# Propuestas",
            yaxis_title="Comprador",
        )
        st.plotly_chart(fig_rk_prop, use_container_width=True)

    with col_right:
        if {"p50_ajuste_min", "p90_ajuste_min"}.issubset(df_rk_plot.columns):
            st.markdown("### 4.2 Tiempos de ajuste por comprador (P50 / P90)")
            df_rk_time = df_rk_plot.copy()

            # Se derrite para graficar P50 y P90 lado a lado
            df_melt = df_rk_time.melt(
                id_vars=["comprador"],
                value_vars=["p50_ajuste_min", "p90_ajuste_min"],
                var_name="percentil",
                value_name="minutos_ajuste",
            )

            fig_rk_time = px.bar(
                df_melt,
                x="minutos_ajuste",
                y="comprador",
                color="percentil",
                barmode="group",
                orientation="h",
                title="Tiempos P50 / P90 de ajuste por comprador",
            )
            fig_rk_time.update_layout(
                xaxis_title="Minutos de ajuste",
                yaxis_title="Comprador",
                legend_title="Percentil",
            )
            st.plotly_chart(fig_rk_time, use_container_width=True)
        else:
            st.info("No se encontraron columnas de tiempos de ajuste (P50/P90) en el ranking.")

    with st.expander("Ver detalle completo del ranking de compradores"):
        st.dataframe(df_rk_plot, use_container_width=True, hide_index=True)
# =======================================================
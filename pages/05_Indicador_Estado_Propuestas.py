# pages/05_Indicador_Estado_Propuestas.py
# Indicador 5 — Estado y pipeline de propuestas (Forecast → Propuesta)

from datetime import date
import os

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_connexa_engine
from modules.ui import render_header, make_date_filters

from modules.queries.uso_general import (
    get_forecast_propuesta_estados,
    get_forecast_propuesta_detalle,
)

# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 5 — Estado de Propuestas",
    page_icon="🧮",
    layout="wide",
)

render_header("Indicador 5 — Estado y Pipeline de Propuestas")

# Filtros de fecha
desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

eng_connexa = get_connexa_engine()

# =======================================================
# Helpers de caché
# =======================================================

@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_estados(desde: date, hasta: date) -> pd.DataFrame:
    if eng_connexa is None:
        return pd.DataFrame()
    return get_forecast_propuesta_estados(eng_connexa, desde, hasta)


@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_detalle(desde: date, hasta: date) -> pd.DataFrame:
    if eng_connexa is None:
        return pd.DataFrame()
    return get_forecast_propuesta_detalle(eng_connexa, desde, hasta)


df_est = _fetch_estados(desde, hasta)
df_det = _fetch_detalle(desde, hasta)

# =======================================================
# 1. Resumen ejecutivo de pipeline
# =======================================================

st.markdown("## 1. Resumen ejecutivo del pipeline de propuestas")

if df_est.empty and df_det.empty:
    st.info("No se encontraron propuestas en el rango seleccionado.")
else:
    total_prop = int(df_est["propuestas"].sum()) if not df_est.empty else 0

    if not df_det.empty and "pp_closed_at" in df_det.columns:
        abiertas = int(df_det["pp_closed_at"].isna().sum())
        cerradas = int(df_det["pp_closed_at"].notna().sum())
    else:
        abiertas = cerradas = 0

    pct_cerradas = (cerradas / total_prop * 100) if total_prop > 0 else 0.0
    pct_abiertas = (abiertas / total_prop * 100) if total_prop > 0 else 0.0

    # Métricas generales
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Propuestas totales (rango)", value=total_prop)
    with col2:
        st.metric("Propuestas abiertas", value=abiertas, delta=f"{pct_abiertas:,.1f} % del total")
    with col3:
        st.metric("Propuestas cerradas", value=cerradas, delta=f"{pct_cerradas:,.1f} % del total")

    # Tiempos globales (si existieran)
    if not df_det.empty:
        avg_ajuste = df_det["adjust_time_min"].mean(skipna=True) if "adjust_time_min" in df_det.columns else None
        avg_lead   = df_det["lead_open_min"].mean(skipna=True)   if "lead_open_min"   in df_det.columns else None
        avg_exec   = df_det["exec_time_min"].mean(skipna=True)   if "exec_time_min"   in df_det.columns else None
    else:
        avg_ajuste = avg_lead = avg_exec = None

    col4, col5, col6 = st.columns(3)
    with col4:
        st.metric(
            "Tiempo promedio de ajuste (min)",
            value=f"{avg_ajuste:,.1f}" if avg_ajuste is not None else "—",
        )
    with col5:
        st.metric(
            "Lead time promedio apertura (min)",
            value=f"{avg_lead:,.1f}" if avg_lead is not None else "—",
        )
    with col6:
        st.metric(
            "Tiempo promedio ejecución forecast (min)",
            value=f"{avg_exec:,.1f}" if avg_exec is not None else "—",
        )

# =======================================================
# 2. Distribución de estados de propuestas
# =======================================================

st.markdown("---")
st.markdown("## 2. Distribución de estados de propuestas")

if df_est.empty:
    st.info("No se encontraron estados de propuestas para el rango seleccionado.")
else:
    df_est_plot = df_est.copy()

    fig_est = px.bar(
        df_est_plot,
        x="pp_status",
        y="propuestas",
        title="Propuestas por estado en el rango",
        text="propuestas",
    )
    fig_est.update_layout(
        xaxis_title="Estado de la propuesta",
        yaxis_title="Cantidad de propuestas",
    )
    st.plotly_chart(fig_est, use_container_width=True)

    with st.expander("Ver tabla de estados"):
        st.dataframe(df_est_plot, use_container_width=True, hide_index=True)

# =======================================================
# 3. Evolución temporal: abiertas vs cerradas
# =======================================================

st.markdown("---")
st.markdown("## 3. Evolución mensual de propuestas abiertas vs cerradas")

if df_det.empty:
    st.info("No se encontraron propuestas para construir la serie temporal.")
else:
    df_t = df_det.copy()
    if "base_ts" not in df_t.columns:
        st.warning("No se encontró la columna base_ts en el detalle de propuestas.")
    else:
        df_t["mes"] = pd.to_datetime(df_t["base_ts"]).dt.to_period("M").dt.to_timestamp()

        # Clasificación abierta/cerrada según pp_closed_at
        df_t["estado_apertura"] = df_t["pp_closed_at"].apply(
            lambda v: "Abierta" if pd.isna(v) else "Cerrada"
        )

        df_month = (
            df_t.groupby(["mes", "estado_apertura"], dropna=False)
                .agg(propuestas=("pp_id", "nunique"))
                .reset_index()
        )

        fig_month = px.bar(
            df_month,
            x="mes",
            y="propuestas",
            color="estado_apertura",
            barmode="group",
            title="Propuestas abiertas vs cerradas (mensual)",
        )
        fig_month.update_layout(
            xaxis_title="Mes",
            yaxis_title="Cantidad de propuestas",
            legend_title="Estado",
        )
        st.plotly_chart(fig_month, use_container_width=True)

        with st.expander("Ver tabla mensual de abiertas/cerradas"):
            st.dataframe(df_month, use_container_width=True, hide_index=True)

# =======================================================
# 4. Detalle con filtros (estado y comprador)
# =======================================================

st.markdown("---")
st.markdown("## 4. Detalle de propuestas con filtros gerenciales")

if df_det.empty:
    st.info("No hay detalle de propuestas para mostrar.")
else:
    df_d = df_det.copy()

    # Opciones de filtros
    estados_disp = sorted(df_d["pp_status"].dropna().unique().tolist()) if "pp_status" in df_d.columns else []
    compradores_disp = (
        sorted(df_d["user_name"].dropna().astype(str).unique().tolist())
        if "user_name" in df_d.columns
        else []
    )

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        estado_sel = st.multiselect(
            "Filtrar por estado de propuesta",
            options=estados_disp,
            default=estados_disp,
        )
    with col_f2:
        comprador_sel = st.multiselect(
            "Filtrar por comprador (usuario)",
            options=compradores_disp,
            default=compradores_disp,
        )

    # Aplicación de filtros
    if "pp_status" in df_d.columns and estado_sel:
        df_d = df_d[df_d["pp_status"].isin(estado_sel)]

    if "user_name" in df_d.columns and comprador_sel:
        df_d = df_d[df_d["user_name"].astype(str).isin(comprador_sel)]

    # Orden razonable
    if "base_ts" in df_d.columns:
        df_d = df_d.sort_values("base_ts", ascending=False)

    st.markdown("### Detalle filtrado de propuestas")
    st.dataframe(df_d, use_container_width=True, hide_index=True)

    # Resumen agregado por comprador para este filtro
    if not df_d.empty and "user_name" in df_d.columns:
        df_res_comp = (
            df_d.groupby("user_name", dropna=False)
                .agg(
                    propuestas=("pp_id", "nunique"),
                    monto_total=("pp_total_amount", "sum"),
                    avg_ajuste_min=("adjust_time_min", "mean"),
                )
                .reset_index()
        )
        st.markdown("### Resumen por comprador (con filtros aplicados)")
        st.dataframe(df_res_comp, use_container_width=True, hide_index=True)
# =======================================================
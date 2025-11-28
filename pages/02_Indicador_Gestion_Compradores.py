# pages/02_Indicador_Gestion_Compradores.py
# Indicador 2 — Gestión de Compradores
# Versión 1: foco en uso de Connexa
#   - OC y bultos gestionados desde Connexa por comprador
#   - Ranking de compradores por uso de Connexa
#   - Ranking de compradores por Forecast → Propuesta
#   - Detalle tabular completo para análisis

import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.ui import render_header, make_date_filters
from modules.db import get_pg_engine, get_connexa_engine

from modules.queries.compradores import (
    get_ranking_compradores_resumen,      # OC y bultos Connexa
    get_productividad_comprador_mensual, # (no usado aún, pero disponible)
    get_ranking_comprador_forecast,      # ranking Forecast → Propuesta
)

# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 2 — Gestión de Compradores",
    page_icon="🧑‍💼",
    layout="wide",
)

render_header("Indicador 2 — Gestión de Compradores")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# =======================================================
# 1. Helpers locales
# =======================================================

def _normalize_c_comprador(df: pd.DataFrame, col: str = "c_comprador") -> pd.DataFrame:
    """Normaliza la columna c_comprador a tipo Int64 (nullable) si existe."""
    if df is None or df.empty:
        return df
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    """Suma segura: si la columna no existe o el DF está vacío, devuelve 0."""
    if df is None or df.empty or col not in df.columns:
        return 0.0
    serie = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return float(serie.sum())


@st.cache_data(ttl=ttl)
def load_compradores(desde: date, hasta: date):
    """
    Carga los dataframes necesarios para el indicador de gestión de compradores.

    Versión inicial:
      - df_rk_oc_connexa: ranking de compradores por OC y bultos desde Connexa
      - df_rk_fp: ranking Forecast → Propuesta (Connexa)
      - df_prod_mensual: productividad mensual Forecast → Propuesta (no integrada aún)
    """
    pg_engine = get_pg_engine()
    eng_connexa = get_connexa_engine()

    # 1) Ranking de compradores por OC Connexa
    df_rk_oc_connexa = get_ranking_compradores_resumen(
        pg_engine, desde=desde, hasta=hasta, topn=100
    )

    # 2) Productividad mensual forecast→propuesta por comprador
    df_prod_mensual = get_productividad_comprador_mensual(
        eng_connexa, desde=desde, hasta=hasta
    )

    # 3) Ranking forecast→propuesta (rango)
    df_rk_fp = get_ranking_comprador_forecast(
        eng_connexa, desde=desde, hasta=hasta, topn=100
    )

    return df_rk_oc_connexa, df_rk_fp, df_prod_mensual


# =======================================================
# 2. Construcción del DF maestro por comprador (Connexa)
# =======================================================

df_rk_oc_connexa, df_rk_fp, df_prod_mensual = load_compradores(desde, hasta)

# Normalizamos c_comprador
df_rk_oc_connexa = _normalize_c_comprador(df_rk_oc_connexa)

# Base: ranking de OC Connexa por comprador
if df_rk_oc_connexa is not None and not df_rk_oc_connexa.empty:
    df_master = df_rk_oc_connexa.copy()
else:
    df_master = pd.DataFrame(
        columns=["c_comprador", "oc_total_connexa", "bultos_connexa"]
    )

# Aseguramos columnas esperadas
for col in ["c_comprador", "oc_total_connexa", "bultos_connexa"]:
    if col not in df_master.columns:
        df_master[col] = 0

# Etiqueta legible de comprador (por ahora, el código como texto;
# más adelante se puede enriquecer con tabla de nombres de compradores)
df_master["etiqueta_comprador"] = df_master["c_comprador"].apply(
    lambda x: str(x) if pd.notna(x) else "- sin comprador -"
)

# Normalización numérica
for col in ["oc_total_connexa", "bultos_connexa"]:
    df_master[col] = pd.to_numeric(df_master[col], errors="coerce").fillna(0)


# =======================================================
# 3. Resumen ejecutivo ( KPI globales )
# =======================================================

st.markdown("## Resumen ejecutivo de gestión de compradores")

if df_master.empty:
    st.info("No se encontraron datos de compradores para el rango seleccionado.")
    st.stop()

n_compradores = df_master["etiqueta_comprador"].nunique()
total_oc_conn = int(_safe_sum(df_master, "oc_total_connexa"))
total_bultos_conn = int(_safe_sum(df_master, "bultos_connexa"))

# Total de propuestas generadas (si df_rk_fp trae esa columna)
if df_rk_fp is not None and not df_rk_fp.empty and "propuestas" in df_rk_fp.columns:
    total_prop = int(
        pd.to_numeric(df_rk_fp["propuestas"], errors="coerce").fillna(0).sum()
    )
else:
    total_prop = 0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Compradores activos en Connexa", value=n_compradores)
with col2:
    st.metric("OC generadas desde Connexa (rango)", value=total_oc_conn)
with col3:
    st.metric("Bultos gestionados desde Connexa", value=f"{total_bultos_conn:,.0f}")
with col4:
    st.metric("Propuestas Forecast→Propuesta (rango)", value=total_prop)

st.markdown("---")

# =======================================================
# 4. Tabs de análisis
# =======================================================

tab1, tab2, tab3 = st.tabs(
    ["📊 Visión general", "🏅 Ranking de compradores", "📋 Detalle tabular"]
)

# ---------------------------
# TAB 1: Visión general
# ---------------------------
with tab1:
    st.subheader("Uso de Connexa por comprador (OC y bultos)")

    df_plot = df_master.copy()
    df_plot = df_plot.sort_values("oc_total_connexa", ascending=False)

    # Gráfico de barras: OC Connexa por comprador
    if not df_plot.empty:
        fig_bar_oc = px.bar(
            df_plot,
            x="etiqueta_comprador",
            y="oc_total_connexa",
            title="OC generadas desde Connexa por comprador",
        )
        fig_bar_oc.update_layout(
            xaxis_title="Comprador",
            yaxis_title="Cantidad de OC (Connexa)",
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig_bar_oc, use_container_width=True)
    else:
        st.info("No hay datos para graficar OC desde Connexa.")

    st.markdown("### Volumen de bultos gestionado desde Connexa")

    df_bultos = df_master.copy()
    df_bultos["bultos_connexa"] = pd.to_numeric(
        df_bultos.get("bultos_connexa", 0), errors="coerce"
    ).fillna(0)

    df_bultos = df_bultos.sort_values("bultos_connexa", ascending=False)

    if not df_bultos.empty:
        fig_bar_bultos = px.bar(
            df_bultos,
            x="etiqueta_comprador",
            y="bultos_connexa",
            title="Bultos gestionados desde Connexa por comprador",
        )
        fig_bar_bultos.update_layout(
            xaxis_title="Comprador",
            yaxis_title="Bultos (Connexa)",
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig_bar_bultos, use_container_width=True)
    else:
        st.info("No hay datos de bultos para mostrar productividad.")


# ---------------------------
# TAB 2: Ranking de compradores
# ---------------------------
with tab2:
    st.subheader("Ranking de uso de Connexa por comprador")

    df_rank_usage = df_master.copy()
    df_rank_usage["oc_total_connexa"] = pd.to_numeric(
        df_rank_usage.get("oc_total_connexa", 0), errors="coerce"
    ).fillna(0)
    df_rank_usage["bultos_connexa"] = pd.to_numeric(
        df_rank_usage.get("bultos_connexa", 0), errors="coerce"
    ).fillna(0)

    df_rank_usage = df_rank_usage.sort_values(
        ["oc_total_connexa", "bultos_connexa"], ascending=False
    ).head(20)

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### Top compradores por cantidad de OC desde Connexa")
        if not df_rank_usage.empty:
            fig_rk_oc = px.bar(
                df_rank_usage,
                x="oc_total_connexa",
                y="etiqueta_comprador",
                orientation="h",
                text="oc_total_connexa",
                title="Top 20 compradores por OC Connexa",
            )
            fig_rk_oc.update_layout(
                xaxis_title="Cantidad de OC desde Connexa",
                yaxis_title="Comprador",
            )
            st.plotly_chart(fig_rk_oc, use_container_width=True)
        else:
            st.info("No hay datos para ranking de OC Connexa.")

    with col_r:
        st.markdown("#### Top compradores por bultos gestionados desde Connexa")
        if not df_rank_usage.empty:
            fig_rk_bultos = px.bar(
                df_rank_usage,
                x="bultos_connexa",
                y="etiqueta_comprador",
                orientation="h",
                text="bultos_connexa",
                title="Top 20 compradores por bultos Connexa",
            )
            fig_rk_bultos.update_layout(
                xaxis_title="Bultos desde Connexa",
                yaxis_title="Comprador",
            )
            st.plotly_chart(fig_rk_bultos, use_container_width=True)
        else:
            st.info("No hay datos para ranking de bultos Connexa.")

    st.markdown("---")
    st.subheader("Ranking de productividad Forecast → Propuesta")

    if df_rk_fp is not None and not df_rk_fp.empty:
        df_fp_rank = df_rk_fp.copy()
        # Se asume que df_rk_fp tiene columna 'comprador' y 'propuestas'
        fig_fp = px.bar(
            df_fp_rank,
            x="propuestas",
            y="comprador",
            orientation="h",
            title="Ranking por cantidad de propuestas generadas",
            text="propuestas",
        )
        fig_fp.update_layout(
            xaxis_title="Propuestas generadas",
            yaxis_title="Comprador",
        )
        st.plotly_chart(fig_fp, use_container_width=True)
    else:
        st.info("No hay datos de ranking Forecast→Propuesta para el rango seleccionado.")


# ---------------------------
# TAB 3: Detalle tabular
# ---------------------------
with tab3:
    st.subheader("Detalle completo por comprador (Connexa)")

    st.dataframe(
        df_master.sort_values("etiqueta_comprador"),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown(
        "_Pueden ordenar, filtrar y copiar los datos desde esta tabla para análisis adicionales._"
    )
# =======================================================
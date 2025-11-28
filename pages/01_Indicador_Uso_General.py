# pages/01_Indicador_Uso_General.py
# Indicador 1 — Uso general del sistema
#
# Responde a:
#   a) Evolución mensual de los indicadores de utilización del sistema
#   b) Volumen total gestionado
#   c) Tiempo medio utilizado
#   d) Efectividad

from datetime import date
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from modules.db import (
    get_connexa_engine,   # connexa_platform_ms
    get_diarco_engine,    # diarco_data
    get_sqlserver_engine  # SGM (SQL Server)
)

from modules.ui import render_header, make_date_filters

from modules.queries.uso_general import (
    ensure_mon_objects,
    ensure_forecast_views,
    get_oc_generadas_mensual,
    get_forecast_propuesta_conversion_mensual,
    get_embudo_connexa_sgm_mensual,
    get_proporcion_ci_vs_sgm_mensual,
)


# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 1 — Uso general del sistema",
    page_icon="📈",
    layout="wide",
)

render_header("Indicador 1 — Uso general del sistema")

# Filtros de fecha
desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# =======================================================
# 1. Funciones auxiliares y de caché
# =======================================================

@st.cache_data(ttl=ttl)
def _init_mon(desde: date, hasta: date) -> bool:
    """
    Asegura que existan las vistas mon.* y mon.v_forecast_propuesta_base.
    """
    try:
        eng_d = get_diarco_engine()
        eng_c = get_connexa_engine()
        if eng_d is not None:
            ensure_mon_objects(eng_d)
        if eng_c is not None:
            ensure_forecast_views(eng_c)
        return True
    except Exception:
        return False


@st.cache_data(ttl=ttl)
def load_uso_general(desde: date, hasta: date):
    """
    Carga los principales DataFrames usados por el indicador de uso general:
      - df_oc     : OC generadas desde Connexa (mon.v_oc_generadas_mensual)
      - df_fp     : Forecast → Propuesta (serie mensual, conversión)
      - df_emb    : Embudo Connexa → SGM (pedidos vs OC, bultos)
      - df_prop   : Proporción de OC SGM originadas en Connexa
    """
    _ = _init_mon(desde, hasta)

    eng_d = get_diarco_engine()
    eng_c = get_connexa_engine()
    eng_s = get_sqlserver_engine()

    df_oc   = get_oc_generadas_mensual(eng_d, desde, hasta) if eng_d else pd.DataFrame()
    df_fp   = get_forecast_propuesta_conversion_mensual(eng_c, desde, hasta) if eng_c else pd.DataFrame()
    df_emb  = get_embudo_connexa_sgm_mensual(eng_d, eng_s, desde, hasta) if (eng_d and eng_s) else pd.DataFrame()
    df_prop = get_proporcion_ci_vs_sgm_mensual(eng_s, desde, hasta) if eng_s else pd.DataFrame()

    return df_oc, df_fp, df_emb, df_prop


# --- SQL local para tiempos medios (sobre mon.v_forecast_propuesta_base) ---
SQL_TIEMPOS_MENSUALES = text("""
SELECT
  date_trunc('month', (base_ts AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
  COUNT(*)                                                AS propuestas,
  AVG(exec_time_min)                                      AS avg_exec_min,
  AVG(lead_open_min)                                      AS avg_lead_open_min,
  AVG(adjust_time_min)                                    AS avg_adjust_min,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY adjust_time_min) AS p50_adjust_min,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY adjust_time_min) AS p90_adjust_min
FROM mon.v_forecast_propuesta_base
WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
  AND pp_id IS NOT NULL
GROUP BY 1
ORDER BY 1;
""")


@st.cache_data(ttl=ttl)
def load_tiempos_mensuales(desde: date, hasta: date) -> pd.DataFrame:
    """
    Devuelve tiempos medios de uso del sistema (Forecast → Propuesta),
    agregados por mes a partir de mon.v_forecast_propuesta_base.
    """
    eng_c = get_connexa_engine()
    if eng_c is None:
        return pd.DataFrame()

    _ = _init_mon(desde, hasta)

    with eng_c.connect() as con:
        df = pd.read_sql(SQL_TIEMPOS_MENSUALES, con, params={"desde": desde, "hasta": hasta})

    if df.empty:
        return df

    df["mes"] = pd.to_datetime(df["mes"])

    for col in ("avg_exec_min", "avg_lead_open_min", "avg_adjust_min",
                "p50_adjust_min", "p90_adjust_min"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =======================================================
# 2. Carga de datos
# =======================================================

df_oc, df_fp, df_emb, df_prop = load_uso_general(desde, hasta)
df_tiempos = load_tiempos_mensuales(desde, hasta)

# =======================================================
# 3. Evolución mensual de uso (visión integrada)
# =======================================================
st.markdown("## 1. Evolución mensual de los indicadores de utilización")

# Normalizaciones básicas
if not df_fp.empty and "mes" in df_fp.columns:
    df_fp["mes"] = pd.to_datetime(df_fp["mes"])

if not df_emb.empty and "mes" in df_emb.columns:
    df_emb["mes"] = pd.to_datetime(df_emb["mes"])

if not df_prop.empty and "mes" in df_prop.columns:
    df_prop["mes"] = pd.to_datetime(df_prop["mes"])

# Unir algunas métricas clave por mes en un solo DataFrame de resumen
df_resumen = None
if not df_fp.empty:
    df_resumen = df_fp[["mes", "ejecuciones", "propuestas", "conversion"]].copy()

if df_emb is not None and not df_emb.empty:
    cols_emb = ["mes", "pedidos_connexa", "oc_sgm"]
    df_e = df_emb[cols_emb].copy()
    df_resumen = df_e if df_resumen is None else pd.merge(df_resumen, df_e, on="mes", how="outer")

if df_prop is not None and not df_prop.empty and "proporcion_ci" in df_prop.columns:
    df_p = df_prop[["mes", "proporcion_ci"]].copy()
    df_resumen = df_p if df_resumen is None else pd.merge(df_resumen, df_p, on="mes", how="outer")

if df_resumen is None or df_resumen.empty:
    st.info("No se encontraron datos suficientes para mostrar la evolución mensual de uso.")
else:
    df_resumen = df_resumen.sort_values("mes")

    # Conversión a porcentajes para lectura gerencial
    if "conversion" in df_resumen.columns:
        df_resumen["conversion_pct"] = pd.to_numeric(df_resumen["conversion"], errors="coerce").fillna(0.0) * 100.0
    if "proporcion_ci" in df_resumen.columns:
        df_resumen["proporcion_ci_pct"] = pd.to_numeric(df_resumen["proporcion_ci"], errors="coerce").fillna(0.0) * 100.0

    # KPIs globales (sobre el rango)
    total_ejec = int(pd.to_numeric(df_resumen.get("ejecuciones", 0), errors="coerce").fillna(0).sum())
    total_prop = int(pd.to_numeric(df_resumen.get("propuestas", 0), errors="coerce").fillna(0).sum())
    total_ped  = int(pd.to_numeric(df_resumen.get("pedidos_connexa", 0), errors="coerce").fillna(0).sum())
    total_oc   = int(pd.to_numeric(df_resumen.get("oc_sgm", 0), errors="coerce").fillna(0).sum())

    conv_global = (total_prop / total_ejec) if total_ejec > 0 else 0.0
    tasa_embudo = (total_oc / total_ped * 100.0) if total_ped > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Ejecuciones de forecast (rango)", value=total_ejec)
    with col2:
        st.metric("Propuestas generadas (rango)", value=total_prop)
    with col3:
        st.metric("Pedidos Connexa (NIPs)", value=total_ped)
    with col4:
        st.metric("OC SGM generadas", value=total_oc)

    col5, col6 = st.columns(2)
    with col5:
        st.metric("Tasa global Forecast → Propuesta", value=f"{conv_global * 100:,.1f} %")
    with col6:
        st.metric("Tasa global Pedidos Connexa → OC SGM", value=f"{tasa_embudo:,.1f} %")

    # Gráfico 1: combinación de volúmenes (ejecuciones, propuestas, pedidos, OC)
    st.markdown("### 1.1 Volúmenes mensuales (Forecast, Propuestas, Pedidos, OC)")

    df_vol = df_resumen.copy()
    for c in ("ejecuciones", "propuestas", "pedidos_connexa", "oc_sgm"):
        if c in df_vol.columns:
            df_vol[c] = pd.to_numeric(df_vol[c], errors="coerce").fillna(0)

    # Mezcla solo las columnas existentes
    series_vol = [c for c in ["ejecuciones", "propuestas", "pedidos_connexa", "oc_sgm"] if c in df_vol.columns]
    if series_vol:
        fig_vol = px.bar(
            df_vol,
            x="mes",
            y=series_vol,
            barmode="group",
            title="Evolución mensual de volúmenes (Forecast, Propuestas, Pedidos Connexa, OC SGM)",
        )
        fig_vol.update_layout(xaxis_title="Mes", yaxis_title="Cantidad", legend_title="Métrica")
        st.plotly_chart(fig_vol, use_container_width=True)

    # Gráfico 2: tasas de efectividad mensuales
    st.markdown("### 1.2 Tasas de efectividad mensuales")

    df_eff = df_resumen[["mes"]].copy()
    if "conversion_pct" in df_resumen.columns:
        df_eff["Forecast→Propuesta (%)"] = df_resumen["conversion_pct"]
    if "proporcion_ci_pct" in df_resumen.columns:
        df_eff["OC SGM originadas en Connexa (%)"] = df_resumen["proporcion_ci_pct"]

    if len(df_eff.columns) > 1:
        df_eff_melt = df_eff.melt(id_vars="mes", var_name="indicador", value_name="valor")
        fig_eff = px.line(
            df_eff_melt,
            x="mes",
            y="valor",
            color="indicador",
            markers=True,
            title="Evolución mensual de tasas de efectividad",
        )
        fig_eff.update_layout(xaxis_title="Mes", yaxis_title="Porcentaje (%)")
        st.plotly_chart(fig_eff, use_container_width=True)

    with st.expander("Detalle de evolución mensual (tabla resumen)"):
        st.dataframe(df_resumen, use_container_width=True)


# =======================================================
# 4. Volumen total gestionado
# =======================================================
st.markdown("---")
st.markdown("## 2. Volumen total gestionado (bultos)")

if df_emb.empty:
    st.info("No se encontraron datos de embudo Connexa → SGM para el rango seleccionado.")
else:
    df_emb_plot = df_emb.copy()
    if "mes" in df_emb_plot.columns:
        df_emb_plot["mes"] = pd.to_datetime(df_emb_plot["mes"])

    for c in ("bultos_connexa", "bultos_sgm"):
        if c in df_emb_plot.columns:
            df_emb_plot[c] = pd.to_numeric(df_emb_plot[c], errors="coerce").fillna(0.0)

    total_bultos_connexa = float(df_emb_plot.get("bultos_connexa", 0).sum())
    total_bultos_sgm     = float(df_emb_plot.get("bultos_sgm", 0).sum())

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Bultos gestionados en Connexa (rango)", value=f"{total_bultos_connexa:,.0f}")
    with col2:
        st.metric("Bultos embarcados en OC SGM desde Connexa (rango)", value=f"{total_bultos_sgm:,.0f}")

    # Gráfico: barras apiladas por mes (Connexa vs SGM)
    if "bultos_connexa" in df_emb_plot.columns and "bultos_sgm" in df_emb_plot.columns:
        df_stack = df_emb_plot[["mes", "bultos_connexa", "bultos_sgm"]].melt(
            id_vars="mes", var_name="origen", value_name="bultos"
        )
        fig_bultos = px.bar(
            df_stack,
            x="mes",
            y="bultos",
            color="origen",
            title="Bultos gestionados mensuales (Connexa vs OC SGM)",
        )
        fig_bultos.update_layout(xaxis_title="Mes", yaxis_title="Bultos")
        st.plotly_chart(fig_bultos, use_container_width=True)

    with st.expander("Detalle mensual de bultos (tabla)"):
        st.dataframe(df_emb_plot, use_container_width=True)


# =======================================================
# 5. Tiempo medio utilizado (uso del sistema)
# =======================================================
st.markdown("---")
st.markdown("## 3. Tiempo medio utilizado (Forecast → Propuesta)")

if df_tiempos.empty:
    st.info("No se encontraron datos de tiempos de ejecución/ajuste para el rango seleccionado.")
else:
    # KPIs globales (P50/P90 de ajuste, promedios)
    kpi_p50 = float(df_tiempos["p50_adjust_min"].median()) if "p50_adjust_min" in df_tiempos.columns else 0.0
    kpi_p90 = float(df_tiempos["p90_adjust_min"].median()) if "p90_adjust_min" in df_tiempos.columns else 0.0
    kpi_avg_exec = float(df_tiempos["avg_exec_min"].mean()) if "avg_exec_min" in df_tiempos.columns else 0.0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Tiempo medio de ejecución forecast (promedio, min)", value=f"{kpi_avg_exec:,.1f}")
    with col2:
        st.metric("P50 tiempo de ajuste propuestas (min)", value=f"{kpi_p50:,.1f}")
    with col3:
        st.metric("P90 tiempo de ajuste propuestas (min)", value=f"{kpi_p90:,.1f}")

    # Gráfico: evolución mensual del tiempo de ajuste medio
    if "avg_adjust_min" in df_tiempos.columns:
        fig_t = px.line(
            df_tiempos,
            x="mes",
            y="avg_adjust_min",
            markers=True,
            title="Tiempo medio de ajuste de propuestas (minutos, mensual)",
        )
        fig_t.update_layout(xaxis_title="Mes", yaxis_title="Minutos")
        st.plotly_chart(fig_t, use_container_width=True)

    # Gráfico opcional: P50 vs P90 por mes
    if "p50_adjust_min" in df_tiempos.columns and "p90_adjust_min" in df_tiempos.columns:
        df_tp = df_tiempos[["mes", "p50_adjust_min", "p90_adjust_min"]].melt(
            id_vars="mes", var_name="percentil", value_name="minutos"
        )
        fig_tp = px.line(
            df_tp,
            x="mes",
            y="minutos",
            color="percentil",
            markers=True,
            title="Percentiles de tiempo de ajuste (P50 / P90)",
        )
        fig_tp.update_layout(xaxis_title="Mes", yaxis_title="Minutos")
        st.plotly_chart(fig_tp, use_container_width=True)

    with st.expander("Detalle mensual de tiempos (tabla)"):
        st.dataframe(df_tiempos, use_container_width=True)


# =======================================================
# 6. Efectividad (síntesis)
# =======================================================
st.markdown("---")
st.markdown("## 4. Efectividad del uso de la herramienta")

if df_resumen is None or df_resumen.empty:
    st.info("No se pueden calcular indicadores de efectividad sin series mensuales.")
else:
    # Se reutilizan conversion_pct y proporcion_ci_pct calculados antes
    conv_promedio = float(df_resumen["conversion_pct"].mean()) if "conversion_pct" in df_resumen.columns else 0.0
    prop_ci_prom  = float(df_resumen["proporcion_ci_pct"].mean()) if "proporcion_ci_pct" in df_resumen.columns else 0.0

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Efectividad promedio Forecast → Propuesta", value=f"{conv_promedio:,.1f} %")
    with col2:
        st.metric("Efectividad promedio OC SGM originadas en Connexa", value=f"{prop_ci_prom:,.1f} %")

    st.caption(
        "Estos promedios se calculan como la media simple de las tasas mensuales "
        "dentro del rango seleccionado."
    )

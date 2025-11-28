# pages/02_Indicador_Gestion_Compradores.py
# Indicador 2 — Gestión de Compradores
#
# Ejes que cubre:
#   a) Forecast y propuestas por comprador en Connexa
#   b) OC emitidas desde Connexa por comprador
#   c) OC totales emitidas en SGM por comprador
#   d) Proporción OC Connexa / OC totales SGM por comprador
#   e) Ranking de uso (bultos y OC)
#   f) Evolución mensual de uso
#   g) Productividad (tiempos) por comprador

from datetime import date
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from modules.db import (
    get_connexa_engine,    # connexa_platform_ms
    get_diarco_engine,     # diarco_data
    get_sqlserver_engine,  # data-sync (SGM)
)
from modules.ui import render_header, make_date_filters

from modules.queries.uso_general import (
    ensure_mon_objects,
    ensure_forecast_views,
)

from modules.queries.compradores import (
    get_ranking_compradores_resumen,    # OC y bultos desde Connexa
    get_productividad_comprador_mensual,
    get_ranking_comprador_forecast,     # ranking por propuestas/tiempos
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

# Filtros de fecha comunes
desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# =======================================================
# 1. SQL auxiliares específicos de este indicador
# =======================================================

# 1) Resumen de OC por comprador en SGM (usa V_OC_RESUMEN_MENSUAL de SQL Server)
SQL_SGM_OC_RESUMEN_COMPRADOR = text("""
SELECT
    C_COMPRADOR AS c_comprador,
    SUM(Total_OC)            AS oc_sgm_totales,
    SUM(Total_Bultos_Pedidos) AS bultos_sgm_totales
FROM [data-sync].[dbo].[V_OC_RESUMEN_MENSUAL]
WHERE DATEFROMPARTS(Anio_Emision, Mes_Emision, 1) >= :desde
  AND DATEFROMPARTS(Anio_Emision, Mes_Emision, 1) <= :hasta
GROUP BY C_COMPRADOR;
""")

# 2) Mapa de códigos de comprador → nombre (desde diarco_data)
SQL_MAPA_COMPRADORES = text("""
SELECT cod_comprador::numeric AS c_comprador, n_comprador
FROM src.m_9_compradores;
""")

# 3) Uso de Forecast → Propuesta por comprador en Connexa
#    (número de forecasts y propuestas asociadas)
SQL_FORECAST_OC_BY_BUYER = text("""
SELECT
  COALESCE(NULLIF(trim(user_name), ''), buyer_id::text, '- sin comprador -') AS comprador,
  COUNT(DISTINCT fe_id)                                                      AS forecasts,
  COUNT(DISTINCT pp_id) FILTER (WHERE pp_id IS NOT NULL)                     AS propuestas
FROM mon.v_forecast_propuesta_base
WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
GROUP BY 1
ORDER BY forecasts DESC;
""")


# =======================================================
# 2. Funciones de inicialización y carga (con caché)
# =======================================================

@st.cache_data(ttl=ttl)
def _init_mon(desde: date, hasta: date) -> bool:
    """
    Asegura la existencia de vistas mon.* y mon.v_forecast_propuesta_base.
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
def load_mapa_compradores():
    """
    Devuelve un DataFrame con columnas:
      - c_comprador (numeric)
      - n_comprador (texto)
    para mapear los códigos al nombre del comprador.
    """
    eng_d = get_diarco_engine()
    if eng_d is None:
        return pd.DataFrame()

    with eng_d.connect() as con:
        df = pd.read_sql(SQL_MAPA_COMPRADORES, con)

    if not df.empty:
        df["c_comprador"] = pd.to_numeric(df["c_comprador"], errors="coerce").astype("Int64")
    return df


@st.cache_data(ttl=ttl)
def load_oc_sgm_por_comprador(desde: date, hasta: date) -> pd.DataFrame:
    """
    Devuelve OC totales y bultos totales en SGM por comprador,
    a partir de V_OC_RESUMEN_MENSUAL (SQL Server).
    """
    eng_sgm = get_sqlserver_engine()
    if eng_sgm is None:
        return pd.DataFrame()

    with eng_sgm.connect() as con:
        df = pd.read_sql(
            SQL_SGM_OC_RESUMEN_COMPRADOR,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    df["c_comprador"] = pd.to_numeric(df["c_comprador"], errors="coerce").astype("Int64")
    df["oc_sgm_totales"] = pd.to_numeric(df["oc_sgm_totales"], errors="coerce").fillna(0).astype("Int64")
    df["bultos_sgm_totales"] = pd.to_numeric(df["bultos_sgm_totales"], errors="coerce").fillna(0.0)
    return df


@st.cache_data(ttl=ttl)
def load_forecast_por_comprador(desde: date, hasta: date) -> pd.DataFrame:
    """
    Devuelve, por comprador (user_name/buyer_id):
      - forecasts: # de ejecuciones de forecast
      - propuestas: # de propuestas generadas
    desde mon.v_forecast_propuesta_base.
    """
    eng_c = get_connexa_engine()
    if eng_c is None:
        return pd.DataFrame()

    _ = _init_mon(desde, hasta)

    with eng_c.connect() as con:
        df = pd.read_sql(
            SQL_FORECAST_OC_BY_BUYER,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    df["forecasts"] = pd.to_numeric(df["forecasts"], errors="coerce").fillna(0).astype("Int64")
    df["propuestas"] = pd.to_numeric(df["propuestas"], errors="coerce").fillna(0).astype("Int64")
    return df


@st.cache_data(ttl=ttl)
def load_compradores(desde: date, hasta: date):
    """
    Carga todo lo necesario para el indicador de compradores:

      - df_rk_oc_connexa: OC y bultos desde Connexa (mon.v_oc_generadas_mensual_ext)
      - df_rk_fp: ranking por propuestas (Forecast → Propuesta)
      - df_prod_mensual: productividad mensual por comprador (tiempos)
      - df_oc_sgm: OC totales y bultos totales en SGM
      - df_forecast: forecasts y propuestas por comprador
      - df_mapa: mapa c_comprador → n_comprador
    """
    _ = _init_mon(desde, hasta)

    eng_d = get_diarco_engine()
    eng_c = get_connexa_engine()

    df_rk_oc_connexa = get_ranking_compradores_resumen(eng_d, desde, hasta, topn=50) if eng_d else pd.DataFrame()
    df_rk_fp         = get_ranking_comprador_forecast(eng_c, desde, hasta, topn=50) if eng_c else pd.DataFrame()
    df_prod_mensual  = get_productividad_comprador_mensual(eng_c, desde, hasta) if eng_c else pd.DataFrame()
    df_oc_sgm        = load_oc_sgm_por_comprador(desde, hasta)
    df_forecast      = load_forecast_por_comprador(desde, hasta)
    df_mapa          = load_mapa_compradores()

    return df_rk_oc_connexa, df_rk_fp, df_prod_mensual, df_oc_sgm, df_forecast, df_mapa


# =======================================================
# 3. Preparación de dataset integrado por comprador
# =======================================================

(
    df_rk_oc_connexa,
    df_rk_fp,
    df_prod_mensual,
    df_oc_sgm,
    df_forecast,
    df_mapa,
) = load_compradores(desde, hasta)


def _normalize_c_comprador(df: pd.DataFrame, col: str = "c_comprador") -> pd.DataFrame:
    """
    Normaliza la columna c_comprador a tipo Int64 (nullable) si existe.
    """
    if df is None or df.empty:
        return df
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


# Normalizar tipos de c_comprador en todos los DF que lo tengan
df_rk_oc_connexa = _normalize_c_comprador(df_rk_oc_connexa)
df_oc_sgm        = _normalize_c_comprador(df_oc_sgm)
df_mapa          = _normalize_c_comprador(df_mapa)

# DataFrame maestro: una fila por comprador
df_master = None

# 1) Base Connexa (OC generadas desde Connexa)
if df_rk_oc_connexa is not None and not df_rk_oc_connexa.empty:
    # Se espera que get_ranking_compradores_resumen devuelva:
    #   c_comprador, comprador (texto), oc_total_connexa, bultos_connexa
    df_master = df_rk_oc_connexa.copy()

    # Si no trajera c_comprador, la creamos y normalizamos
    if "c_comprador" not in df_master.columns:
        df_master["c_comprador"] = pd.NA
    # IMPORTANTE: normalizar tipo aquí
    df_master = _normalize_c_comprador(df_master)
else:
    df_master = pd.DataFrame(
        columns=["c_comprador", "comprador", "oc_total_connexa", "bultos_connexa"]
    )

# 2) Agregar OC totales SGM
if df_oc_sgm is not None and not df_oc_sgm.empty:
    if df_master is None or df_master.empty:
        df_master = df_oc_sgm.copy()
    else:
        # Ambos DF ya tienen c_comprador como Int64 gracias a _normalize_c_comprador
        df_master = pd.merge(
            df_master,
            df_oc_sgm,
            on="c_comprador",
            how="outer",
        )

# 3) Agregar forecast/propuestas (Connexa)
if df_forecast is not None and not df_forecast.empty:
    # df_forecast viene indexado por 'comprador' (texto)
    if "comprador" in df_forecast.columns and "comprador" in df_master.columns:
        df_master = pd.merge(
            df_master,
            df_forecast.rename(columns={
                "forecasts": "forecasts_connexa",
                "propuestas": "propuestas_connexa",
            }),
            on="comprador",
            how="outer",
        )
    else:
        # Fallback: join por índice si no estuviera la columna comprador en master
        df_master = df_master.join(
            df_forecast.rename(columns={
                "forecasts": "forecasts_connexa",
                "propuestas": "propuestas_connexa",
            }),
            how="outer",
            rsuffix="_forecast",
        )

# 4) Completar etiquetas de comprador a partir del mapa (código → nombre)
if df_mapa is not None and not df_mapa.empty:
    if "c_comprador" in df_master.columns:
        df_master = pd.merge(
            df_master,
            df_mapa.rename(columns={"n_comprador": "nombre_oficial"}),
            on="c_comprador",
            how="left",
        )
        # Etiqueta final: nombre oficial si está, sino 'comprador' ya existente, sino código
        df_master["etiqueta_comprador"] = df_master.apply(
            lambda r: (
                str(r["nombre_oficial"]).strip()
                if pd.notna(r.get("nombre_oficial"))
                else (
                    str(r["comprador"]).strip()
                    if pd.notna(r.get("comprador"))
                    else (
                        str(r["c_comprador"])
                        if pd.notna(r.get("c_comprador"))
                        else "- sin comprador -"
                    )
                )
            ),
            axis=1,
        )
    else:
        # Sin c_comprador, se queda con 'comprador' como etiqueta
        if "comprador" in df_master.columns:
            df_master["etiqueta_comprador"] = df_master["comprador"].astype(str)
else:
    # Sin mapa, fallback
    if "comprador" in df_master.columns:
        df_master["etiqueta_comprador"] = df_master["comprador"].astype(str)

# Normalizar columnas numéricas
for col in [
    "oc_total_connexa", "bultos_connexa",
    "oc_sgm_totales", "bultos_sgm_totales",
    "forecasts_connexa", "propuestas_connexa",
]:
    if col in df_master.columns:
        df_master[col] = pd.to_numeric(df_master[col], errors="coerce").fillna(0)

# Proporciones y OC directas
if "oc_sgm_totales" in df_master.columns and "oc_total_connexa" in df_master.columns:
    df_master["oc_directas_sgm"] = (df_master["oc_sgm_totales"] - df_master["oc_total_connexa"]).clip(lower=0)
    df_master["prop_oc_connexa_sobre_sgm"] = df_master.apply(
        lambda r: (r["oc_total_connexa"] / r["oc_sgm_totales"]) if r["oc_sgm_totales"] > 0 else 0.0,
        axis=1,
    )
else:
    df_master["oc_directas_sgm"] = 0.0
    df_master["prop_oc_connexa_sobre_sgm"] = 0.0

# Limpiar filas completamente vacías (compradores sin ningún dato)
if not df_master.empty:
    df_master = df_master[
        (df_master.get("oc_total_connexa", 0) > 0) |
        (df_master.get("oc_sgm_totales", 0) > 0) |
        (df_master.get("forecasts_connexa", 0) > 0) |
        (df_master.get("propuestas_connexa", 0) > 0)
    ]

# =======================================================
# 4. Resumen ejecutivo por comprador
# =======================================================
st.markdown("## 1. Visión general por comprador")

if df_master.empty:
    st.info("No se encontraron datos de compradores para el rango seleccionado.")
else:
    df_master = df_master.sort_values("etiqueta_comprador")

    n_compradores = df_master["etiqueta_comprador"].nunique()

    total_forecasts = int(df_master.get("forecasts_connexa", 0).sum())
    total_prop      = int(df_master.get("propuestas_connexa", 0).sum())
    total_oc_conn   = int(pd.Series(df_master.get("oc_total_connexa", pd.Series(dtype="Int64"))).fillna(0).sum())
    total_oc_sgm    = int(df_master.get("oc_sgm_totales", 0).sum())
    total_oc_direct = int(df_master.get("oc_directas_sgm", 0).sum())

    prop_global_ci = (total_oc_conn / total_oc_sgm) if total_oc_sgm > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Compradores con actividad", value=n_compradores)
    with col2:
        st.metric("Forecasts realizados en Connexa", value=total_forecasts)
    with col3:
        st.metric("OC Connexa (rango)", value=total_oc_conn)
    with col4:
        st.metric("OC totales SGM (rango)", value=total_oc_sgm)

    col5, col6 = st.columns(2)
    with col5:
        st.metric("OC directas en SGM (sin pasar por Connexa)", value=total_oc_direct)
    with col6:
        st.metric(
            "% global de OC SGM que vienen desde Connexa",
            value=f"{prop_global_ci * 100:,.1f} %",
        )

    # Top compradores por proporción CI/SGM
    st.markdown("### 1.1 Proporción de uso Connexa / SGM por comprador")

    df_prop = df_master.copy()
    df_prop["prop_pct"] = df_prop["prop_oc_connexa_sobre_sgm"] * 100.0

    # Filtrar sólo compradores con al menos alguna OC en SGM
    df_prop_valid = df_prop[df_prop["oc_sgm_totales"] > 0].copy()
    df_prop_valid = df_prop_valid.sort_values("prop_pct", ascending=False).head(15)

    if df_prop_valid.empty:
        st.info("No hay compradores con OC registradas en SGM en el rango seleccionado.")
    else:
        fig_prop = px.bar(
            df_prop_valid,
            x="prop_pct",
            y="etiqueta_comprador",
            orientation="h",
            title="Proporción de OC SGM que provienen de Connexa (Top 15 compradores)",
            text="prop_pct",
        )
        fig_prop.update_layout(
            xaxis_title="% OC SGM desde Connexa",
            yaxis_title="Comprador",
        )
        st.plotly_chart(fig_prop, use_container_width=True)

        with st.expander("Detalle de proporciones por comprador (tabla)"):
            cols_show = [
                "etiqueta_comprador",
                "c_comprador",
                "oc_total_connexa",
                "oc_sgm_totales",
                "oc_directas_sgm",
                "prop_pct",
            ]
            st.dataframe(
                df_prop[cols_show].sort_values("prop_pct", ascending=False),
                use_container_width=True,
            )


# =======================================================
# 5. Ranking de uso por comprador (OC y bultos)
# =======================================================
st.markdown("---")
st.markdown("## 2. Ranking de uso por comprador (OC y bultos)")

if df_master.empty:
    st.info("No se pueden construir rankings sin datos de compradores.")
else:
    df_rank = df_master.copy()

    # Ranking por OC Connexa
    st.markdown("### 2.1 Ranking por OC emitidas desde Connexa")

    df_rank_oc = df_rank.sort_values("oc_total_connexa", ascending=False).head(10)
    fig_rk_oc = px.bar(
        df_rank_oc,
        x="oc_total_connexa",
        y="etiqueta_comprador",
        orientation="h",
        title="Top 10 compradores por # OC Connexa",
        text="oc_total_connexa",
    )
    fig_rk_oc.update_layout(xaxis_title="# OC Connexa", yaxis_title="Comprador")
    st.plotly_chart(fig_rk_oc, use_container_width=True)

    # Ranking por bultos
    st.markdown("### 2.2 Ranking por bultos gestionados desde Connexa")

    if "bultos_connexa" in df_rank.columns:
        df_rank_b = df_rank.sort_values("bultos_connexa", ascending=False).head(10)
        fig_rk_b = px.bar(
            df_rank_b,
            x="bultos_connexa",
            y="etiqueta_comprador",
            orientation="h",
            title="Top 10 compradores por bultos en OC Connexa",
            text="bultos_connexa",
        )
        fig_rk_b.update_layout(xaxis_title="Bultos Connexa", yaxis_title="Comprador")
        st.plotly_chart(fig_rk_b, use_container_width=True)

    with st.expander("Detalle completo de uso por comprador (tabla)"):
        st.dataframe(df_rank, use_container_width=True)


# =======================================================
# 6. Evolución mensual y productividad por comprador
# =======================================================
st.markdown("---")
st.markdown("## 3. Evolución mensual y productividad por comprador")

# df_prod_mensual viene de get_productividad_comprador_mensual:
# columnas esperadas: mes, comprador, propuestas, monto_total, p50_ajuste_min, p90_ajuste_min, p50_lead_min, avg_exec_min

if df_prod_mensual.empty:
    st.info("No se encontraron datos de productividad mensual de compradores.")
else:
    if "mes" in df_prod_mensual.columns:
        df_prod_mensual["mes"] = pd.to_datetime(df_prod_mensual["mes"])

    # Selector de comprador para analizar detalle
    compradores_disponibles = sorted(df_prod_mensual["comprador"].dropna().unique().tolist())
    comprador_sel = st.selectbox(
        "Seleccionar comprador para ver detalle de evolución mensual",
        options=compradores_disponibles,
    )

    df_sel = df_prod_mensual[df_prod_mensual["comprador"] == comprador_sel].copy()

    col1, col2 = st.columns(2)
    with col1:
        total_prop_comp = int(df_sel.get("propuestas", 0).sum())
        st.metric(
            "Propuestas gestionadas por el comprador (rango)",
            value=total_prop_comp,
        )
    with col2:
        if "monto_total" in df_sel.columns:
            monto_tot = float(df_sel["monto_total"].sum())
            st.metric(
                "Monto total gestionado (suma periodo)",
                value=f"{monto_tot:,.0f}",
            )

    # Gráfico 1: propuestas mensuales
    fig_prop_comp = px.bar(
        df_sel,
        x="mes",
        y="propuestas",
        title=f"Propuestas mensuales — {comprador_sel}",
    )
    fig_prop_comp.update_layout(xaxis_title="Mes", yaxis_title="# Propuestas")
    st.plotly_chart(fig_prop_comp, use_container_width=True)

    # Gráfico 2: tiempos de ajuste P50/P90 por mes
    if "p50_ajuste_min" in df_sel.columns and "p90_ajuste_min" in df_sel.columns:
        df_tp = df_sel[["mes", "p50_ajuste_min", "p90_ajuste_min"]].melt(
            id_vars="mes", var_name="percentil", value_name="minutos"
        )
        fig_tp = px.line(
            df_tp,
            x="mes",
            y="minutos",
            color="percentil",
            markers=True,
            title=f"Tiempos de ajuste (P50 / P90) — {comprador_sel}",
        )
        fig_tp.update_layout(xaxis_title="Mes", yaxis_title="Minutos")
        st.plotly_chart(fig_tp, use_container_width=True)

    with st.expander("Detalle mensual del comprador seleccionado (tabla)"):
        st.dataframe(df_sel, use_container_width=True)

# Ranking adicional basado en Forecast → Propuesta
st.markdown("---")
st.markdown("## 4. Ranking de compradores por uso de Forecast → Propuesta")

if df_rk_fp.empty:
    st.info("No se encontraron datos de ranking de compradores por propuestas.")
else:
    df_rk_fp_plot = df_rk_fp.copy()
    if "comprador" not in df_rk_fp_plot.columns:
        df_rk_fp_plot["comprador"] = df_rk_fp_plot.index.astype(str)

    fig_rk_fp = px.bar(
        df_rk_fp_plot.sort_values("propuestas", ascending=False).head(10),
        x="propuestas",
        y="comprador",
        orientation="h",
        title="Top 10 compradores por # Propuestas (Forecast → Propuesta)",
        text="propuestas",
    )
    fig_rk_fp.update_layout(xaxis_title="# Propuestas", yaxis_title="Comprador")
    st.plotly_chart(fig_rk_fp, use_container_width=True)

    with st.expander("Detalle completo ranking Forecast → Propuesta (tabla)"):
        st.dataframe(df_rk_fp_plot, use_container_width=True)

import os
import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_OC_GENERADAS_RANGO,
    SQL_OC_GENERADAS_RANGO_EXT,
    SQL_RANKING_COMPRADORES,
    SQL_RANKING_COMPRADORES_NOMBRE,
)

# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 1 — OC generadas por CONNEXA y Ranking de Compradores",
    page_icon="🧾",
    layout="wide",
)
render_header("Indicador 1 — OC generadas por CONNEXA y Ranking de Compradores")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# -------------------------------------------------------
# Funciones de acceso a datos (cacheadas)
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_oc_generadas(desde, hasta, usar_nombres: bool = True) -> pd.DataFrame:
    """
    Lee OC generadas desde mon.v_oc_generadas_mensual / _ext.
    La vista ya está basada en t080_oc_precarga_connexa (c_compra_connexa).
    """
    eng = get_pg_engine()
    with eng.connect() as con:
        if usar_nombres:
            df = pd.read_sql(
                SQL_OC_GENERADAS_RANGO_EXT,
                con,
                params={"desde": desde, "hasta": hasta},
            )
        else:
            df = pd.read_sql(
                SQL_OC_GENERADAS_RANGO,
                con,
                params={"desde": desde, "hasta": hasta},
            )

    # Normalización tipos
    if "c_proveedor" in df.columns:
        df["c_proveedor"] = df["c_proveedor"].astype("Int64")
    if "total_oc" in df.columns:
        df["total_oc"] = pd.to_numeric(df["total_oc"], errors="coerce").fillna(0).astype("Int64")
    if "total_bultos" in df.columns:
        df["total_bultos"] = pd.to_numeric(df["total_bultos"], errors="coerce").fillna(0.0)

    return df


@st.cache_data(ttl=ttl)
def fetch_ranking(desde, hasta, topn: int = 20, usar_nombres: bool = True) -> pd.DataFrame:
    """
    Ranking de compradores por OC y Bultos en el rango seleccionado.
    """
    eng = get_pg_engine()
    with eng.connect() as con:
        if usar_nombres:
            df = pd.read_sql(
                SQL_RANKING_COMPRADORES_NOMBRE,
                con,
                params={"desde": desde, "hasta": hasta, "topn": topn},
            )
        else:
            df = pd.read_sql(
                SQL_RANKING_COMPRADORES,
                con,
                params={"desde": desde, "hasta": hasta, "topn": topn},
            )

    if "oc_total" in df.columns:
        df["oc_total"] = pd.to_numeric(df["oc_total"], errors="coerce").astype("Int64")
    if "bultos_total" in df.columns:
        df["bultos_total"] = pd.to_numeric(df["bultos_total"], errors="coerce")

    return df


# -------------------------------------------------------
# Controles generales
# -------------------------------------------------------
usar_nombres = st.toggle(
    "Mostrar nombres (compradores / proveedores)",
    value=True,
    help="Si se desactiva, se muestran sólo códigos de comprador/proveedor.",
)

tab_resumen, tab_ranking = st.tabs(["📊 Resumen de OC CONNEXA", "🏆 Ranking de Compradores"])

# -------------------------------------------------------
# TAB 1: Resumen OC
# -------------------------------------------------------
with tab_resumen:
    df = fetch_oc_generadas(desde, hasta, usar_nombres=usar_nombres)

    if df.empty:
        st.info("No hay datos de OC generadas por CONNEXA para el rango seleccionado.")
    else:
        st.caption(f"{len(df)} filas agregadas (mes–comprador–proveedor).")

        with st.expander("Ver tabla agregada"):
            st.dataframe(df, width="stretch", hide_index=True)

        # Totales por mes
        tot_mes = (
            df.groupby("mes", as_index=False)
              .agg(
                  total_oc=("total_oc", "sum"),
                  total_bultos=("total_bultos", "sum"),
              )
              .sort_values("mes")
        )

        col1, col2 = st.columns(2)
        with col1:
            fig1 = px.bar(
                tot_mes,
                x="mes",
                y="total_oc",
                title="OC generadas por CONNEXA por mes",
            )
            st.plotly_chart(fig1, use_container_width=True)

        with col2:
            fig2 = px.line(
                tot_mes,
                x="mes",
                y="total_bultos",
                title="Bultos de OC CONNEXA por mes",
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Métricas de rango
        st.markdown("### Totales del rango seleccionado")
        col3, col4 = st.columns(2)
        with col3:
            st.metric("OC generadas por CONNEXA (rango)", value=int(tot_mes["total_oc"].sum()))
        with col4:
            st.metric("Bultos de OC CONNEXA (rango)", value=float(tot_mes["total_bultos"].sum()))

        # Mini–Ranking de compradores (Top 10) dentro del resumen
        st.markdown("### Top 10 Compradores por #OC CONNEXA")
        rk_small = fetch_ranking(desde, hasta, topn=10, usar_nombres=usar_nombres)

        if rk_small.empty:
            st.info("No hay datos de ranking de compradores para el rango seleccionado.")
        else:
            y_col = "comprador" if "comprador" in rk_small.columns else "c_comprador"
            fig3 = px.bar(
                rk_small.sort_values("oc_total"),
                x="oc_total",
                y=y_col,
                orientation="h",
                title="Top 10 Compradores por cantidad de OC CONNEXA",
                text="oc_total",
            )
            fig3.update_layout(
                yaxis_title="",
                xaxis_title="OC Totales",
            )
            st.plotly_chart(fig3, use_container_width=True)

# -------------------------------------------------------
# TAB 2: Ranking de Compradores (vista detallada)
# -------------------------------------------------------
with tab_ranking:
    col_filtros = st.columns([1, 1, 2])
    with col_filtros[0]:
        topn = st.slider("Top N", min_value=5, max_value=50, value=20, step=5)
    with col_filtros[1]:
        ordenar_por = st.selectbox(
            "Ordenar por",
            options=["oc_total", "bultos_total"],
            index=0,
            help="Criterio principal para el ranking.",
        )

    df_rank = fetch_ranking(desde, hasta, topn=topn, usar_nombres=usar_nombres)

    if df_rank.empty:
        st.info("No hay datos para el ranking de compradores en el rango seleccionado.")
    else:
        df_plot = df_rank.sort_values(ordenar_por, ascending=True)
        y_col = "comprador" if "comprador" in df_plot.columns else "c_comprador"

        fig = px.bar(
            df_plot,
            x=ordenar_por,
            y=y_col,
            orientation="h",
            title=f"Top {topn} Compradores por {ordenar_por.replace('_', ' ').title()} (OC CONNEXA)",
            text=ordenar_por,
        )
        fig.update_layout(
            yaxis_title="",
            xaxis_title=ordenar_por.replace("_", " ").title(),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Detalle")
        st.dataframe(df_rank, width="stretch", hide_index=True)
        st.download_button(
            "Descargar CSV",
            data=df_rank.to_csv(index=False).encode("utf-8"),
            file_name="ranking_compradores_oc_connexa.csv",
            mime="text/csv",
        )

st.caption(
    "Fuente: mon.v_oc_generadas_mensual / mon.v_oc_generadas_mensual_ext "
    "(basadas en public.t080_oc_precarga_connexa)."
)

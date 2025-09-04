
import os
import pandas as pd
import streamlit as st
from modules.db import get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries import SQL_RANKING_COMPRADORES, SQL_RANKING_COMPRADORES_NOMBRE
import plotly.express as px

st.set_page_config(page_title="Indicador 5 — Ranking Compradores", page_icon="🏆", layout="wide")
render_header("Indicador 5 — Ranking de Compradores que más usan el sistema")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS","300"))

@st.cache_data(ttl=ttl)
def fetch_ranking(desde, hasta, topn=20):
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(SQL_RANKING_COMPRADORES_NOMBRE, con, params={"desde": desde, "hasta": hasta, "topn": topn})
    # tipos amigables
    df["oc_total"] = pd.to_numeric(df["oc_total"], errors="coerce").astype("Int64")
    df["bultos_total"] = pd.to_numeric(df["bultos_total"], errors="coerce")
    return df

col_filtros = st.columns([1,1,2])
with col_filtros[0]:
    topn = st.slider("Top N", min_value=5, max_value=50, value=20, step=5)
with col_filtros[1]:
    ordenar_por = st.selectbox("Ordenar por", options=["oc_total", "bultos_total"], index=0)

df = fetch_ranking(desde, hasta, topn=topn)

if df.empty:
    st.info("No hay datos para el rango seleccionado.")
else:
    # Ordenamiento para el gráfico
    df_plot = df.sort_values(ordenar_por, ascending=True)
    fig = px.bar(
        df_plot,
        x=ordenar_por,
        y="comprador",
        orientation="h",
        title=f"Top {topn} Compradores por {ordenar_por.replace('_',' ').title()}",
        text=ordenar_por,
    )
    fig.update_layout(yaxis_title="", xaxis_title=ordenar_por.replace("_", " ").title())
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Detalle")
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.download_button(
        "Descargar CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="ranking_compradores_con_nombres.csv",
        mime="text/csv",
    )

st.caption("Fuente: mon.v_oc_generadas_mensual_ext (JOIN a src.m_9_compradores).")
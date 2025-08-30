
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

st.set_page_config(page_title="Indicador 1 — OC Generadas", page_icon="🧾", layout="wide")
render_header("Indicador 1 — Generación de OC desde CONNEXA")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS","300"))

@st.cache_data(ttl=ttl)
def fetch_oc_generadas(desde, hasta, usar_nombres: bool = True):
    eng = get_pg_engine()
    with eng.connect() as con:
        if usar_nombres:
            df = pd.read_sql(SQL_OC_GENERADAS_RANGO_EXT, con, params={"desde": desde, "hasta": hasta})
        else:
            df = pd.read_sql(SQL_OC_GENERADAS_RANGO, con, params={"desde": desde, "hasta": hasta})
    # normalización tipos
    if "c_proveedor" in df.columns:
        df["c_proveedor"] = df["c_proveedor"].astype("Int64")
    return df

@st.cache_data(ttl=ttl)
def fetch_ranking(desde, hasta, topn=10, usar_nombres: bool = True):
    eng = get_pg_engine()
    with eng.connect() as con:
        if usar_nombres:
            df = pd.read_sql(SQL_RANKING_COMPRADORES_NOMBRE, con, params={"desde": desde, "hasta": hasta, "topn": topn})
        else:
            df = pd.read_sql(SQL_RANKING_COMPRADORES, con, params={"desde": desde, "hasta": hasta, "topn": topn})
    return df

usar_nombres = st.toggle("Mostrar nombres (compradores/proveedores)", value=True)

df = fetch_oc_generadas(desde, hasta, usar_nombres=usar_nombres)
st.caption(f"{len(df)} filas agregadas (mes-comprador-proveedor)")

with st.expander("Ver tabla agregada"):
    st.dataframe(df, use_container_width=True, hide_index=True)

# Totales por mes
tot_mes = df.groupby("mes", as_index=False).agg(total_oc=("total_oc","sum"), total_bultos=("total_bultos","sum"))
col1, col2 = st.columns(2)
with col1:
    fig1 = px.bar(tot_mes, x="mes", y="total_oc", title="Total de OC por Mes")
    st.plotly_chart(fig1, use_container_width=True)
with col2:
    fig2 = px.line(tot_mes, x="mes", y="total_bultos", title="Total de Bultos por Mes")
    st.plotly_chart(fig2, use_container_width=True)

# Ranking de compradores
rk = fetch_ranking(desde, hasta, topn=10, usar_nombres=usar_nombres)
col3, col4 = st.columns([2,1])
with col3:
    y_col = "comprador" if "comprador" in rk.columns else "c_comprador"
    fig3 = px.bar(rk.sort_values("oc_total"), x="oc_total", y=y_col, orientation="h", title="Top Compradores por #OC")
    st.plotly_chart(fig3, use_container_width=True)
with col4:
    st.metric("OC (rango)", value=int(tot_mes["total_oc"].sum()))
    st.metric("Bultos (rango)", value=float(tot_mes["total_bultos"].sum()))


import os
import pandas as pd
import streamlit as st
from modules.db import get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries import SQL_RANKING_COMPRADORES
import plotly.express as px

st.set_page_config(page_title="Indicador 5 — Ranking Compradores", page_icon="🏆", layout="wide")
render_header("Indicador 5 — Ranking de Compradores que más usan el sistema")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS","300"))

@st.cache_data(ttl=ttl)
def fetch_ranking(desde, hasta, topn=20):
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(SQL_RANKING_COMPRADORES, con, params={"desde": desde, "hasta": hasta, "topn": topn})
    return df

topn = st.slider("Top N", 5, 50, 20, step=5)
rk = fetch_ranking(desde, hasta, topn=topn)

col1, col2 = st.columns([2,1])
with col1:
    fig = px.bar(rk.sort_values("oc_total"), x="oc_total", y="c_comprador", orientation="h", title="Top Compradores por #OC (rango)")
    st.plotly_chart(fig, use_container_width=True)
with col2:
    st.dataframe(rk, use_container_width=True, hide_index=True)


import os
import pandas as pd
import plotly.express as px
import streamlit as st
from modules.db import get_sqlserver_engine, get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_SGM_KIKKER_VS_OC_MENSUAL,
    SQL_SGM_KIKKER_DETALLE,
    SQL_SGM_KIKKER_DUP,
    SQL_PG_KIKKER_MENSUAL,
    SQL_C_COMPRA_KIKKER_GEN,
)
from datetime import timedelta

st.set_page_config(page_title="Indicador 2 — SGM (CONNEXA → SGM)", page_icon="✅", layout="wide")
render_header("Indicador 2 — OC en SGM generadas desde CONNEXA")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

@st.cache_data(ttl=ttl)
def fetch_sgm_mensual(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None: return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_KIKKER_VS_OC_MENSUAL, con, params={"desde": desde, "hasta": hasta})
    return df

@st.cache_data(ttl=ttl)
def fetch_sgm_detalle(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None: return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_KIKKER_DETALLE, con, params={"desde": desde, "hasta": hasta})
    return df

@st.cache_data(ttl=ttl)
def fetch_pg_kikker_mensual(desde, hasta):
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(SQL_PG_KIKKER_MENSUAL, con, params={"desde": desde, "hasta": hasta})
    return df

@st.cache_data(ttl=ttl)
def fetch_sgm_dup(desde, hasta):
    eng = get_sqlserver_engine()
    if eng is None: return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_KIKKER_DUP, con, params={"desde": desde, "hasta": hasta})
    return df

tab_gen, tab_recon, tab_apr = st.tabs(["Generadas (mensual)", "Reconciliación por KIKKER", "Aprobadas (pendiente)"])

with tab_gen:
    sgm_m = fetch_sgm_mensual(desde, hasta)
    if sgm_m.empty:
        st.info("Sin datos SGM o falta configurar SQL Server en .env")
    else:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(sgm_m, x="mes", y="kikker_distintos", title="KIKKER distintos en SGM (originados en CONNEXA)")
            st.plotly_chart(fig, width="stretch")
        with col2:
            fig2 = px.bar(sgm_m, x="mes", y="oc_sgm_distintas", title="OC SGM distintas (resultado)")
            st.plotly_chart(fig2, width="stretch")

        st.caption("Comparar ambas curvas: deberían ser ~iguales si la relación es 1:1. Desvíos indican splitting/duplicaciones o diferencias de corte.")
        st.dataframe(sgm_m, width="stretch", hide_index=True)
with tab_recon:
    sgm_d = fetch_sgm_detalle(desde, hasta)
    pg_m  = fetch_pg_kikker_mensual(desde, hasta)
    dup   = fetch_sgm_dup(desde, hasta)

    if sgm_d.empty:
        st.info("Sin detalle SGM para reconciliar.")
    else:
        # set de KIKKER en SGM
        eng_pg = get_pg_engine()
        hasta_mas_1 = hasta + timedelta(days=1) # type: ignore

        with eng_pg.connect() as con:
            df_pg_k = pd.read_sql(SQL_C_COMPRA_KIKKER_GEN, con, params={"desde": desde, "hasta_mas_1": hasta_mas_1}) # type: ignore


        sgm_k = sgm_d[["C_COMPRA_KIKKER"]].dropna().drop_duplicates().rename(columns={"C_COMPRA_KIKKER":"kikker"})
        recon = sgm_k.merge(df_pg_k, on="kikker", how="outer", indicator=True)
        recon["estado"] = recon["_merge"].map({
            "both": "OK (1:1)",
            "left_only": "SGM sin CONNEXA",
            "right_only": "CONNEXA sin SGM",
        })
        kpis = recon["estado"].value_counts().to_frame("kikkers").reset_index().rename(columns={"index":"estado"})

        colA, colB = st.columns([1,2])
        with colA:
            st.subheader("KPIs de reconciliación")
            st.dataframe(kpis, width="stretch", hide_index=True)
            if not dup.empty:
                st.metric("KIKKER con >1 OC SGM", int(dup.shape[0]))
        with colB:
            if not pg_m.empty:
                fig = px.line(pg_m, x="mes", y="kikker_distintos_pg", title="KIKKER distintos en CONNEXA (PG)")
                st.plotly_chart(fig, width="stretch")

        st.divider()
        with st.expander("Duplicaciones en SGM (KIKKER → múltiples OC SGM)"):
            st.dataframe(dup, width="stretch", hide_index=True)

        st.divider()
        with st.expander("Listado de reconciliación (KIKKER)"):
            st.dataframe(recon[["kikker","estado"]], width="stretch", hide_index=True)
            st.download_button(
                "Descargar CSV de reconciliación",
                data=recon.to_csv(index=False).encode("utf-8"),
                file_name="reconciliacion_kikker.csv",
                mime="text/csv",
            )

# -------
# with tab_recon:
#     sgm_d = fetch_sgm_detalle(desde, hasta)
#     pg_m  = fetch_pg_kikker_mensual(desde, hasta)
#     dup   = fetch_sgm_dup(desde, hasta)

#     if sgm_d.empty:
#         st.info("Sin detalle SGM para reconciliar.")
#     else:
#         # set de KIKKER en SGM
#         sgm_k = sgm_d[["C_COMPRA_KIKKER"]].dropna().drop_duplicates().rename(columns={"C_COMPRA_KIKKER":"kikker"})
#         # set de KIKKER en PG (CONNEXA)
#         eng_pg = get_pg_engine()
#         with eng_pg.connect() as con:
#             df_pg_k = pd.read_sql(SQL_C_COMPRA_KIKKERP_GEN, con, params={"desde": desde, "hasta": hasta})      

#         # join para reconciliación
#         recon = sgm_k.merge(df_pg_k, on="kikker", how="outer", indicator=True)
#         recon["estado"] = recon["_merge"].map({
#             "both": "OK (1:1)",
#             "left_only": "SGM sin CONNEXA",
#             "right_only": "CONNEXA sin SGM",
#         })
#         kpis = recon["estado"].value_counts().to_frame("kikkers").reset_index().rename(columns={"index":"estado"})

#         colA, colB = st.columns([1,2])
#         with colA:
#             st.subheader("KPIs de reconciliación")
#             st.dataframe(kpis, width="stretch", hide_index=True)
#             if not dup.empty:
#                 st.metric("KIKKER con >1 OC SGM", int(dup.shape[0]))
#         with colB:
#             if not pg_m.empty:
#                 fig = px.line(pg_m, x="mes", y="kikker_distintos_pg", title="KIKKER distintos en CONNEXA (PG)")
#                 st.plotly_chart(fig, width="stretch")

#         st.divider()
#         with st.expander("Duplicaciones en SGM (KIKKER → múltiples OC SGM)"):
#             st.dataframe(dup, width="stretch", hide_index=True)

#         st.divider()
#         with st.expander("Listado de reconciliación (KIKKER)"):
#             st.dataframe(recon[["kikker","estado"]], width="stretch", hide_index=True)
#             st.download_button(
#                 "Descargar CSV de reconciliación",
#                 data=recon.to_csv(index=False).encode("utf-8"),
#                 file_name="reconciliacion_kikker.csv",
#                 mime="text/csv",
#             )

with tab_apr:
    st.info("Pendiente conectar con la tabla/campos reales de aprobación en SGM (campo de fecha de aprobación).")


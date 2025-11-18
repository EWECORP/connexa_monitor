# 07_Indicador_7_Ventas_Proveedor.py
# Indicador 7 — Ventas por Proveedor y Sucursal (7 / 15 / 30 / 90 días)

import os
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import timedelta

from modules.db import get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_VENTAS_PROVEEDOR,
    QRY_PROVEEDORES,
)

# -------------------------
# Configuración de Página
# -------------------------
st.set_page_config(
    page_title="Indicador 7 — Ventas por Proveedor",
    page_icon="📊",
    layout="wide"
)
render_header("Indicador 7 — Ventas por Proveedor y Sucursal (7 / 15 / 30 / 90 días)")

# -------------------------
# Filtros de Fecha
# -------------------------
# Usamos 'hasta' como ancla; traemos siempre hasta 90 días hacia atrás
desde_ui, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# -------------------------
# Cache: cargar proveedores
# -------------------------
@st.cache_data(ttl=ttl)
def fetch_proveedores():
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(QRY_PROVEEDORES, con)
    df = df.sort_values("c_proveedor")
    return df

# Consulta principal (trae SIEMPRE 90 días hacia atrás desde 'hasta')
@st.cache_data(ttl=ttl)
def fetch_ventas_proveedor(hasta, proveedor):
    """
    Trae datos de ventas para el proveedor en los últimos 90 días
    respecto de la fecha 'hasta' (inclusive).
    """
    hasta = pd.to_datetime(hasta).date()
    desde_90 = hasta - timedelta(days=90)  # rango amplio para calcular 7/15/30/90

    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_VENTAS_PROVEEDOR,
            con,
            params={
                "desde": desde_90,
                "hasta": hasta,
                "proveedor": proveedor
            },
        )
    return df


# -------------------------
# Selección de Proveedor
# -------------------------
df_prov = fetch_proveedores()
lista_prov = df_prov["c_proveedor"].tolist()

col_p = st.columns([2, 1])
with col_p[0]:
    proveedor_sel = st.selectbox(
        "Proveedor",
        options=lista_prov,
        index=0,
        format_func=lambda x: f"{int(x)}"
    )
with col_p[1]:
    st.write(" ")

# -------------------------
# Obtener Ventas del proveedor (últimos 90 días)
# -------------------------
df = fetch_ventas_proveedor(hasta, proveedor_sel)

if df.empty:
    st.info("No hay ventas para el rango de 90 días y proveedor seleccionado.")
    st.stop()

# Normalizaciones
df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce").fillna(0.0)
df["fecha"] = pd.to_datetime(df["fecha"])

# -------------------------
# Filtro opcional de Sucursal
# -------------------------
sucursales = df["codigo_sucursal"].dropna().unique().tolist()
sucursal_sel = st.selectbox(
    "Sucursal (opcional — TODAS)",
    options=["TODAS"] + sucursales
)

if sucursal_sel != "TODAS":
    df = df[df["codigo_sucursal"] == sucursal_sel]

if df.empty:
    st.info("No hay ventas para la sucursal seleccionada en los últimos 90 días.")
    st.stop()

# -------------------------
# Construcción de ventanas 7 / 15 / 30 / 90 días
# -------------------------
anchor = pd.to_datetime(hasta)

windows = [7, 15, 30, 90]
totales = {}
suc_dict = {}
art_dict = {}

for d in windows:
    # Ventana: últimos d días, inclusive 'hasta'
    d_from = anchor - pd.Timedelta(days=d - 1)
    mask = (df["fecha"] >= d_from) & (df["fecha"] <= anchor)
    df_w = df.loc[mask].copy()

    totales[d] = {
        "unidades": df_w["unidades"].sum(),
        "articulos": df_w["codigo_articulo"].nunique(),
        "sucursales": df_w["codigo_sucursal"].nunique(),
    }

    # Agregación por sucursal
    df_suc = (
        df_w.groupby(["codigo_sucursal", "suc_nombre"], as_index=False)
             .agg(unidades=("unidades", "sum"))
    )
    suc_dict[d] = df_suc

    # Agregación por artículo
    df_art = (
        df_w.groupby(["codigo_articulo"], as_index=False)
             .agg(unidades=("unidades", "sum"))
    )
    art_dict[d] = df_art

# -------------------------
# KPIs del rango por ventana
# -------------------------
st.subheader("KPIs por horizonte (7 / 15 / 30 / 90 días)")

row1 = st.columns(4)
row1[0].metric("Unidades 7 días",  f"{totales[7]['unidades']:,.0f}")
row1[1].metric("Unidades 15 días", f"{totales[15]['unidades']:,.0f}")
row1[2].metric("Unidades 30 días", f"{totales[30]['unidades']:,.0f}")
row1[3].metric("Unidades 90 días", f"{totales[90]['unidades']:,.0f}")

row2 = st.columns(4)
row2[0].metric("Artículos 7 días",  totales[7]["articulos"])
row2[1].metric("Artículos 15 días", totales[15]["articulos"])
row2[2].metric("Artículos 30 días", totales[30]["articulos"])
row2[3].metric("Artículos 90 días", totales[90]["articulos"])

st.caption(
    "Las métricas se calculan siempre hacia atrás desde la fecha 'Hasta' seleccionada, "
    "limitadas a los últimos 90 días."
)

# -------------------------
# Selección de horizonte para gráficos
# -------------------------
st.subheader("Detalle por horizonte")
horizonte = st.selectbox(
    "Horizonte (días)",
    options=windows,
    index=2,  # 30 días por defecto
)

df_suc_h = suc_dict[horizonte]
df_art_h = art_dict[horizonte]

col_g1, col_g2 = st.columns(2)

# Gráfico: Unidades por Sucursal para el horizonte elegido
with col_g1:
    st.markdown(f"**Unidades por Sucursal — Últimos {horizonte} días**")
    if df_suc_h.empty:
        st.info("Sin ventas para este horizonte y selección.")
    else:
        fig_suc = px.bar(
            df_suc_h.sort_values("unidades", ascending=True),
            x="unidades",
            y="suc_nombre",
            orientation="h",
            title=f"Unidades vendidas por Sucursal (últimos {horizonte} días)",
        )
        st.plotly_chart(fig_suc, use_container_width=True)

# Gráfico: Ranking de artículos para el horizonte elegido
with col_g2:
    st.markdown(f"**Top Artículos por Unidades — Últimos {horizonte} días**")
    if df_art_h.empty:
        st.info("Sin ventas para este horizonte y selección.")
    else:
        df_art_top = df_art_h.sort_values("unidades").tail(20)
        fig_art = px.bar(
            df_art_top,
            x="unidades",
            y="codigo_articulo",
            orientation="h",
            title=f"Top 20 Artículos por Unidades (últimos {horizonte} días)",
        )
        st.plotly_chart(fig_art, use_container_width=True)

# -------------------------
# Detalle exportable para el horizonte seleccionado
# -------------------------
st.subheader(f"Detalle de ventas — últimos {horizonte} días")

d_from_sel = anchor - pd.Timedelta(days=horizonte - 1)
mask_sel = (df["fecha"] >= d_from_sel) & (df["fecha"] <= anchor)
df_det = df.loc[mask_sel].copy()

st.dataframe(df_det, use_container_width=True, hide_index=True)

st.download_button(
    f"Descargar CSV ({horizonte} días)",
    data=df_det.to_csv(index=False).encode("utf-8"),
    file_name=f"ventas_proveedor_{int(proveedor_sel)}_{horizonte}d.csv",
    mime="text/csv",
)

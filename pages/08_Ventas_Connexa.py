# 08_Ventas_Connexa.py
# Indicador 8 — Ventas por Proveedor y Sucursal (7 / 15 / 30 / 90 días)

import os
from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries.stock_ventas import (
    SQL_VENTAS_PROVEEDOR,
    QRY_PROVEEDORES,
    QRY_PROVEEDOR_COMPRADOR,  # <- NUEVO (ver nota en queries)
)

# -------------------------
# Configuración de Página
# -------------------------
st.set_page_config(
    page_title="Indicador 8 — Ventas por Proveedor",
    page_icon="📊",
    layout="wide",
)
render_header("Indicador 8 — Ventas por Proveedor y Sucursal (7 / 15 / 30 / 90 días)")

ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# -------------------------
# Filtros de Fecha
# -------------------------
# Se usa 'hasta' como ancla; se trae SIEMPRE hasta 90 días hacia atrás.
_, hasta = make_date_filters()

# -------------------------
# Cache: catálogos / dimensiones
# -------------------------
@st.cache_data(ttl=ttl, show_spinner=False)
def fetch_proveedores() -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(QRY_PROVEEDORES, con)
    # normalización
    df["c_proveedor"] = pd.to_numeric(df["c_proveedor"], errors="coerce").astype("Int64")
    df["n_proveedor"] = df["n_proveedor"].astype(str).str.strip()
    df = df.dropna(subset=["c_proveedor"]).sort_values(["n_proveedor", "c_proveedor"])
    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def fetch_proveedor_comprador() -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(QRY_PROVEEDOR_COMPRADOR, con)
    # normalización
    df["c_proveedor"] = pd.to_numeric(df["c_proveedor"], errors="coerce").astype("Int64")
    df["cod_comprador"] = pd.to_numeric(df["cod_comprador"], errors="coerce").astype("Int64")
    df["n_comprador"] = df["n_comprador"].astype(str).str.strip()
    return df


# Consulta principal (trae SIEMPRE 90 días hacia atrás desde 'hasta')
@st.cache_data(ttl=ttl, show_spinner=True)
def fetch_ventas_proveedor(hasta_date, proveedor: int) -> pd.DataFrame:
    """
    Trae datos de ventas para el proveedor en los últimos 90 días
    respecto de la fecha 'hasta' (inclusive).
    """
    hasta_date = pd.to_datetime(hasta_date).date()
    desde_90 = hasta_date - timedelta(days=90)

    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_VENTAS_PROVEEDOR,
            con,
            params={
                "desde": desde_90,
                "hasta": hasta_date,
                "proveedor": int(proveedor),
            },
        )
    return df


# -------------------------
# Dataframes base
# -------------------------
df_prov = fetch_proveedores()
df_map = fetch_proveedor_comprador()

# Enriquecer catálogo de proveedores con comprador (si existe)
df_cat = df_prov.merge(df_map[["c_proveedor", "cod_comprador", "n_comprador"]], on="c_proveedor", how="left")

# Listas para filtros
compradores = (
    df_cat[["cod_comprador", "n_comprador"]]
    .dropna(subset=["cod_comprador"])
    .drop_duplicates()
    .sort_values(["n_comprador", "cod_comprador"])
)
opt_compradores = [("TODOS", "TODOS")] + [
    (int(r["cod_comprador"]), f"{int(r['cod_comprador'])} — {r['n_comprador']}")
    for _, r in compradores.iterrows()
]

# -------------------------
# Panel de filtros
# -------------------------
st.subheader("Filtros")

col_f1, col_f2, col_f3 = st.columns([1.2, 2.5, 1.0])

with col_f1:
    comprador_sel = st.selectbox(
        "Comprador (opcional)",
        options=opt_compradores,
        index=0,
        format_func=lambda x: x[1],
    )[0]

with col_f2:
    texto_busqueda = st.text_input(
        "Buscar proveedor (código o nombre)",
        value="",
        help="Admite búsqueda parcial por código o por nombre. Ej: 'La Serenísima', '1234', 'acme'.",
    ).strip().lower()

with col_f3:
    horizonte_default = st.selectbox("Horizonte default", options=[7, 15, 30, 90], index=2)

# Filtrar catálogo de proveedores según comprador y texto
df_f = df_cat.copy()

if comprador_sel != "TODOS":
    df_f = df_f[df_f["cod_comprador"].astype("Int64") == int(comprador_sel)]

if texto_busqueda:
    mask = (
        df_f["c_proveedor"].astype(str).str.contains(texto_busqueda, na=False)
        | df_f["n_proveedor"].str.lower().str.contains(texto_busqueda, na=False)
    )
    df_f = df_f[mask]

df_f = df_f.sort_values(["n_proveedor", "c_proveedor"])

# Preparar opciones del select de proveedor
prov_opts = [
    (
        int(r["c_proveedor"]),
        f"{int(r['c_proveedor'])} — {r['n_proveedor']}"
        + (f"  |  Comprador: {r['n_comprador']}" if pd.notna(r.get("n_comprador")) and str(r.get("n_comprador")).strip() else ""),
    )
    for _, r in df_f.iterrows()
]

# Si la búsqueda deja sin resultados, informar y cortar temprano
if len(prov_opts) == 0:
    st.warning("No se encontraron proveedores con los filtros aplicados.")
    st.stop()

col_p1, col_p2 = st.columns([3, 1])
with col_p1:
    proveedor_sel = st.selectbox(
        "Proveedor",
        options=prov_opts,
        index=0,
        format_func=lambda x: x[1],
    )[0]
with col_p2:
    aplicar = st.button("Aplicar filtros", type="primary", use_container_width=True)

# Control de ejecución bajo demanda:
# - La primera vez, se ejecuta automáticamente.
# - Luego, sólo se re-ejecuta la consulta si se presiona el botón.
if "prov_last" not in st.session_state:
    st.session_state["prov_last"] = proveedor_sel
    st.session_state["hasta_last"] = hasta
    st.session_state["run"] = True

if aplicar:
    st.session_state["prov_last"] = proveedor_sel
    st.session_state["hasta_last"] = hasta
    st.session_state["run"] = True

# Encabezado descriptivo del proveedor seleccionado
prov_row = df_cat[df_cat["c_proveedor"].astype("Int64") == int(st.session_state["prov_last"])].head(1)
prov_name = prov_row["n_proveedor"].iloc[0] if not prov_row.empty else str(st.session_state["prov_last"])
prov_buyer = prov_row["n_comprador"].iloc[0] if (not prov_row.empty and "n_comprador" in prov_row.columns) else None

st.caption(
    f"Proveedor seleccionado: **{int(st.session_state['prov_last'])} — {prov_name}**"
    + (f" | Comprador: **{prov_buyer}**" if prov_buyer and str(prov_buyer).strip() else "")
)

# -------------------------
# Obtener Ventas del proveedor (últimos 90 días)
# -------------------------
if not st.session_state.get("run", False):
    st.stop()

df = fetch_ventas_proveedor(st.session_state["hasta_last"], st.session_state["prov_last"])
st.session_state["run"] = False  # se consume el “gatillo”

if df.empty:
    st.info("No hay ventas para el rango de 90 días y proveedor seleccionado.")
    st.stop()

# -------------------------
# Normalizaciones
# -------------------------
# Esperados: fecha, unidades, codigo_sucursal, suc_nombre, codigo_articulo
df["unidades"] = pd.to_numeric(df.get("unidades"), errors="coerce").fillna(0.0)
df["fecha"] = pd.to_datetime(df.get("fecha"), errors="coerce")

# limpiar sucursales
if "codigo_sucursal" in df.columns:
    df["codigo_sucursal"] = df["codigo_sucursal"].astype(str).str.strip()

if "suc_nombre" in df.columns:
    df["suc_nombre"] = df["suc_nombre"].astype(str).str.strip()

# -------------------------
# Filtro opcional de Sucursal
# -------------------------
st.divider()
st.subheader("Segmentación")

col_s1, col_s2 = st.columns([2, 1])
with col_s1:
    sucursales = (
        df[["codigo_sucursal", "suc_nombre"]]
        .dropna(subset=["codigo_sucursal"])
        .drop_duplicates()
        .sort_values(["suc_nombre", "codigo_sucursal"])
    )
    suc_opts = ["TODAS"] + [
        f"{r['codigo_sucursal']} — {r['suc_nombre']}" if pd.notna(r.get("suc_nombre")) and str(r.get("suc_nombre")).strip()
        else f"{r['codigo_sucursal']}"
        for _, r in sucursales.iterrows()
    ]
    suc_sel_label = st.selectbox("Sucursal (opcional)", options=suc_opts, index=0)

with col_s2:
    top_art = st.slider("Top artículos", min_value=10, max_value=100, value=20, step=10)

if suc_sel_label != "TODAS":
    suc_code = suc_sel_label.split("—")[0].strip()
    df = df[df["codigo_sucursal"] == suc_code]

if df.empty:
    st.info("No hay ventas para la sucursal seleccionada en los últimos 90 días.")
    st.stop()

# -------------------------
# Construcción de ventanas 7 / 15 / 30 / 90 días
# -------------------------
anchor = pd.to_datetime(st.session_state["hasta_last"])

windows = [7, 15, 30, 90]
totales = {}
suc_dict = {}
art_dict = {}

for d in windows:
    d_from = anchor - pd.Timedelta(days=d - 1)
    mask = (df["fecha"] >= d_from) & (df["fecha"] <= anchor)
    df_w = df.loc[mask].copy()

    totales[d] = {
        "unidades": float(df_w["unidades"].sum()),
        "articulos": int(df_w["codigo_articulo"].nunique()) if "codigo_articulo" in df_w.columns else 0,
        "sucursales": int(df_w["codigo_sucursal"].nunique()) if "codigo_sucursal" in df_w.columns else 0,
        "dias": int(df_w["fecha"].dt.date.nunique()) if "fecha" in df_w.columns else 0,
    }

    # Agregación por sucursal
    if "codigo_sucursal" in df_w.columns:
        if "suc_nombre" in df_w.columns:
            df_suc = (
                df_w.groupby(["codigo_sucursal", "suc_nombre"], as_index=False)
                    .agg(unidades=("unidades", "sum"))
            )
            df_suc["label"] = df_suc.apply(
                lambda r: f"{r['codigo_sucursal']} — {r['suc_nombre']}" if str(r.get("suc_nombre", "")).strip() else str(r["codigo_sucursal"]),
                axis=1
            )
        else:
            df_suc = (
                df_w.groupby(["codigo_sucursal"], as_index=False)
                    .agg(unidades=("unidades", "sum"))
            )
            df_suc["label"] = df_suc["codigo_sucursal"].astype(str)
    else:
        df_suc = pd.DataFrame(columns=["label", "unidades"])
    suc_dict[d] = df_suc

    # Agregación por artículo
    if "codigo_articulo" in df_w.columns:
        df_art = (
            df_w.groupby(["codigo_articulo"], as_index=False)
                .agg(unidades=("unidades", "sum"))
        )
    else:
        df_art = pd.DataFrame(columns=["codigo_articulo", "unidades"])
    art_dict[d] = df_art

# -------------------------
# KPIs por ventana
# -------------------------
st.divider()
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

row3 = st.columns(4)
row3[0].metric("Sucursales 7 días",  totales[7]["sucursales"])
row3[1].metric("Sucursales 15 días", totales[15]["sucursales"])
row3[2].metric("Sucursales 30 días", totales[30]["sucursales"])
row3[3].metric("Sucursales 90 días", totales[90]["sucursales"])

st.caption(
    "Las métricas se calculan hacia atrás desde la fecha 'Hasta' seleccionada, "
    "limitadas a los últimos 90 días."
)

# -------------------------
# Selección de horizonte para gráficos
# -------------------------
st.subheader("Detalle por horizonte")
horizonte = st.selectbox(
    "Horizonte (días)",
    options=windows,
    index=windows.index(horizonte_default),
)

df_suc_h = suc_dict[horizonte]
df_art_h = art_dict[horizonte]

col_g1, col_g2 = st.columns(2)

# Gráfico: Unidades por Sucursal
with col_g1:
    st.markdown(f"**Unidades por Sucursal — Últimos {horizonte} días**")
    if df_suc_h.empty:
        st.info("Sin ventas para este horizonte y selección.")
    else:
        df_suc_plot = df_suc_h.sort_values("unidades", ascending=True)
        fig_suc = px.bar(
            df_suc_plot,
            x="unidades",
            y="label",
            orientation="h",
            title=f"Unidades vendidas por Sucursal (últimos {horizonte} días)",
        )
        st.plotly_chart(fig_suc, use_container_width=True)

# Gráfico: Ranking de artículos
with col_g2:
    st.markdown(f"**Top Artículos por Unidades — Últimos {horizonte} días**")
    if df_art_h.empty:
        st.info("Sin ventas para este horizonte y selección.")
    else:
        df_art_top = df_art_h.sort_values("unidades").tail(int(top_art))
        fig_art = px.bar(
            df_art_top,
            x="unidades",
            y="codigo_articulo",
            orientation="h",
            title=f"Top {top_art} Artículos por Unidades (últimos {horizonte} días)",
        )
        st.plotly_chart(fig_art, use_container_width=True)

# -------------------------
# Detalle exportable
# -------------------------
st.divider()
st.subheader(f"Detalle de ventas — últimos {horizonte} días")

d_from_sel = anchor - pd.Timedelta(days=horizonte - 1)
mask_sel = (df["fecha"] >= d_from_sel) & (df["fecha"] <= anchor)
df_det = df.loc[mask_sel].copy()

# Columnas “amigables” si existen
cols_prefer = [
    "fecha",
    "codigo_sucursal",
    "suc_nombre",
    "codigo_articulo",
    "unidades",
]
cols = [c for c in cols_prefer if c in df_det.columns] + [c for c in df_det.columns if c not in cols_prefer]
df_det = df_det[cols].sort_values(["fecha"] + (["codigo_sucursal"] if "codigo_sucursal" in df_det.columns else []), ascending=[False] + ([True] if "codigo_sucursal" in df_det.columns else []))

st.dataframe(df_det, use_container_width=True, hide_index=True)

st.download_button(
    f"Descargar CSV ({horizonte} días)",
    data=df_det.to_csv(index=False).encode("utf-8"),
    file_name=f"ventas_proveedor_{int(st.session_state['prov_last'])}_{horizonte}d.csv",
    mime="text/csv",
)
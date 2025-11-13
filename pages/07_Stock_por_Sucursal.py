# pages/07_Stock_por_Sucursal.py
# Panel: Control de Stock por Sucursal y Proveedor
#
# Objetivo:
#   Permitir a los compradores visualizar el stock actual por artículo-sucursal,
#   filtrado por sucursal y proveedor, identificando:
#     - Artículos por debajo del stock mínimo.
#     - Artículos con sobre-stock (muchos días de stock).
#     - Valor del stock por proveedor.
#
# Requisitos:
#   - Tablas utilizadas:
#       src.base_stock_sucursal
#       src.base_productos_vigentes
#
#   - Materialized View:
#       mv_stock_cartera_30d

# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np

from modules.db import get_diarco_engine
from modules.queries import (
    QRY_MV_STOCK_CARTERA_30D,
    QRY_COMPPRADORES,
    QRY_PROVEEDORES,
)

# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================
st.set_page_config(
    page_title="Stock por Sucursal",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📦 Control de Stock por Sucursal, Proveedor y Comprador")
st.markdown(
    """
Panel para analizar **stock**, **días de cobertura**,  
parámetros mínimos y tolerancias configurables, por **comprador** y **proveedor**.
"""
)

# ============================================================
# 1. CARGA DE DATOS
# ============================================================

@st.cache_data(show_spinner=True)
def load_mv() -> pd.DataFrame:
    """Carga la vista materializada de stock + ventas + parámetros."""
    engine = get_diarco_engine()
    return pd.read_sql(QRY_MV_STOCK_CARTERA_30D, con=engine)


@st.cache_data(show_spinner=False)
def load_compradores() -> pd.DataFrame:
    """Carga catálogo de compradores."""
    engine = get_diarco_engine()
    return pd.read_sql(QRY_COMPPRADORES, con=engine)


@st.cache_data(show_spinner=False)
def load_proveedores() -> pd.DataFrame:
    """Carga catálogo de proveedores."""
    engine = get_diarco_engine()
    return pd.read_sql(QRY_PROVEEDORES, con=engine)


@st.cache_data(show_spinner=True)
def preparar_dataset() -> pd.DataFrame:
    """Normaliza y enriquece el dataset base a partir de la MV."""
    df = load_mv()

    # Columnas de seguridad
    if "stock_reserva" not in df.columns:
        df["stock_reserva"] = 0

    if "venta_promedio_diaria_30d" not in df.columns:
        df["venta_promedio_diaria_30d"] = 0
    df["venta_promedio_diaria_30d"] = df["venta_promedio_diaria_30d"].fillna(0)

    # Stock total (stock + reserva)
    df["stock"] = df["stock"].fillna(0)
    df["stock_total"] = df["stock"] + df["stock_reserva"].fillna(0)

    # Días de stock: usamos lo calculado en la MV si existe,
    # si no, lo recalculamos en base a la venta promedio 30d
    if "dias_stock" in df.columns:
        df["dias_stock"] = pd.to_numeric(df["dias_stock"], errors="coerce")
    else:
        df["dias_stock"] = np.where(
            df["venta_promedio_diaria_30d"] > 0,
            df["stock_total"] / df["venta_promedio_diaria_30d"],
            np.nan,
        )

    # Parámetros mínimos/máximos
    if "q_dias_stock" not in df.columns:
        df["q_dias_stock"] = np.nan
    if "q_dias_sobre_stock" not in df.columns:
        df["q_dias_sobre_stock"] = np.nan

    # Stock mínimo
    if "stock_minimo" not in df.columns:
        df["stock_minimo"] = 0
    df["stock_minimo"] = df["stock_minimo"].fillna(0)

    # Flags de negocio base
    df["bajo_stock_minimo"] = df["stock_total"] < df["stock_minimo"]

    df["sin_venta_30d_con_stock"] = (
        (df["venta_promedio_diaria_30d"] == 0) & (df["stock_total"] > 0)
    )

    df["valor_stock_costo"] = df["stock_total"] * df["precio_costo"].fillna(0)

    # Falta de cobertura estructural (días reales < mínimos parametrizados)
    df["falta_cobertura"] = np.where(
        df["q_dias_stock"].notna(),
        df["dias_stock"] < df["q_dias_stock"],
        False,
    )

    # sobre_stock_param lo vamos a recalcular dinámicamente en función de la tolerancia
    df["sobre_stock_param"] = False

    # Normalizar tipos clave
    df["codigo_sucursal"] = pd.to_numeric(df["codigo_sucursal"], errors="coerce")
    df["codigo_articulo"] = pd.to_numeric(df["codigo_articulo"], errors="coerce")
    df["codigo_proveedor"] = pd.to_numeric(df["codigo_proveedor"], errors="coerce")
    if "cod_comprador" in df.columns:
        df["cod_comprador"] = pd.to_numeric(df["cod_comprador"], errors="coerce")

    return df


df = preparar_dataset()
df_comp_raw = load_compradores()
df_prov_raw = load_proveedores()

if df.empty:
    st.warning("No se pudieron cargar datos desde la vista materializada.")
    st.stop()

# ============================================================
# 2. ARMADO DE SELECTORES NEMOTÉCNICOS
# ============================================================

# --- COMPRADORES (para selector) ---
df_comp_sel = df_comp_raw.dropna(subset=["cod_comprador"]).copy()
df_comp_sel["cod_comprador"] = pd.to_numeric(df_comp_sel["cod_comprador"], errors="coerce")
df_comp_sel["label"] = (
    df_comp_sel["cod_comprador"].astype(int).astype(str)
    + " – "
    + df_comp_sel["n_comprador"].astype(str)
)
map_comp_label_to_code = dict(zip(df_comp_sel["label"], df_comp_sel["cod_comprador"]))
comprador_labels = sorted(df_comp_sel["label"].unique())

comprador_sel_labels = st.sidebar.multiselect(
    "🧑‍💼 Comprador",
    options=comprador_labels,
    default=[],
)
comprador_sel = [map_comp_label_to_code[l] for l in comprador_sel_labels]

# --- PROVEEDORES (para selector) ---
df_prov_sel = df_prov_raw.dropna(subset=["c_proveedor"]).copy()
df_prov_sel["c_proveedor"] = pd.to_numeric(df_prov_sel["c_proveedor"], errors="coerce")
df_prov_sel["label"] = (
    df_prov_sel["c_proveedor"].astype(int).astype(str)
    + " – "
    + df_prov_sel["n_proveedor"].astype(str)
)
map_prov_label_to_code = dict(zip(df_prov_sel["label"], df_prov_sel["c_proveedor"]))
proveedor_labels = sorted(df_prov_sel["label"].unique())

proveedor_sel_labels = st.sidebar.multiselect(
    "🏭 Proveedor",
    options=proveedor_labels,
    default=[],
)
proveedor_sel = [map_prov_label_to_code[l] for l in proveedor_sel_labels]

# --- SUCURSALES ---
sucursales = sorted(df["codigo_sucursal"].dropna().unique().tolist())
sucursal_sel = st.sidebar.multiselect(
    "🏬 Sucursal",
    options=sucursales,
    default=sucursales,
)

st.sidebar.markdown("---")

# --- Parámetros adicionales ---
solo_activos_compra = st.sidebar.checkbox("Solo activos para compra", value=True)
solo_con_stock = st.sidebar.checkbox("Solo artículos con stock total > 0", value=True)

# 🔹 Nueva tolerancia de stock máximo = mínimo + tolerancia
tolerancia_dias = st.sidebar.number_input(
    "Tolerancia sobre stock mínimo (días)",
    min_value=0,
    max_value=60,
    value=7,
    step=1,
    help="Se considera sobre-stock cuando los días reales de stock superan (días mínimos + tolerancia).",
)

# ============================================================
# 3. APLICACIÓN DE FILTROS
# ============================================================
df_f = df.copy()

if sucursal_sel:
    df_f = df_f[df_f["codigo_sucursal"].isin(sucursal_sel)]

if proveedor_sel:
    df_f = df_f[df_f["codigo_proveedor"].isin(proveedor_sel)]

if "cod_comprador" in df_f.columns and comprador_sel:
    df_f = df_f[df_f["cod_comprador"].isin(comprador_sel)]

if solo_activos_compra and "active_for_purchase" in df_f.columns:
    df_f = df_f[df_f["active_for_purchase"] == True]

if solo_con_stock:
    df_f = df_f[df_f["stock_total"] > 0]

# ============================================================
# 3.b CÁLCULO DE MÁXIMO OBJETIVO DINÁMICO Y SOBRE-STOCK
# ============================================================
df_f["dias_max_objetivo"] = np.where(
    df_f["q_dias_stock"].notna(),
    df_f["q_dias_stock"] + tolerancia_dias,
    np.nan,
)

df_f["sobre_stock_param"] = np.where(
    df_f["dias_max_objetivo"].notna(),
    df_f["dias_stock"] > df_f["dias_max_objetivo"],
    False,
)

st.caption(f"Registros luego de filtros: {len(df_f):,}")

# ============================================================
# 4. MÉTRICAS RESUMEN
# ============================================================
total_skus = df_f["codigo_articulo"].nunique()
bajo_min = df_f[df_f["bajo_stock_minimo"]]["codigo_articulo"].nunique()
faltan = df_f[df_f["falta_cobertura"]]["codigo_articulo"].nunique()
sobre = df_f[df_f["sobre_stock_param"]]["codigo_articulo"].nunique()
sin_venta = df_f[df_f["sin_venta_30d_con_stock"]]["codigo_articulo"].nunique()
valor_total = df_f["valor_stock_costo"].sum()
dias_prom = df_f["dias_stock"].mean()

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("SKUs filtrados", f"{total_skus:,}")
c2.metric("Bajo stock mínimo", f"{bajo_min:,}")
c3.metric("Falta cobertura (días < mín)", f"{faltan:,}")
c4.metric(f"Sobre stock (días > mín + {tolerancia_dias})", f"{sobre:,}")
c5.metric("Sin venta 30 días", f"{sin_venta:,}")
c6.metric("Cobertura promedio (días)", f"{dias_prom:0.1f}" if not np.isnan(dias_prom) else "-")

# ============================================================
# 5. TABS: DETALLE Y RESUMEN
# ============================================================
tab_detalle, tab_resumen = st.tabs(["🔎 Detalle por artículo", "📊 Resumen por proveedor"])

# ============================================================
# 5.1. DETALLE
# ============================================================
with tab_detalle:
    st.subheader("Detalle de artículos por sucursal")

    columnas_detalle = [
        "codigo_sucursal",
        "codigo_articulo",
        "codigo_proveedor",
        "cod_comprador",
        "stock_total",
        "stock_minimo",
        "venta_promedio_diaria_30d",
        "dias_stock",
        "q_dias_stock",
        "dias_max_objetivo",
        "bajo_stock_minimo",
        "falta_cobertura",
        "sobre_stock_param",
        "sin_venta_30d_con_stock",
        "valor_stock_costo",
        "fecha_ultimo_ingreso",
        "fecha_ultima_venta",
    ]

    df_show = df_f[columnas_detalle].copy()

    # Normalizar claves para merge con catálogos
    df_show["codigo_proveedor"] = pd.to_numeric(df_show["codigo_proveedor"], errors="coerce")
    df_prov_merge = df_prov_raw.copy()
    df_prov_merge["c_proveedor"] = pd.to_numeric(df_prov_merge["c_proveedor"], errors="coerce")

    df_comp_merge = df_comp_raw.copy()
    df_comp_merge["cod_comprador"] = pd.to_numeric(df_comp_merge["cod_comprador"], errors="coerce")
    if "cod_comprador" in df_show.columns:
        df_show["cod_comprador"] = pd.to_numeric(df_show["cod_comprador"], errors="coerce")

    # Enriquecer con nombres
    df_show = df_show.merge(
        df_prov_merge[["c_proveedor", "n_proveedor"]],
        how="left",
        left_on="codigo_proveedor",
        right_on="c_proveedor",
    )

    df_show = df_show.merge(
        df_comp_merge[["cod_comprador", "n_comprador"]],
        how="left",
        on="cod_comprador",
    )

    rename_map = {
        "codigo_sucursal": "Sucursal",
        "codigo_articulo": "Artículo",
        "codigo_proveedor": "Proveedor",
        "n_proveedor": "Nombre proveedor",
        "cod_comprador": "Comprador",
        "n_comprador": "Nombre comprador",
        "stock_total": "Stock total",
        "stock_minimo": "Stock mínimo",
        "venta_promedio_diaria_30d": "Venta prom. diaria 30d",
        "dias_stock": "Días stock (real)",
        "q_dias_stock": "Días mín. objetivo",
        "dias_max_objetivo": f"Días máx. objetivo (mín + {tolerancia_dias})",
        "bajo_stock_minimo": "Bajo stock mínimo",
        "falta_cobertura": "Falta cobertura",
        "sobre_stock_param": "Sobre stock",
        "sin_venta_30d_con_stock": "Sin venta 30d con stock",
        "valor_stock_costo": "Valor stock (costo)",
        "fecha_ultimo_ingreso": "Último ingreso",
        "fecha_ultima_venta": "Última venta",
    }
    df_show = df_show.rename(columns=rename_map)

    # Ordenar: primero faltantes, luego sobre stock, luego sin venta
    df_show = df_show.sort_values(
        by=["Falta cobertura", "Sobre stock", "Sin venta 30d con stock", "Días stock (real)"],
        ascending=[False, False, False, True],
        na_position="last",
    )

    st.dataframe(df_show, width='stretch', height=600)

# ============================================================
# 5.2. RESUMEN POR PROVEEDOR / SUCURSAL / COMPRADOR
# ============================================================
with tab_resumen:
    st.subheader("Resumen por proveedor / sucursal / comprador")

    grp_cols = ["codigo_proveedor", "codigo_sucursal", "cod_comprador"]

    df_grp = (
        df_f.groupby(grp_cols)
        .agg(
            SKUs=("codigo_articulo", "nunique"),
            BajoMin=("bajo_stock_minimo", "sum"),
            FaltaCob=("falta_cobertura", "sum"),
            SobreStock=("sobre_stock_param", "sum"),
            SinVenta=("sin_venta_30d_con_stock", "sum"),
            StockTotal=("stock_total", "sum"),
            ValorCosto=("valor_stock_costo", "sum"),
            CoberturaProm=("dias_stock", "mean"),
        )
        .reset_index()
    )

    # Normalizar para joins
    df_grp["codigo_proveedor"] = pd.to_numeric(df_grp["codigo_proveedor"], errors="coerce")
    df_grp["cod_comprador"] = pd.to_numeric(df_grp["cod_comprador"], errors="coerce")

    df_prov_merge = df_prov_raw.copy()
    df_prov_merge["c_proveedor"] = pd.to_numeric(df_prov_merge["c_proveedor"], errors="coerce")
    df_comp_merge = df_comp_raw.copy()
    df_comp_merge["cod_comprador"] = pd.to_numeric(df_comp_merge["cod_comprador"], errors="coerce")

    df_grp = df_grp.merge(
        df_prov_merge[["c_proveedor", "n_proveedor"]],
        how="left",
        left_on="codigo_proveedor",
        right_on="c_proveedor",
    )

    df_grp = df_grp.merge(
        df_comp_merge[["cod_comprador", "n_comprador"]],
        how="left",
        on="cod_comprador",
    )

    df_grp = df_grp.rename(
        columns={
            "codigo_proveedor": "Proveedor",
            "codigo_sucursal": "Sucursal",
            "cod_comprador": "Comprador",
            "n_proveedor": "Nombre proveedor",
            "n_comprador": "Nombre comprador",
            "StockTotal": "Stock total (unid.)",
            "ValorCosto": "Valor stock (costo)",
            "CoberturaProm": "Cobertura promedio (días)",
        }
    )

    df_grp = df_grp.sort_values(
        by=["Valor stock (costo)"],
        ascending=False,
    )

    st.dataframe(df_grp, width='stretch', height=600)

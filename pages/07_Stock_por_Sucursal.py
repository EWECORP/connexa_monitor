# 07_Stock_por_Sucursal.py
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
# Autor: [EWE / Zeetrex] – versión inicial
# pages/07_Stock_por_Sucursal.py
# Panel: Stock por Sucursal, Proveedor y Comprador (con ventas 30 días)
#
# Muestra:
#   - Stock actual por artículo-sucursal.
#   - Venta promedio diaria últimos 30 días.
#   - Días de cobertura 30 días.
#   - Filtros por sucursal, proveedor y comprador.

import streamlit as st
import pandas as pd
import numpy as np

from modules.db import get_conn_diarco_data
from modules.queries import (
    QRY_STOCK_SUCURSAL,
    QRY_PRODUCTOS_VIGENTES,
    QRY_VENTAS_30D,
)

st.title("📦 Stock por Sucursal, Proveedor y Comprador")

st.markdown(
    """
Este panel integra **stock actual**, **parámetros de producto** y **ventas de los últimos 30 días**  
para dar visibilidad a los compradores sobre:

- Cobertura de stock en días (basada en venta promedio 30 días).
- Artículos **por debajo del stock mínimo**.
- Artículos con **sobre-stock**.
- Vista de cartera por **comprador**.
"""
)

# --------------------------------------------------------------------
# Carga de datos (con caché)
# --------------------------------------------------------------------
@st.cache_data(show_spinner=True)
def load_stock():
    conn = get_conn_diarco_data()
    df = pd.read_sql(QRY_STOCK_SUCURSAL, conn)
    conn.close()
    return df


@st.cache_data(show_spinner=True)
def load_productos():
    conn = get_conn_diarco_data()
    df = pd.read_sql(QRY_PRODUCTOS_VIGENTES, conn)
    conn.close()
    return df


@st.cache_data(show_spinner=True)
def load_ventas_30d():
    conn = get_conn_diarco_data()
    df = pd.read_sql(QRY_VENTAS_30D, conn)
    conn.close()
    return df


# --------------------------------------------------------------------
# Preparación del dataset unificado
# --------------------------------------------------------------------
def preparar_dataset():
    df_stock = load_stock()
    df_prod  = load_productos()
    df_ventas = load_ventas_30d()

    if df_stock.empty:
        return pd.DataFrame()

    # Renombrar columnas productos para facilitar merge
    df_prod = df_prod.rename(
        columns={
            "c_sucu_empr": "codigo_sucursal",
            "c_articulo": "codigo_articulo",
            "c_proveedor_primario": "proveedor_primario",
        }
    )

    # Merge stock + productos (LEFT: todo el stock, aunque falte en maestro)
    df = pd.merge(
        df_stock,
        df_prod,
        on=["codigo_sucursal", "codigo_articulo"],
        how="left",
        suffixes=("", "_pv"),
    )

    # Merge ventas 30 días (LEFT: mantenemos todos los artículos del stock)
    df = pd.merge(
        df,
        df_ventas,
        on=["codigo_sucursal", "codigo_articulo"],
        how="left",
    )

    # Stock total (ajustable)
    df["stock_total"] = df["stock"].fillna(0) + df["stock_reserva"].fillna(0)

    # Días de stock "original" (por si quieren seguir viéndolo)
    df["dias_stock_util"] = df["q_dias_stock"].fillna(df["dias_stock"])

    # Venta promedio diaria 30d
    df["venta_promedio_diaria_30d"] = df["venta_promedio_diaria_30d"].fillna(0)

    # Días de cobertura 30d: stock_total / venta_promedio_diaria_30d
    df["dias_cobertura_30d"] = np.where(
        df["venta_promedio_diaria_30d"] > 0,
        df["stock_total"] / df["venta_promedio_diaria_30d"],
        np.nan,
    )

    # Stock mínimo
    df["stock_minimo"] = df["stock_minimo"].fillna(0)

    # Flag: debajo de stock mínimo
    df["bajo_stock_minimo"] = df["stock_total"] < df["stock_minimo"]

    # Valor stock a costo
    df["valor_stock_costo"] = df["stock_total"] * df["precio_costo"].fillna(0)

    # Flag: artículos sin venta 30d pero con stock (riesgo)
    df["sin_venta_30d_con_stock"] = (df["venta_promedio_diaria_30d"] == 0) & (df["stock_total"] > 0)

    return df


df = preparar_dataset()

if df.empty:
    st.warning("No se pudieron cargar datos para el panel de stock.")
    st.stop()

# --------------------------------------------------------------------
# Sidebar: filtros (Sucursal / Proveedor / Comprador)
# --------------------------------------------------------------------
st.sidebar.header("Filtros")

# Sucursal
sucursales = sorted(df["codigo_sucursal"].dropna().unique().tolist())
sucursal_sel = st.sidebar.multiselect(
    "Sucursal",
    options=sucursales,
    default=sucursales,
)

# Proveedor (de tabla de stock, pueden cambiar a proveedor_primario si prefieren)
proveedores = sorted(df["codigo_proveedor"].dropna().unique().tolist())
proveedor_sel = st.sidebar.multiselect(
    "Proveedor",
    options=proveedores,
    default=[],
)

# Comprador (desde maestro de productos)
compradores = sorted(df["cod_comprador"].dropna().unique().tolist())
comprador_sel = st.sidebar.multiselect(
    "Comprador",
    options=compradores,
    default=[],
)

# Filtro: solo activos para compra
solo_activos_compra = st.sidebar.checkbox("Solo activos para compra", value=True)

# Filtro: solo artículos con stock total > 0
solo_con_stock = st.sidebar.checkbox("Solo artículos con stock total > 0", value=True)

# Umbral sobre-stock según cobertura 30d
umbral_sobre_stock = st.sidebar.slider(
    "Umbral de sobre-stock (días de cobertura 30d)",
    min_value=10,
    max_value=180,
    value=60,
    step=10,
)

df_f = df.copy()

if sucursal_sel:
    df_f = df_f[df_f["codigo_sucursal"].isin(sucursal_sel)]

if proveedor_sel:
    df_f = df_f[df_f["codigo_proveedor"].isin(proveedor_sel)]

if comprador_sel:
    df_f = df_f[df_f["cod_comprador"].isin(comprador_sel)]

if solo_activos_compra:
    df_f["active_for_purchase"] = df_f["active_for_purchase"].fillna(False)
    df_f = df_f[df_f["active_for_purchase"] == True]

if solo_con_stock:
    df_f = df_f[df_f["stock_total"] > 0]

# Flag de sobre-stock ahora en base a cobertura 30d
df_f["sobre_stock_cobertura"] = df_f["dias_cobertura_30d"] > umbral_sobre_stock

# --------------------------------------------------------------------
# Métricas de cabecera
# --------------------------------------------------------------------
total_skus = df_f["codigo_articulo"].nunique()
skus_bajo_minimo = df_f[df_f["bajo_stock_minimo"]].codigo_articulo.nunique()
skus_sobre_stock = df_f[df_f["sobre_stock_cobertura"]].codigo_articulo.nunique()
skus_sin_venta_30d_con_stock = df_f[df_f["sin_venta_30d_con_stock"]].codigo_articulo.nunique()
valor_stock_total = df_f["valor_stock_costo"].sum()

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("SKUs filtrados", f"{total_skus:,}")
col2.metric("SKUs bajo stock mínimo", f"{skus_bajo_minimo:,}")
col3.metric(f"SKUs con cobertura > {umbral_sobre_stock} días", f"{skus_sobre_stock:,}")
col4.metric("SKUs sin venta 30d con stock", f"{skus_sin_venta_30d_con_stock:,}")
col5.metric("Valor stock (costo)", f"${valor_stock_total:,.0f}")

# --------------------------------------------------------------------
# Tabs: Detalle de cartera y Resumen por proveedor
# --------------------------------------------------------------------
tab_detalle, tab_proveedor = st.tabs(["🔎 Detalle de cartera", "📊 Resumen por proveedor"])

with tab_detalle:
    st.subheader("Detalle por artículo (vista de cartera)")

    columnas_detalle = [
        "codigo_sucursal",
        "cod_comprador",
        "codigo_proveedor",
        "proveedor_primario",
        "codigo_articulo",
        "stock_total",
        "stock_minimo",
        "venta_promedio_diaria_30d",
        "dias_cobertura_30d",
        "bajo_stock_minimo",
        "sobre_stock_cobertura",
        "sin_venta_30d_con_stock",
        "valor_stock_costo",
        "fecha_ultimo_ingreso",
        "fecha_ultima_venta",
    ]

    df_show = df_f[columnas_detalle].copy()

    df_show = df_show.rename(columns={
        "codigo_sucursal": "Sucursal",
        "cod_comprador": "Comprador",
        "codigo_proveedor": "Proveedor Stock",
        "proveedor_primario": "Proveedor Primario",
        "codigo_articulo": "Artículo",
        "stock_total": "Stock total",
        "stock_minimo": "Stock mínimo",
        "venta_promedio_diaria_30d": "Venta prom. diaria 30d",
        "dias_cobertura_30d": "Días cobertura 30d",
        "bajo_stock_minimo": "Bajo stock mínimo",
        "sobre_stock_cobertura": f"Sobre-stock (> {umbral_sobre_stock} días)",
        "sin_venta_30d_con_stock": "Sin venta 30d con stock",
        "valor_stock_costo": "Valor stock (costo)",
        "fecha_ultimo_ingreso": "F. último ingreso",
        "fecha_ultima_venta": "F. última venta",
    })

    # Orden: primero críticos (bajo mínimo), luego sin venta 30d, luego cobertura ascendente
    df_show = df_show.sort_values(
        by=["Bajo stock mínimo", "Sin venta 30d con stock", "Días cobertura 30d"],
        ascending=[False, False, True],
        na_position="last",
    )

    st.dataframe(df_show, use_container_width=True, height=600)

with tab_proveedor:
    st.subheader("Resumen por proveedor / sucursal / comprador")

    grp_cols = ["codigo_proveedor", "codigo_sucursal", "cod_comprador"]

    df_grp = df_f.groupby(grp_cols).agg(
        skus=("codigo_articulo", "nunique"),
        skus_bajo_minimo=("bajo_stock_minimo", lambda s: s[s].sum()),
        skus_sobre_stock=("sobre_stock_cobertura", lambda s: s[s].sum()),
        skus_sin_venta_30d=("sin_venta_30d_con_stock", lambda s: s[s].sum()),
        stock_total_unidades=("stock_total", "sum"),
        valor_stock_costo=("valor_stock_costo", "sum"),
        dias_cobertura_prom=("dias_cobertura_30d", "mean"),
    ).reset_index()

    df_grp = df_grp.rename(columns={
        "codigo_proveedor": "Proveedor",
        "codigo_sucursal": "Sucursal",
        "cod_comprador": "Comprador",
        "skus": "SKUs",
        "skus_bajo_minimo": "SKUs bajo mínimo",
        "skus_sobre_stock": f"SKUs sobre {umbral_sobre_stock} días",
        "skus_sin_venta_30d": "SKUs sin venta 30d con stock",
        "stock_total_unidades": "Stock total (unid.)",
        "valor_stock_costo": "Valor stock (costo)",
        "dias_cobertura_prom": "Cobertura 30d promedio",
    })

    df_grp = df_grp.sort_values(
        by=["Valor stock (costo)"],
        ascending=False,
    )

    st.dataframe(df_grp, use_container_width=True, height=500)

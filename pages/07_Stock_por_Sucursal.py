# pages/07_Stock_por_Sucursal.py
# Panel: Control de Stock por Sucursal y Proveedor
#
# Objetivo:
#   Permitir visualizar el stock actual por artículo-sucursal,
#   filtrado por sucursal y proveedor, identificando:
#     - Artículos por debajo del stock mínimo.
#     - Artículos con sobre-stock (muchos días de stock).
#     - Valor del stock por proveedor.
#     - Artículos sin venta en 30 días pero con stock (posible obsolescencia).
#     - Tolerancia dinámica para definir sobre-stock (días_stock > días_mín + tolerancia).
#     - Junto al stock informar próximos ingresos: pedidos en curso / OC pendientes / transferencias.
#
# Fuentes:
#   - src.base_stock_sucursal
#   - src.base_productos_vigentes
#   - src.base_ventas_extendida (ventas 30d)
#   - (opcional) src.base_productos_en_transito
#   - (opcional) src.base_transferencias_pendientes
#
# MV esperada (opcional):
#   - datamart.mv_stock_cartera_30d
#     Si no existe, el panel hace fallback a una query dinámica.

# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import date
import pandas as pd
import numpy as np
import streamlit as st
from sqlalchemy import text

from modules.db import get_diarco_engine
from modules.queries.stock_sucursal import (
    QRY_COMPPRADORES,
    QRY_PROVEEDORES,
    QRY_MV_STOCK_CARTERA_30D,
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

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# ============================================================
# 0. SQL FALLBACK (si NO existe la MV)
# ============================================================
SQL_FALLBACK_STOCK_CARTERA_30D = text("""
WITH vtas AS (
    SELECT
        codigo_articulo::int            AS codigo_articulo,
        sucursal::int                  AS codigo_sucursal,
        SUM(unidades)::numeric         AS unidades_30d,
        COUNT(DISTINCT fecha)          AS dias_con_venta,
        (SUM(unidades)::numeric / 30.0) AS venta_promedio_diaria_30d
    FROM src.base_ventas_extendida
    WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2
),
stk AS (
    SELECT
        bs.codigo_articulo::int   AS codigo_articulo,
        bs.codigo_sucursal::int   AS codigo_sucursal,
        bs.codigo_proveedor::int  AS codigo_proveedor,
        bs.precio_costo::numeric  AS precio_costo,
        bs.precio_venta::numeric  AS precio_venta,
        COALESCE(bs.stock, 0)::numeric AS stock,
        COALESCE(bs.stock_reserva, 0)::numeric AS stock_reserva,
        bs.fecha_ultimo_ingreso,
        bs.fecha_ultima_venta,
        bs.ultimo_ingreso,
        bs.dias_stock::numeric    AS dias_stock,
        bs.q_dias_stock::numeric  AS q_dias_stock,
        bs.q_dias_sobre_stock::numeric AS q_dias_sobre_stock,

        -- “próximos ingresos” (si ya vienen en base_stock_sucursal)
        bs.pedido_pendiente::numeric,
        bs.transfer_pendiente::numeric,
        bs.pedido_sgm::numeric
    FROM src.base_stock_sucursal bs
),
vig AS (
    SELECT
        c_articulo::int            AS codigo_articulo,
        c_sucu_empr::int           AS codigo_sucursal,
        c_proveedor_primario::int  AS codigo_proveedor,
        COALESCE(stock_minimo, 0)::numeric AS stock_minimo,
        cod_comprador::int         AS cod_comprador,
        active_for_purchase,
        active_for_sale,
        active_on_mix,
        habilitado,
        abastecimiento,
        cod_cd
    FROM src.base_productos_vigentes
),
transit AS (
    SELECT
        t.c_articulo::int     AS codigo_articulo,
        t.c_sucu_dest::int    AS codigo_sucursal,
        SUM(t.q_unid_transito)::numeric AS en_transito
    FROM src.base_productos_en_transito t
    GROUP BY 1, 2
),
transf AS (
    SELECT
        x.c_articulo::int     AS codigo_articulo,
        x.c_sucu_dest::int    AS codigo_sucursal,
        SUM(x.q_pendiente)::numeric AS transferencias_pendientes
    FROM src.base_transferencias_pendientes x
    GROUP BY 1, 2
)
SELECT
    s.codigo_articulo,
    s.codigo_sucursal,

    -- priorizar proveedor del stock; si no existe, usar primario
    COALESCE(s.codigo_proveedor, v.codigo_proveedor) AS codigo_proveedor,

    s.precio_costo,
    s.precio_venta,
    s.stock,
    s.stock_reserva,
    (s.stock + s.stock_reserva) AS stock_total,

    v.stock_minimo,
    v.cod_comprador,
    v.active_for_purchase,
    v.active_for_sale,
    v.active_on_mix,
    v.habilitado,
    v.abastecimiento,
    v.cod_cd,

    COALESCE(vt.venta_promedio_diaria_30d, 0) AS venta_promedio_diaria_30d,

    --  si dias_stock viene 0, recalcular
    COALESCE(
        NULLIF(s.dias_stock, 0),
        CASE
            WHEN COALESCE(vt.venta_promedio_diaria_30d, 0) > 0
                THEN (s.stock + s.stock_reserva) / vt.venta_promedio_diaria_30d
            ELSE NULL
        END
    ) AS dias_stock,

    s.q_dias_stock,
    s.q_dias_sobre_stock,

    s.fecha_ultimo_ingreso,
    s.fecha_ultima_venta,
    s.ultimo_ingreso,

    s.pedido_pendiente,
    s.transfer_pendiente,
    s.pedido_sgm,

    COALESCE(t.en_transito, 0) AS en_transito,
    COALESCE(tr.transferencias_pendientes, 0) AS transferencias_pendientes

FROM stk s
LEFT JOIN vig v
  ON v.codigo_articulo = s.codigo_articulo
 AND v.codigo_sucursal = s.codigo_sucursal
LEFT JOIN vtas vt
  ON vt.codigo_articulo = s.codigo_articulo
 AND vt.codigo_sucursal = s.codigo_sucursal
LEFT JOIN transit t
  ON t.codigo_articulo = s.codigo_articulo
 AND t.codigo_sucursal = s.codigo_sucursal
LEFT JOIN transf tr
  ON tr.codigo_articulo = s.codigo_articulo
 AND tr.codigo_sucursal = s.codigo_sucursal
;
""")


# ============================================================
# 1. CARGA DE DATOS
# ============================================================

@st.cache_data(ttl=TTL, show_spinner=True)
def _exists_relation(schema: str, relname: str) -> bool:
    engine = get_diarco_engine()
    sql = text("""
        SELECT 1
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema
          AND c.relname = :relname
        LIMIT 1
    """)
    with engine.connect() as con:
        return con.execute(sql, {"schema": schema, "relname": relname}).fetchone() is not None


@st.cache_data(ttl=TTL, show_spinner=True)
def load_dataset() -> pd.DataFrame:
    """
    Intenta cargar la MV (datamart.mv_stock_cartera_30d).
    Si no existe, hace fallback a una query dinámica.
    """
    engine = get_diarco_engine()

    # QRY_MV_STOCK_CARTERA_30D apunta a datamart.mv_stock_cartera_30d
    # (según modules/queries.py). :contentReference[oaicite:1]{index=1}
    if _exists_relation("datamart", "mv_stock_cartera_30d"):
        return pd.read_sql(QRY_MV_STOCK_CARTERA_30D, con=engine)

    # fallback robusto
    return pd.read_sql(SQL_FALLBACK_STOCK_CARTERA_30D, con=engine)


@st.cache_data(ttl=TTL, show_spinner=False)
def load_compradores() -> pd.DataFrame:
    engine = get_diarco_engine()
    return pd.read_sql(QRY_COMPPRADORES, con=engine)


@st.cache_data(ttl=TTL, show_spinner=False)
def load_proveedores() -> pd.DataFrame:
    engine = get_diarco_engine()
    return pd.read_sql(QRY_PROVEEDORES, con=engine)


def preparar_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza y enriquece dataset base."""
    if df is None or df.empty:
        return pd.DataFrame()

    # Seguridad de columnas esperadas
    for col, default in [
        ("stock", 0),
        ("stock_reserva", 0),
        ("venta_promedio_diaria_30d", 0),
        ("precio_costo", 0),
        ("stock_minimo", 0),
        ("q_dias_stock", np.nan),
        ("q_dias_sobre_stock", np.nan),
    ]:
        if col not in df.columns:
            df[col] = default

    # Tipos y nulos
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0)
    df["stock_reserva"] = pd.to_numeric(df["stock_reserva"], errors="coerce").fillna(0)
    df["venta_promedio_diaria_30d"] = pd.to_numeric(df["venta_promedio_diaria_30d"], errors="coerce").fillna(0)
    df["precio_costo"] = pd.to_numeric(df["precio_costo"], errors="coerce").fillna(0)
    df["stock_minimo"] = pd.to_numeric(df["stock_minimo"], errors="coerce").fillna(0)

    # Stock total
    df["stock_total"] = df["stock"] + df["stock_reserva"]

    # Días stock
    if "dias_stock" in df.columns:
        df["dias_stock"] = pd.to_numeric(df["dias_stock"], errors="coerce")
    else:
        df["dias_stock"] = np.where(
            df["venta_promedio_diaria_30d"] > 0,
            df["stock_total"] / df["venta_promedio_diaria_30d"],
            np.nan,
        )

    # Flags de negocio
    df["bajo_stock_minimo"] = df["stock_total"] < df["stock_minimo"]

    df["sin_venta_30d_con_stock"] = (
        (df["venta_promedio_diaria_30d"] == 0) & (df["stock_total"] > 0)
    )

    df["valor_stock_costo"] = df["stock_total"] * df["precio_costo"]

    df["falta_cobertura"] = np.where(
        df["q_dias_stock"].notna(),
        df["dias_stock"] < df["q_dias_stock"],
        False,
    )

    # Normalizar claves (permitiendo nulos)
    for k in ["codigo_sucursal", "codigo_articulo", "codigo_proveedor", "cod_comprador"]:
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce").astype("Int64")

    # Fechas
    for dcol in ["fecha_ultimo_ingreso", "fecha_ultima_venta"]:
        if dcol in df.columns:
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce")

    return df


df_raw = load_dataset()
df = preparar_dataset(df_raw)
df_comp_raw = load_compradores()
df_prov_raw = load_proveedores()

if df.empty:
    st.warning("No se pudieron cargar datos (ni MV ni fallback). Verificar tablas src.* y permisos.")
    st.stop()

# ============================================================
# 2. SELECTORES (SIDEBAR)
# ============================================================

# --- COMPRADORES ---
comprador_sel = []
if "cod_comprador" in df.columns and not df_comp_raw.empty and "cod_comprador" in df_comp_raw.columns:
    df_comp_sel = df_comp_raw.dropna(subset=["cod_comprador"]).copy()
    df_comp_sel["cod_comprador"] = pd.to_numeric(df_comp_sel["cod_comprador"], errors="coerce").astype("Int64")
    df_comp_sel["label"] = (
        df_comp_sel["cod_comprador"].astype(str)
        + " – "
        + df_comp_sel["n_comprador"].astype(str)
    )
    map_comp_label_to_code = dict(zip(df_comp_sel["label"], df_comp_sel["cod_comprador"]))
    comprador_labels = sorted(df_comp_sel["label"].unique().tolist())

    comprador_sel_labels = st.sidebar.multiselect("🧑‍💼 Comprador", options=comprador_labels, default=[])
    comprador_sel = [map_comp_label_to_code[l] for l in comprador_sel_labels]

else:
    st.sidebar.info("No hay 'cod_comprador' en el dataset o no está disponible el catálogo.")


# --- PROVEEDORES ---
proveedor_sel = []
if not df_prov_raw.empty and "c_proveedor" in df_prov_raw.columns:
    df_prov_sel = df_prov_raw.dropna(subset=["c_proveedor"]).copy()
    df_prov_sel["c_proveedor"] = pd.to_numeric(df_prov_sel["c_proveedor"], errors="coerce").astype("Int64")
    df_prov_sel["label"] = (
        df_prov_sel["c_proveedor"].astype(str)
        + " – "
        + df_prov_sel["n_proveedor"].astype(str)
    )
    map_prov_label_to_code = dict(zip(df_prov_sel["label"], df_prov_sel["c_proveedor"]))
    proveedor_labels = sorted(df_prov_sel["label"].unique().tolist())

    proveedor_sel_labels = st.sidebar.multiselect("🏭 Proveedor", options=proveedor_labels, default=[])
    proveedor_sel = [map_prov_label_to_code[l] for l in proveedor_sel_labels]
else:
    st.sidebar.info("No está disponible el catálogo de proveedores (src.m_10_proveedores).")


# --- SUCURSALES ---
sucursales = sorted(df["codigo_sucursal"].dropna().unique().tolist()) if "codigo_sucursal" in df.columns else []
sucursal_sel = st.sidebar.multiselect(
    "🏬 Sucursal",
    options=sucursales,
    default=sucursales,
)

st.sidebar.markdown("---")

solo_activos_compra = st.sidebar.checkbox("Solo activos para compra", value=True)
solo_con_stock = st.sidebar.checkbox("Solo artículos con stock total > 0", value=True)

tolerancia_dias = st.sidebar.number_input(
    "Tolerancia sobre stock mínimo (días)",
    min_value=0,
    max_value=60,
    value=7,
    step=1,
    help="Se considera sobre-stock cuando los días reales de stock superan (días mínimos + tolerancia).",
)

# ============================================================
# 3. FILTROS
# ============================================================
df_f = df.copy()

if sucursal_sel and "codigo_sucursal" in df_f.columns:
    df_f = df_f[df_f["codigo_sucursal"].isin(sucursal_sel)]

if proveedor_sel and "codigo_proveedor" in df_f.columns:
    df_f = df_f[df_f["codigo_proveedor"].isin(proveedor_sel)]

if comprador_sel and "cod_comprador" in df_f.columns:
    df_f = df_f[df_f["cod_comprador"].isin(comprador_sel)]

if solo_activos_compra and "active_for_purchase" in df_f.columns:
    df_f = df_f[df_f["active_for_purchase"] == True]  # noqa: E712

if solo_con_stock and "stock_total" in df_f.columns:
    df_f = df_f[df_f["stock_total"] > 0]

# ============================================================
# 3.b SOBRE-STOCK DINÁMICO
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
# 4. MÉTRICAS
# ============================================================
total_skus = int(df_f["codigo_articulo"].nunique()) if "codigo_articulo" in df_f.columns else 0
bajo_min = int(df_f[df_f["bajo_stock_minimo"]]["codigo_articulo"].nunique()) if "bajo_stock_minimo" in df_f.columns else 0
faltan = int(df_f[df_f["falta_cobertura"]]["codigo_articulo"].nunique()) if "falta_cobertura" in df_f.columns else 0
sobre = int(df_f[df_f["sobre_stock_param"]]["codigo_articulo"].nunique()) if "sobre_stock_param" in df_f.columns else 0
sin_venta = int(df_f[df_f["sin_venta_30d_con_stock"]]["codigo_articulo"].nunique()) if "sin_venta_30d_con_stock" in df_f.columns else 0
valor_total = float(df_f["valor_stock_costo"].sum()) if "valor_stock_costo" in df_f.columns else 0.0
dias_prom = float(df_f["dias_stock"].mean()) if "dias_stock" in df_f.columns else np.nan

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("SKUs filtrados", f"{total_skus:,}")
c2.metric("Bajo stock mínimo", f"{bajo_min:,}")
c3.metric("Falta cobertura (días < mín)", f"{faltan:,}")
c4.metric(f"Sobre stock (días > mín + {tolerancia_dias})", f"{sobre:,}")
c5.metric("Sin venta 30 días", f"{sin_venta:,}")
c6.metric("Cobertura promedio (días)", f"{dias_prom:0.1f}" if not np.isnan(dias_prom) else "-")

# ============================================================
# 5. TABS
# ============================================================
tab_detalle, tab_resumen = st.tabs(["🔎 Detalle por artículo", "📊 Resumen por proveedor"])


# ============================================================
# 5.1 DETALLE
# ============================================================
with tab_detalle:
    st.subheader("Detalle de artículos por sucursal")

    # columnas base (si faltan, se ignoran)
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
        # “próximos ingresos” (si existen)
        "pedido_pendiente",
        "transfer_pendiente",
        "pedido_sgm",
    ]
    columnas_existentes = [c for c in columnas_detalle if c in df_f.columns]
    df_show = df_f[columnas_existentes].copy()

    # Enriquecer con nombres (si catálogos disponibles)
    if "codigo_proveedor" in df_show.columns and not df_prov_raw.empty:
        df_prov_merge = df_prov_raw.copy()
        df_prov_merge["c_proveedor"] = pd.to_numeric(df_prov_merge["c_proveedor"], errors="coerce").astype("Int64")
        df_show = df_show.merge(
            df_prov_merge[["c_proveedor", "n_proveedor"]],
            how="left",
            left_on="codigo_proveedor",
            right_on="c_proveedor",
        )

    if "cod_comprador" in df_show.columns and not df_comp_raw.empty:
        df_comp_merge = df_comp_raw.copy()
        df_comp_merge["cod_comprador"] = pd.to_numeric(df_comp_merge["cod_comprador"], errors="coerce").astype("Int64")
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
        "pedido_pendiente": "Pedido pendiente",
        "transfer_pendiente": "Transfer pendiente",
        "pedido_sgm": "Pedido SGM / OC pend.",
    }
    df_show = df_show.rename(columns={k: v for k, v in rename_map.items() if k in df_show.columns})

    # Ordenar: faltantes, sobre stock, sin venta
    sort_cols = [c for c in ["Falta cobertura", "Sobre stock", "Sin venta 30d con stock", "Días stock (real)"] if c in df_show.columns]
    if sort_cols:
        asc = []
        for c in sort_cols:
            if c == "Días stock (real)":
                asc.append(True)
            else:
                asc.append(False)
        df_show = df_show.sort_values(by=sort_cols, ascending=asc, na_position="last")

    st.dataframe(df_show, use_container_width=True, height=600)


# ============================================================
# 5.2 RESUMEN
# ============================================================
with tab_resumen:
    st.subheader("Resumen por proveedor / sucursal / comprador")

    grp_cols = [c for c in ["codigo_proveedor", "codigo_sucursal", "cod_comprador"] if c in df_f.columns]
    if not grp_cols:
        st.warning("No hay columnas de agrupación disponibles en el dataset.")
        st.stop()

    agg_dict = {
        "codigo_articulo": ("SKUs", "nunique"),
        "bajo_stock_minimo": ("BajoMin", "sum"),
        "falta_cobertura": ("FaltaCob", "sum"),
        "sobre_stock_param": ("SobreStock", "sum"),
        "sin_venta_30d_con_stock": ("SinVenta", "sum"),
        "stock_total": ("StockTotal", "sum"),
        "valor_stock_costo": ("ValorCosto", "sum"),
        "dias_stock": ("CoberturaProm", "mean"),
    }
    # solo agregar agregaciones presentes
    real_aggs = {}
    for col, (alias, fn) in agg_dict.items():
        if col in df_f.columns:
            real_aggs[alias] = (col, fn)

    df_grp = df_f.groupby(grp_cols).agg(**real_aggs).reset_index()

    # Joins de nombres
    if "codigo_proveedor" in df_grp.columns and not df_prov_raw.empty:
        df_prov_merge = df_prov_raw.copy()
        df_prov_merge["c_proveedor"] = pd.to_numeric(df_prov_merge["c_proveedor"], errors="coerce").astype("Int64")
        df_grp = df_grp.merge(
            df_prov_merge[["c_proveedor", "n_proveedor"]],
            how="left",
            left_on="codigo_proveedor",
            right_on="c_proveedor",
        )

    if "cod_comprador" in df_grp.columns and not df_comp_raw.empty:
        df_comp_merge = df_comp_raw.copy()
        df_comp_merge["cod_comprador"] = pd.to_numeric(df_comp_merge["cod_comprador"], errors="coerce").astype("Int64")
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

    if "Valor stock (costo)" in df_grp.columns:
        df_grp = df_grp.sort_values(by=["Valor stock (costo)"], ascending=False)

    st.dataframe(df_grp, use_container_width=True, height=600)

st.caption(
    "Nota: Si no existe datamart.mv_stock_cartera_30d, el panel ejecuta fallback desde tablas src.*."
)
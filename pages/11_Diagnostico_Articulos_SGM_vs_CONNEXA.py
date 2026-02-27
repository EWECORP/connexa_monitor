# pages/11_Diagnostico_Articulos_SGM_vs_CONNEXA.py
# -*- coding: utf-8 -*-

"""
Diagnóstico — Artículos SGM vs CONNEXA

Objetivo:
- Seleccionar un Proveedor
- Listar sus artículos (desde CONNEXA o desde SGM)
- Seleccionar un Artículo
- Ver el estado del artículo por sucursal/tienda en ambos mundos:
  - ORIGEN (SGM / SQL Server)
  - CONNEXA (PostgreSQL)
- Detectar faltantes y probables motivos (baja maestro / no habilitado por sucursal / etc.)

Notas:
- El bloque "CONFIG SGM" es el único que debería ajustarse si cambia el modelo real.
- La parte CONNEXA asume tablas/vistas ya existentes en su plataforma (src.*).
"""

import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import text

from modules.db import get_sqlserver_engine, get_pg_engine
from modules.ui import render_header


# -------------------------------------------------------------------
# Config Streamlit
# -------------------------------------------------------------------
st.set_page_config(
    page_title="Diagnóstico — Artículos SGM vs CONNEXA",
    page_icon="🧪",
    layout="wide",
)

render_header("Diagnóstico — Artículos por Proveedor / Estado por Sucursal (SGM vs CONNEXA)")

ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# -------------------------------------------------------------------
# CONFIG SGM (SQL Server / ORIGEN)
# -------------------------------------------------------------------
SGM_TABLE_ART_SUC = "[DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL]"
SGM_TABLE_ART     = "[DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS]"

SGM_COL_ART        = "C_ARTICULO"
SGM_COL_SUC        = "C_SUCU_EMPR"
SGM_COL_BAJA       = "M_BAJA"               # T050: S/N
SGM_COL_HAB        = "M_HABILITADO_SUCU"    # T051: S/N
SGM_COL_F_ALTA     = "F_ALTA"               # existe en T050 y T051
SGM_COL_F_BAJA     = "F_BAJA"               # existe SOLO en T050
SGM_COL_ABAST      = "C_SISTEMATICA"        # T051: 0..3 (si existe)
SGM_COL_PROV_PRIM  = "C_PROVEEDOR_PRIMARIO"


# -------------------------------------------------------------------
# SQL — CONNEXA (PostgreSQL)
# -------------------------------------------------------------------
SQL_PG_PROVEEDORES = text("""
SELECT c_proveedor::bigint AS c_proveedor,
       TRIM(BOTH FROM n_proveedor)::text AS n_proveedor
FROM src.m_10_proveedores
ORDER BY 2 NULLS LAST, 1;
""")

SQL_PG_SUCURSALES = text("""
SELECT id_tienda::text AS c_sucu_empr,
       TRIM(BOTH FROM suc_nombre)::text AS suc_nombre
FROM src.m_91_sucursales
ORDER BY 1;
""")

# FIX: habilitado / active_for_* son int (0/1) => coalesce a 0::int
SQL_PG_ARTICULOS_POR_PROV = text("""
SELECT
  c_articulo::bigint AS c_articulo,
  c_sucu_empr::text  AS c_sucu_empr,

  COALESCE(habilitado, 0)::int            AS habilitado_i,
  COALESCE(active_for_purchase, 0)::int   AS active_for_purchase_i,
  COALESCE(active_for_sale, 0)::int       AS active_for_sale_i,
  COALESCE(active_on_mix, 0)::int         AS active_on_mix_i,

  abastecimiento::text         AS abastecimiento,
  cod_cd::text                 AS cod_cd,
  fecha_registro::timestamp    AS fecha_registro,
  fecha_baja::timestamp        AS fecha_baja,
  fecha_extraccion::timestamp  AS fecha_extraccion,
  estado_sincronizacion::text  AS estado_sincronizacion
FROM src.base_productos_vigentes
WHERE c_proveedor_primario::bigint = :c_proveedor
ORDER BY c_articulo, c_sucu_empr;
""")

SQL_PG_STOCK_ART_SUC = text("""
SELECT
  codigo_articulo::bigint      AS c_articulo,
  codigo_sucursal::text        AS c_sucu_empr,
  stock::numeric               AS stock,
  pedido_pendiente::numeric    AS pedido_pendiente,
  transfer_pendiente::numeric  AS transfer_pendiente,
  fecha_stock::date            AS fecha_stock,
  fecha_extraccion::timestamp  AS fecha_extraccion_stock,
  estado_sincronizacion::text  AS estado_sincronizacion_stock
FROM src.base_stock_sucursal
WHERE codigo_articulo::bigint = :c_articulo
ORDER BY codigo_sucursal;
""")


# -------------------------------------------------------------------
# SQL — SGM (SQL Server)
# -------------------------------------------------------------------
SQL_SGM_ARTICULOS_PROV = text(f"""
SELECT
  CAST(a.{SGM_COL_ART} AS bigint) AS c_articulo,
  LTRIM(RTRIM(CAST(a.{SGM_COL_BAJA} AS varchar(1)))) AS sgm_baja_art,
  TRY_CONVERT(datetime, a.{SGM_COL_F_ALTA}) AS sgm_f_alta_art,
  TRY_CONVERT(datetime, a.{SGM_COL_F_BAJA}) AS sgm_f_baja_art
FROM {SGM_TABLE_ART} a
WHERE CAST(a.{SGM_COL_PROV_PRIM} AS bigint) = :c_proveedor
ORDER BY CAST(a.{SGM_COL_ART} AS bigint);
""")

SQL_SGM_ARTICULO_T050 = text(f"""
SELECT
  CAST(a.{SGM_COL_ART} AS bigint) AS c_articulo,
  LTRIM(RTRIM(CAST(a.{SGM_COL_BAJA} AS varchar(1)))) AS sgm_baja_art,
  TRY_CONVERT(datetime, a.{SGM_COL_F_ALTA}) AS sgm_f_alta_art,
  TRY_CONVERT(datetime, a.{SGM_COL_F_BAJA}) AS sgm_f_baja_art
FROM {SGM_TABLE_ART} a
WHERE CAST(a.{SGM_COL_ART} AS bigint) = :c_articulo;
""")

# IMPORTANTE: F_BAJA NO existe en T051 => se deja NULL AS sgm_f_baja_sucu
SQL_SGM_ESTADO_ART_SUC = text(f"""
SELECT
  CAST(s.{SGM_COL_ART} AS bigint)        AS c_articulo,
  CAST(s.{SGM_COL_SUC} AS varchar(10))   AS c_sucu_empr,

  -- Habilitado por sucursal (S/N)
  LTRIM(RTRIM(CAST(s.{SGM_COL_HAB} AS varchar(1)))) AS sgm_habilitado_sucu,

  -- F_ALTA existe en T051
  TRY_CONVERT(datetime, s.{SGM_COL_F_ALTA}) AS sgm_f_alta_sucu,

  -- F_BAJA no existe en T051
  CAST(NULL AS datetime) AS sgm_f_baja_sucu,

  -- Sistemática / Abastecimiento (si C_SISTEMATICA existe en T051)
  TRY_CAST(s.{SGM_COL_ABAST} AS int) AS sgm_c_sistematica,
  CASE TRY_CAST(s.{SGM_COL_ABAST} AS int)
    WHEN 0 THEN 'E.CD'
    WHEN 1 THEN 'E.PROV'
    WHEN 2 THEN 'C.DOCKING'
    WHEN 3 THEN 'E.QX'
    ELSE 'DESCONOCIDO'
  END AS sgm_abastecimiento
FROM {SGM_TABLE_ART_SUC} s
WHERE CAST(s.{SGM_COL_ART} AS bigint) = :c_articulo
ORDER BY c_sucu_empr;
""")


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _norm_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def _flag_s(x) -> bool:
    return _norm_str(x).upper() == "S"


def _abast_norm(x) -> str:
    v = _norm_str(x).upper().replace(".", "").replace(" ", "")
    return v


def _int01_to_bool(x) -> bool:
    if pd.isna(x):
        return False
    try:
        return int(x) != 0
    except Exception:
        return str(x).strip().lower() in ("true", "t", "s", "si", "1", "y", "yes")


# -------------------------------------------------------------------
# Fetchers (PG)
# -------------------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_pg_proveedores() -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        return pd.read_sql(SQL_PG_PROVEEDORES, con)


@st.cache_data(ttl=ttl)
def fetch_pg_sucursales() -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        return pd.read_sql(SQL_PG_SUCURSALES, con)


@st.cache_data(ttl=ttl)
def fetch_pg_articulos_prov(c_proveedor: int) -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        return pd.read_sql(SQL_PG_ARTICULOS_POR_PROV, con, params={"c_proveedor": c_proveedor})


@st.cache_data(ttl=ttl)
def fetch_pg_stock_articulo(c_articulo: int) -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        return pd.read_sql(SQL_PG_STOCK_ART_SUC, con, params={"c_articulo": c_articulo})


# -------------------------------------------------------------------
# Fetchers (SGM)
# -------------------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_sgm_articulos_prov(c_proveedor: int) -> pd.DataFrame:
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        return pd.read_sql(SQL_SGM_ARTICULOS_PROV, con, params={"c_proveedor": c_proveedor})


@st.cache_data(ttl=ttl)
def fetch_sgm_t050_articulo(c_articulo: int) -> pd.DataFrame:
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        return pd.read_sql(SQL_SGM_ARTICULO_T050, con, params={"c_articulo": c_articulo})


@st.cache_data(ttl=ttl)
def fetch_sgm_estado_articulo(c_articulo: int) -> pd.DataFrame:
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        return pd.read_sql(SQL_SGM_ESTADO_ART_SUC, con, params={"c_articulo": c_articulo})


# -------------------------------------------------------------------
# UI — Selección Proveedor / Fuente / Artículo
# -------------------------------------------------------------------
df_prov = fetch_pg_proveedores()
if df_prov.empty:
    st.error("No se pudieron obtener proveedores desde CONNEXA (PG). Revisen conexión / permisos.")
    st.stop()

col_f1, col_f2, col_f3 = st.columns([2, 2, 3])

with col_f1:
    prov_buscar = st.text_input(
        "Buscar proveedor (código o nombre)",
        value="",
        placeholder="Ej: 'Arcor' o '32295' o 'lácteos'...",
    ).strip().lower()

    df_prov_view = df_prov.copy()
    df_prov_view["c_proveedor_str"] = df_prov_view["c_proveedor"].astype(str)
    df_prov_view["n_proveedor_str"] = df_prov_view["n_proveedor"].astype(str)

    if prov_buscar:
        mask = (
            df_prov_view["c_proveedor_str"].str.lower().str.contains(prov_buscar, na=False)
            | df_prov_view["n_proveedor_str"].str.lower().str.contains(prov_buscar, na=False)
        )
        df_prov_view = df_prov_view[mask].copy()

    if df_prov_view.empty:
        st.warning("No se encontraron proveedores con ese criterio de búsqueda.")
        # fallback: mostrar todos para no bloquear
        df_prov_view = df_prov.copy()
        df_prov_view["c_proveedor_str"] = df_prov_view["c_proveedor"].astype(str)
        df_prov_view["n_proveedor_str"] = df_prov_view["n_proveedor"].astype(str)

    prov_sel = st.selectbox(
        "Proveedor",
        options=df_prov_view["c_proveedor"].tolist(),
        index=0,
        format_func=lambda x: f"{x} — {df_prov.loc[df_prov['c_proveedor'] == x, 'n_proveedor'].iloc[0]}",
    )

with col_f2:
    modo_lista = st.radio(
        "Fuente para listar artículos",
        options=["CONNEXA (PG)", "SGM (SQL Server)"],
        horizontal=True,
        help="Para investigar faltantes, conviene listar desde SGM."
    )

with col_f3:
    st.caption("Comparación por sucursal: existencia, habilitación por sucursal, baja maestro y sistemática.")

# Lista de artículos
if modo_lista == "CONNEXA (PG)":
    df_pg_list = fetch_pg_articulos_prov(int(prov_sel))
    lista_art = sorted(df_pg_list["c_articulo"].dropna().unique().tolist()) if not df_pg_list.empty else []
else:
    df_sgm_list = fetch_sgm_articulos_prov(int(prov_sel))
    if df_sgm_list.empty:
        st.warning("No se pudieron listar artículos desde SGM. Revisen conexión SQL Server o mapeo (proveedor primario).")
    lista_art = sorted(df_sgm_list["c_articulo"].dropna().unique().tolist()) if not df_sgm_list.empty else []

if not lista_art:
    st.warning("No se encontraron artículos para el proveedor en la fuente elegida. Cambien la fuente o revisen mapeo/tablas.")
    st.stop()

c_articulo = st.selectbox("Artículo", options=lista_art)
st.divider()


# -------------------------------------------------------------------
# Datos — Estados por sucursal (SGM vs CONNEXA)
# -------------------------------------------------------------------
df_pg_estado_all = fetch_pg_articulos_prov(int(prov_sel))
df_pg_estado = df_pg_estado_all[df_pg_estado_all["c_articulo"] == int(c_articulo)].copy()

if not df_pg_estado.empty:
    df_pg_estado["pg_habilitado_flag"] = df_pg_estado["habilitado_i"].apply(_int01_to_bool)
    df_pg_estado["pg_active_for_purchase_flag"] = df_pg_estado["active_for_purchase_i"].apply(_int01_to_bool)
    df_pg_estado["pg_active_for_sale_flag"] = df_pg_estado["active_for_sale_i"].apply(_int01_to_bool)
    df_pg_estado["pg_active_on_mix_flag"] = df_pg_estado["active_on_mix_i"].apply(_int01_to_bool)

df_pg_stock = fetch_pg_stock_articulo(int(c_articulo))
df_sgm_estado = fetch_sgm_estado_articulo(int(c_articulo))

df_t050 = fetch_sgm_t050_articulo(int(c_articulo))
sgm_baja_art = ""
sgm_f_alta_art = pd.NaT
sgm_f_baja_art = pd.NaT
if not df_t050.empty:
    if "sgm_baja_art" in df_t050.columns:
        sgm_baja_art = _norm_str(df_t050.loc[0, "sgm_baja_art"])
    if "sgm_f_alta_art" in df_t050.columns:
        sgm_f_alta_art = df_t050.loc[0, "sgm_f_alta_art"]
    if "sgm_f_baja_art" in df_t050.columns:
        sgm_f_baja_art = df_t050.loc[0, "sgm_f_baja_art"]

df_suc = fetch_pg_sucursales()
if df_suc.empty:
    st.error("No se pudo obtener el maestro de sucursales desde CONNEXA (PG).")
    st.stop()

# Normalización claves
df_suc["c_sucu_empr"] = df_suc["c_sucu_empr"].astype(str).str.strip()

if not df_pg_estado.empty:
    df_pg_estado["c_sucu_empr"] = df_pg_estado["c_sucu_empr"].astype(str).str.strip()

if not df_sgm_estado.empty:
    df_sgm_estado["c_sucu_empr"] = df_sgm_estado["c_sucu_empr"].astype(str).str.strip()
    df_sgm_estado["sgm_habilitado_sucu"] = df_sgm_estado["sgm_habilitado_sucu"].astype(str).str.strip()

if not df_pg_stock.empty:
    df_pg_stock["c_sucu_empr"] = df_pg_stock["c_sucu_empr"].astype(str).str.strip()


# -------------------------------------------------------------------
# Construcción comparativo por sucursal
# -------------------------------------------------------------------
base = df_suc[["c_sucu_empr", "suc_nombre"]].copy()

# Merge SGM (T051)
if df_sgm_estado.empty:
    m = base.copy()
    m["sgm_habilitado_sucu"] = pd.NA
    m["sgm_f_alta_sucu"] = pd.NA
    m["sgm_f_baja_sucu"] = pd.NA
    m["sgm_c_sistematica"] = pd.NA
    m["sgm_abastecimiento"] = pd.NA
else:
    m = base.merge(
        df_sgm_estado[[
            "c_sucu_empr",
            "sgm_habilitado_sucu",
            "sgm_f_alta_sucu",
            "sgm_f_baja_sucu",
            "sgm_c_sistematica",
            "sgm_abastecimiento",
        ]],
        on="c_sucu_empr",
        how="left"
    )

# Merge CONNEXA (productos vigentes)
if not df_pg_estado.empty:
    m = m.merge(
        df_pg_estado[[
            "c_sucu_empr",
            "pg_habilitado_flag",
            "pg_active_for_purchase_flag",
            "pg_active_for_sale_flag",
            "pg_active_on_mix_flag",
            "abastecimiento",
            "cod_cd",
            "fecha_registro",
            "fecha_baja",
            "fecha_extraccion",
            "estado_sincronizacion",
        ]].rename(columns={
            "abastecimiento": "pg_abastecimiento",
            "cod_cd": "pg_cod_cd",
            "fecha_registro": "pg_fecha_registro",
            "fecha_baja": "pg_fecha_baja",
            "fecha_extraccion": "pg_fecha_extraccion",
            "estado_sincronizacion": "pg_estado_sincronizacion",
        }),
        on="c_sucu_empr",
        how="left"
    )
else:
    for c in [
        "pg_habilitado_flag", "pg_active_for_purchase_flag", "pg_active_for_sale_flag", "pg_active_on_mix_flag",
        "pg_abastecimiento", "pg_cod_cd",
        "pg_fecha_registro", "pg_fecha_baja", "pg_fecha_extraccion", "pg_estado_sincronizacion"
    ]:
        m[c] = pd.NA

# Merge CONNEXA (stock)
if not df_pg_stock.empty:
    m = m.merge(
        df_pg_stock.rename(columns={
            "stock": "pg_stock",
            "pedido_pendiente": "pg_pedido_pendiente",
            "transfer_pendiente": "pg_transfer_pendiente",
            "fecha_stock": "pg_fecha_stock",
            "fecha_extraccion_stock": "pg_fecha_extraccion_stock",
            "estado_sincronizacion_stock": "pg_estado_sincronizacion_stock",
        })[[
            "c_sucu_empr",
            "pg_stock", "pg_pedido_pendiente", "pg_transfer_pendiente",
            "pg_fecha_stock", "pg_fecha_extraccion_stock", "pg_estado_sincronizacion_stock"
        ]],
        on="c_sucu_empr",
        how="left"
    )
else:
    for c in [
        "pg_stock", "pg_pedido_pendiente", "pg_transfer_pendiente",
        "pg_fecha_stock", "pg_fecha_extraccion_stock", "pg_estado_sincronizacion_stock"
    ]:
        m[c] = pd.NA

# Incorporar datos maestro T050 en todas las filas
m["sgm_baja_art"] = sgm_baja_art
m["sgm_f_alta_art"] = sgm_f_alta_art
m["sgm_f_baja_art"] = sgm_f_baja_art
m["sgm_baja_flag"] = m["sgm_baja_art"].apply(_flag_s)

# Flags
m["en_sgm"] = (
    m["sgm_habilitado_sucu"].notna()
    | m["sgm_f_alta_sucu"].notna()
    | m["sgm_abastecimiento"].notna()
)

m["sgm_habilitado_flag"] = m["sgm_habilitado_sucu"].apply(_flag_s)

m["en_connexa"] = (
    m["pg_habilitado_flag"].notna()
    | m.get("pg_fecha_registro", pd.Series([pd.NA]*len(m))).notna()
    | m.get("pg_fecha_baja", pd.Series([pd.NA]*len(m))).notna()
    | m.get("pg_estado_sincronizacion", pd.Series([pd.NA]*len(m))).notna()
)

m["pg_habilitado_flag"] = m["pg_habilitado_flag"].fillna(False).astype(bool)

# Diffs
m["diff_habilitado"] = m["en_sgm"] & m["en_connexa"] & (m["sgm_habilitado_flag"] != m["pg_habilitado_flag"])

m["sgm_abast_norm"] = m["sgm_abastecimiento"].apply(_abast_norm)
m["pg_abast_norm"] = m["pg_abastecimiento"].apply(_abast_norm)
m["diff_abastecimiento"] = (
    m["sgm_abastecimiento"].notna()
    & m["pg_abastecimiento"].notna()
    & (m["sgm_abast_norm"] != m["pg_abast_norm"])
)

# Motivo probable
def motivo(row) -> str:
    en_sgm = bool(row.get("en_sgm", False))
    en_cnx = bool(row.get("en_connexa", False))

    if en_sgm and (not en_cnx):
        if bool(row.get("sgm_baja_flag", False)):
            return "SGM: Artículo dado de baja en maestro (T050.M_BAJA='S')"
        if not bool(row.get("sgm_habilitado_flag", False)):
            return "SGM: Artículo NO habilitado en la sucursal (T051.M_HABILITADO_SUCU='N')"
        return "SGM: Presente y habilitado, pero no figura en CONNEXA (revisar corrida nocturna / corte / filtros)"

    if (not en_sgm) and en_cnx:
        return "CONNEXA: Presente pero no se observa en SGM por sucursal (revisar extracción / filtros / mapeo)"

    if bool(row.get("diff_habilitado", False)):
        return "Diferencia de habilitación SGM vs CONNEXA"

    if bool(row.get("diff_abastecimiento", False)):
        return "Diferencia de sistemática/abastecimiento SGM vs CONNEXA"

    return "OK / Sin hallazgos"

m["motivo_probable"] = m.apply(motivo, axis=1)


# -------------------------------------------------------------------
# KPIs + Visualizaciones
# -------------------------------------------------------------------
colk1, colk2, colk3, colk4, colk5 = st.columns(5)
colk1.metric("Sucursales con registro en SGM (T051)", int(m["en_sgm"].sum()))
colk2.metric("Sucursales con registro en CONNEXA", int(m["en_connexa"].sum()))
colk3.metric("Faltan en CONNEXA (pero están en SGM)", int(((m["en_sgm"]) & (~m["en_connexa"])).sum()))
colk4.metric("Dif. habilitación", int(m["diff_habilitado"].sum()))
colk5.metric("Dif. sistemática", int(m["diff_abastecimiento"].sum()))

st.caption(
    f"Maestro SGM (T050): M_BAJA='{sgm_baja_art or '∅'}' | "
    f"F_ALTA={str(sgm_f_alta_art) if pd.notna(sgm_f_alta_art) else '∅'} | "
    f"F_BAJA={str(sgm_f_baja_art) if pd.notna(sgm_f_baja_art) else '∅'}"
)

st.divider()

df_bar = pd.DataFrame({
    "estado": ["En ambos", "Sólo SGM", "Sólo CONNEXA", "En ninguno"],
    "cantidad": [
        int(((m["en_sgm"]) & (m["en_connexa"])).sum()),
        int(((m["en_sgm"]) & (~m["en_connexa"])).sum()),
        int(((~m["en_sgm"]) & (m["en_connexa"])).sum()),
        int(((~m["en_sgm"]) & (~m["en_connexa"])).sum()),
    ],
})
fig = px.bar(df_bar, x="estado", y="cantidad", title="Cobertura del artículo por sucursal (SGM vs CONNEXA)")
st.plotly_chart(fig, width="stretch")

st.divider()

with st.expander("Filtros de diagnóstico", expanded=True):
    cA, cB, cC, cD = st.columns(4)
    with cA:
        ver_solo_faltantes = st.checkbox("Ver solo faltantes en CONNEXA (presentes en SGM)", value=True)
    with cB:
        ver_difs = st.checkbox("Ver solo diferencias (habilitación/sistemática)", value=False)
    with cC:
        ver_solo_hab_sgm = st.checkbox("Filtrar: SGM habilitado (S)", value=False)
    with cD:
        buscar = st.text_input("Buscar sucursal (código o nombre)", value="").strip().lower()

df_out = m.copy()
if ver_solo_faltantes:
    df_out = df_out[(df_out["en_sgm"]) & (~df_out["en_connexa"])].copy()
if ver_difs:
    df_out = df_out[(df_out["diff_habilitado"]) | (df_out["diff_abastecimiento"])].copy()
if ver_solo_hab_sgm:
    df_out = df_out[df_out["sgm_habilitado_flag"]].copy()
if buscar:
    df_out = df_out[
        df_out["c_sucu_empr"].astype(str).str.lower().str.contains(buscar)
        | df_out["suc_nombre"].astype(str).str.lower().str.contains(buscar)
    ].copy()

orden_cols = [
    "c_sucu_empr", "suc_nombre",
    "en_sgm", "en_connexa",
    "sgm_baja_art", "sgm_baja_flag", "sgm_f_alta_art", "sgm_f_baja_art",
    "sgm_habilitado_sucu", "sgm_habilitado_flag",
    "sgm_f_alta_sucu", "sgm_f_baja_sucu",
    "sgm_c_sistematica", "sgm_abastecimiento",
    "pg_habilitado_flag",
    "pg_active_for_purchase_flag", "pg_active_for_sale_flag", "pg_active_on_mix_flag",
    "pg_abastecimiento", "pg_cod_cd",
    "pg_fecha_registro", "pg_fecha_baja", "pg_fecha_extraccion", "pg_estado_sincronizacion",
    "pg_stock", "pg_pedido_pendiente", "pg_transfer_pendiente",
    "pg_fecha_stock", "pg_fecha_extraccion_stock", "pg_estado_sincronizacion_stock",
    "diff_habilitado", "diff_abastecimiento",
    "motivo_probable",
]
for c in orden_cols:
    if c not in df_out.columns:
        df_out[c] = pd.NA

df_out = df_out[orden_cols].sort_values(
    ["en_sgm", "en_connexa", "diff_habilitado", "diff_abastecimiento", "c_sucu_empr"],
    ascending=[False, True, False, False, True]
)

st.subheader("Comparación por sucursal")
st.dataframe(df_out, width="stretch", hide_index=True)

st.download_button(
    "Descargar CSV (comparación)",
    data=df_out.to_csv(index=False).encode("utf-8"),
    file_name=f"diagnostico_articulo_{int(c_articulo)}_prov_{int(prov_sel)}.csv",
    mime="text/csv",
)
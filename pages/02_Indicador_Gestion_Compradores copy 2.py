# pages/02_Indicador_Gestion_Compradores.py
# Indicador 2 — Gestión de Compradores (Connexa vs SGM)
# Objetivo gerencial:
#   - Medir performance por comprador: cantidad de OC y cantidad de proveedores atendidos.
#   - Separar Connexa vs SGM directo (SGM directo = no originado por Connexa/CI).
#   - Rankear adopción (%Connexa) y detectar focos de mejora.
#   - Incorporar evolución mensual y drill-down por comprador → proveedores (Connexa y SGM directo).
#   - Mostrar nombres de compradores desde diarco_data: src.m_9_compradores.

import os
from datetime import date
from typing import List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from modules.ui import render_header, make_date_filters
from modules.db import get_pg_engine, get_sqlserver_engine


# -------------------------------------------------------
# Configuración general
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 2 — Gestión de Compradores (Connexa vs SGM)",
    page_icon="🧑‍💼",
    layout="wide",
)

render_header("Indicador 2 — Gestión de Compradores (Connexa vs SGM)")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------
def _to_int64(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df is not None and not df.empty and col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _ensure_numeric_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _label_comprador_ui(c_comprador, n_comprador) -> str:
    """
    Etiqueta uniforme para UI:
      - Si hay nombre: "Nombre (código)"
      - Si no: "código" o "- sin comprador -"
    """
    n = None if pd.isna(n_comprador) else str(n_comprador).strip()
    if n:
        if pd.notna(c_comprador):
            try:
                return f"{n} ({int(c_comprador)})"
            except Exception:
                return f"{n} ({c_comprador})"
        return n

    if pd.notna(c_comprador):
        try:
            return str(int(c_comprador))
        except Exception:
            return str(c_comprador)

    return "- sin comprador -"


# -------------------------------------------------------
# SQL — DIMENSIÓN COMPRADORES (PostgreSQL diarco_data)
# -------------------------------------------------------
SQL_PG_DIM_COMPRADORES = text("""
SELECT
  cod_comprador::int AS c_comprador,
  n_comprador
FROM src.m_9_compradores;
""")


# -------------------------------------------------------
# SQL — RESUMEN POR COMPRADOR (Connexa)
# -------------------------------------------------------
SQL_PG_CONNEXA_COMPRADOR = text("""
SELECT
  c_comprador,
  COUNT(DISTINCT c_compra_connexa)                    AS oc_connexa,
  COUNT(DISTINCT c_proveedor)                        AS prov_connexa,
  SUM(COALESCE(q_bultos_kilos_diarco, 0))            AS bultos_connexa
FROM public.t080_oc_precarga_connexa
WHERE f_alta_sist >= :desde
  AND f_alta_sist < (:hasta + INTERVAL '1 day')
GROUP BY c_comprador
ORDER BY oc_connexa DESC;
""")


# -------------------------------------------------------
# SQL — RESUMEN POR COMPRADOR (SGM total / desde Connexa / directas)
# -------------------------------------------------------
SQL_SGM_COMPRADOR = text("""
WITH cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST) AS f_alta_date,
    C_COMPRADOR,
    C_PROVEEDOR,
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
t874 AS (
  SELECT DISTINCT
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
marca AS (
  SELECT
    c.*,
    CASE WHEN t.u_prefijo_oc IS NULL THEN 0 ELSE 1 END AS es_connexa
  FROM cabe c
  LEFT JOIN t874 t
    ON t.u_prefijo_oc = c.u_prefijo_oc AND t.u_sufijo_oc = c.u_sufijo_oc
)
SELECT
  C_COMPRADOR AS c_comprador,
  COUNT(DISTINCT oc_sgm)                                                   AS oc_sgm_total,
  COUNT(DISTINCT CASE WHEN es_connexa=1 THEN oc_sgm END)                   AS oc_sgm_desde_connexa,
  COUNT(DISTINCT CASE WHEN es_connexa=0 THEN oc_sgm END)                   AS oc_sgm_directas,
  COUNT(DISTINCT C_PROVEEDOR)                                              AS prov_sgm_total,
  COUNT(DISTINCT CASE WHEN es_connexa=1 THEN C_PROVEEDOR END)              AS prov_sgm_desde_connexa,
  COUNT(DISTINCT CASE WHEN es_connexa=0 THEN C_PROVEEDOR END)              AS prov_sgm_directos
FROM marca
GROUP BY C_COMPRADOR
ORDER BY oc_sgm_directas DESC;
""")


# -------------------------------------------------------
# SQL — EVOLUCIÓN MENSUAL (Connexa / SGM)
# -------------------------------------------------------
SQL_PG_CONNEXA_MENSUAL = text("""
SELECT
  date_trunc('month', f_alta_sist)::date AS mes,
  c_comprador,
  COUNT(DISTINCT c_compra_connexa)                 AS oc_connexa,
  COUNT(DISTINCT c_proveedor)                     AS prov_connexa,
  SUM(COALESCE(q_bultos_kilos_diarco, 0))         AS bultos_connexa
FROM public.t080_oc_precarga_connexa
WHERE f_alta_sist >= :desde
  AND f_alta_sist < (:hasta + INTERVAL '1 day')
GROUP BY 1, 2
ORDER BY mes, c_comprador;
""")

SQL_SGM_MENSUAL = text("""
WITH cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST) AS f_alta_date,
    C_COMPRADOR,
    C_PROVEEDOR,
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
t874 AS (
  SELECT DISTINCT
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
marca AS (
  SELECT
    c.*,
    CASE WHEN t.u_prefijo_oc IS NULL THEN 0 ELSE 1 END AS es_connexa
  FROM cabe c
  LEFT JOIN t874 t
    ON t.u_prefijo_oc = c.u_prefijo_oc AND t.u_sufijo_oc = c.u_sufijo_oc
),
marcada_mes AS (
  SELECT
    DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1) AS mes,
    C_COMPRADOR AS c_comprador,
    oc_sgm,
    C_PROVEEDOR,
    es_connexa
  FROM marca
)
SELECT
  mes,
  c_comprador,
  COUNT(DISTINCT oc_sgm)                                                   AS oc_sgm_total,
  COUNT(DISTINCT CASE WHEN es_connexa=1 THEN oc_sgm END)                   AS oc_sgm_desde_connexa,
  COUNT(DISTINCT CASE WHEN es_connexa=0 THEN oc_sgm END)                   AS oc_sgm_directas,
  COUNT(DISTINCT C_PROVEEDOR)                                              AS prov_sgm_total,
  COUNT(DISTINCT CASE WHEN es_connexa=1 THEN C_PROVEEDOR END)              AS prov_sgm_desde_connexa,
  COUNT(DISTINCT CASE WHEN es_connexa=0 THEN C_PROVEEDOR END)              AS prov_sgm_directos
FROM marcada_mes
GROUP BY mes, c_comprador
ORDER BY mes, c_comprador;
""")


# -------------------------------------------------------
# SQL — DRILL-DOWN PROVEEDORES (POR COMPRADOR Y MES)
# -------------------------------------------------------
SQL_PG_CONNEXA_PROVEEDORES_MENSUAL = text("""
SELECT
  date_trunc('month', f_alta_sist)::date AS mes,
  c_comprador,
  c_proveedor,
  COUNT(DISTINCT c_compra_connexa)                 AS oc_connexa
FROM public.t080_oc_precarga_connexa
WHERE f_alta_sist >= :desde
  AND f_alta_sist < (:hasta + INTERVAL '1 day')
  AND c_comprador = :c_comprador
GROUP BY 1, 2, 3
ORDER BY mes, oc_connexa DESC;
""")

SQL_SGM_PROVEEDORES_DIRECTO_MENSUAL = text("""
WITH cabe AS (
  SELECT
    TRY_CONVERT(date, F_ALTA_SIST) AS f_alta_date,
    C_COMPRADOR,
    C_PROVEEDOR,
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc,
    CONCAT(CAST(U_PREFIJO_OC AS varchar(32)), '-', CAST(U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
    AND C_COMPRADOR = :c_comprador
),
t874 AS (
  SELECT DISTINCT
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(U_PREFIJO_OC, 0) <> 0 AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
marca AS (
  SELECT
    c.*,
    CASE WHEN t.u_prefijo_oc IS NULL THEN 0 ELSE 1 END AS es_connexa
  FROM cabe c
  LEFT JOIN t874 t
    ON t.u_prefijo_oc = c.u_prefijo_oc AND t.u_sufijo_oc = c.u_sufijo_oc
),
marcada_mes AS (
  SELECT
    DATEFROMPARTS(YEAR(f_alta_date), MONTH(f_alta_date), 1) AS mes,
    C_COMPRADOR AS c_comprador,
    C_PROVEEDOR,
    oc_sgm,
    es_connexa
  FROM marca
)
SELECT
  mes,
  c_comprador,
  C_PROVEEDOR AS c_proveedor,
  COUNT(DISTINCT oc_sgm) AS oc_sgm_directas
FROM marcada_mes
WHERE es_connexa = 0
GROUP BY mes, c_comprador, C_PROVEEDOR
ORDER BY mes, oc_sgm_directas DESC;
""")


# -------------------------------------------------------
# Loaders (cacheados)
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def load_dim_compradores() -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        df_dim = pd.read_sql(SQL_PG_DIM_COMPRADORES, con)
    df_dim = _to_int64(df_dim, "c_comprador")
    if "n_comprador" in df_dim.columns:
        df_dim["n_comprador"] = df_dim["n_comprador"].astype(str).str.strip()
    return df_dim


@st.cache_data(ttl=ttl)
def load_connexa_resumen(desde: date, hasta: date) -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(SQL_PG_CONNEXA_COMPRADOR, con, params={"desde": desde, "hasta": hasta})
    df = _to_int64(df, "c_comprador")
    return df


@st.cache_data(ttl=ttl)
def load_sgm_resumen(desde: date, hasta: date) -> pd.DataFrame:
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_COMPRADOR, con, params={"desde": desde, "hasta": hasta})
    df = _to_int64(df, "c_comprador")
    return df


@st.cache_data(ttl=ttl)
def load_connexa_mensual(desde: date, hasta: date) -> pd.DataFrame:
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(SQL_PG_CONNEXA_MENSUAL, con, params={"desde": desde, "hasta": hasta})
    df = _to_int64(df, "c_comprador")
    if "mes" in df.columns:
        df["mes_dt"] = pd.to_datetime(df["mes"], errors="coerce")
    return df


@st.cache_data(ttl=ttl)
def load_sgm_mensual(desde: date, hasta: date) -> pd.DataFrame:
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(SQL_SGM_MENSUAL, con, params={"desde": desde, "hasta": hasta})
    df = _to_int64(df, "c_comprador")
    if "mes" in df.columns:
        df["mes_dt"] = pd.to_datetime(df["mes"], errors="coerce")
    return df


@st.cache_data(ttl=ttl)
def load_proveedores_mensual_por_comprador(desde: date, hasta: date, c_comprador: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Devuelven:
      - df_prov_connexa (PG): mes_dt, c_proveedor, oc_connexa
      - df_prov_sgm_dir (SQL): mes_dt, c_proveedor, oc_sgm_directas
    """
    # Connexa (PG)
    eng_pg = get_pg_engine()
    with eng_pg.connect() as con:
        df_cx = pd.read_sql(
            SQL_PG_CONNEXA_PROVEEDORES_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta, "c_comprador": int(c_comprador)},
        )
    df_cx = _ensure_numeric_cols(df_cx, ["oc_connexa"])
    df_cx["c_proveedor"] = pd.to_numeric(df_cx.get("c_proveedor"), errors="coerce").astype("Int64") # type: ignore
    df_cx["mes_dt"] = pd.to_datetime(df_cx.get("mes"), errors="coerce") # type: ignore

    # SGM directo (SQL)
    eng_sql = get_sqlserver_engine()
    if eng_sql is None:
        df_sgm = pd.DataFrame(columns=["mes_dt", "c_proveedor", "oc_sgm_directas"])
        return df_cx, df_sgm

    with eng_sql.connect() as con:
        df_sgm = pd.read_sql(
            SQL_SGM_PROVEEDORES_DIRECTO_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta, "c_comprador": int(c_comprador)},
        )
    df_sgm = _ensure_numeric_cols(df_sgm, ["oc_sgm_directas"])
    df_sgm["c_proveedor"] = pd.to_numeric(df_sgm.get("c_proveedor"), errors="coerce").astype("Int64") # type: ignore
    df_sgm["mes_dt"] = pd.to_datetime(df_sgm.get("mes"), errors="coerce") # type: ignore

    return df_cx, df_sgm


# -------------------------------------------------------
# Construcción DF Master (por comprador)
# -------------------------------------------------------
df_dim = load_dim_compradores()

df_cx = load_connexa_resumen(desde, hasta)
df_sgm = load_sgm_resumen(desde, hasta)

df = pd.merge(df_cx, df_sgm, on="c_comprador", how="outer")

df = _ensure_numeric_cols(
    df,
    [
        "oc_connexa", "prov_connexa", "bultos_connexa",
        "oc_sgm_total", "oc_sgm_desde_connexa", "oc_sgm_directas",
        "prov_sgm_total", "prov_sgm_desde_connexa", "prov_sgm_directos",
    ],
)

# Enriquecer con nombres
if df_dim is not None and not df_dim.empty:
    df = df.merge(df_dim, on="c_comprador", how="left")
else:
    df["n_comprador"] = None

df["comprador_label"] = df.apply(
    lambda r: _label_comprador_ui(r.get("c_comprador"), r.get("n_comprador")),
    axis=1,
)

# Actividad “accionable” para adopción:
#   - Total actividad = Connexa + SGM directas (lo que compite con Connexa)
df["oc_total_actividad"] = df["oc_connexa"] + df["oc_sgm_directas"]
df["prov_total_actividad"] = df["prov_connexa"] + df["prov_sgm_directos"]

df["pct_connexa"] = df.apply(
    lambda r: (r["oc_connexa"] / r["oc_total_actividad"]) if r["oc_total_actividad"] else 0.0,
    axis=1,
)

# Penetración Connexa “dentro” de SGM (validación circuito):
df["pct_connexa_en_sgm"] = df.apply(
    lambda r: (r["oc_sgm_desde_connexa"] / r["oc_sgm_total"]) if r["oc_sgm_total"] else 0.0,
    axis=1,
)


# -------------------------------------------------------
# Resumen ejecutivo
# -------------------------------------------------------
st.markdown("## Resumen ejecutivo")

if df.empty:
    st.info("No se encontraron datos en el rango seleccionado.")
    st.stop()

compradores_activos = int(df["c_comprador"].nunique()) if "c_comprador" in df.columns else int(df["comprador_label"].nunique())
tot_oc_connexa = int(_safe_sum(df, "oc_connexa"))
tot_oc_sgm_dir = int(_safe_sum(df, "oc_sgm_directas"))
tot_oc_actividad = int(_safe_sum(df, "oc_total_actividad"))
tot_bultos_connexa = int(_safe_sum(df, "bultos_connexa"))

pct_pond = (tot_oc_connexa / tot_oc_actividad) if tot_oc_actividad else 0.0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Compradores (rango)", compradores_activos)
c2.metric("OC Connexa", tot_oc_connexa)
c3.metric("OC SGM directas", tot_oc_sgm_dir)
c4.metric("OC actividad total (Connexa + SGM directas)", tot_oc_actividad)
c5.metric("% Connexa (ponderado por OC)", f"{pct_pond:.1%}")

st.caption("Nota: la adopción se calcula sobre la actividad comparable (Connexa + SGM directas).")

st.divider()


# -------------------------------------------------------
# Tabs
# -------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(
    ["📊 Visión gerencial", "🏅 Ranking", "📋 Detalle", "📈 Evolución mensual"]
)

# -----------------------------
# TAB 1 — Visión gerencial
# -----------------------------
with tab1:
    st.subheader("Mapa de adopción (tamaño = #Proveedores totales)")

    df_plot = df[df["oc_total_actividad"] > 0].copy()
    df_plot = df_plot.sort_values("oc_total_actividad", ascending=False)

    fig_scatter = px.scatter(
        df_plot,
        x="oc_total_actividad",
        y="pct_connexa",
        size="prov_total_actividad",
        hover_name="comprador_label",
        title="Adopción Connexa por comprador",
    )
    fig_scatter.update_layout(
        xaxis_title="OC totales (Connexa + SGM directas)",
        yaxis_title="% Connexa",
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    st.subheader("Comparativo de OC por comprador (Top 30 por actividad)")
    df_bar = df.sort_values("oc_total_actividad", ascending=False).head(30)
    fig_bar = px.bar(
        df_bar,
        x="comprador_label",
        y=["oc_connexa", "oc_sgm_directas"],
        barmode="stack",
        title="OC por comprador — Connexa vs SGM directas",
    )
    fig_bar.update_layout(
        xaxis_title="Comprador",
        yaxis_title="Cantidad de OC",
        xaxis_tickangle=-45,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Validación de circuito (penetración Connexa dentro de SGM)")
    df_val = df.sort_values("pct_connexa_en_sgm", ascending=True).head(30)
    fig_val = px.bar(
        df_val,
        x="pct_connexa_en_sgm",
        y="comprador_label",
        orientation="h",
        title="Top 30 compradores con menor %Connexa dentro de SGM (indicador técnico)",
    )
    fig_val.update_layout(
        xaxis_title="% Connexa en SGM (OC SGM desde Connexa / OC SGM total)",
        yaxis_title="Comprador",
    )
    st.plotly_chart(fig_val, use_container_width=True)


# -----------------------------
# TAB 2 — Ranking accionable
# -----------------------------
with tab2:
    st.subheader("Ranking accionable para impulsar Connexa")

    colA, colB = st.columns(2)
    with colA:
        min_oc = st.number_input(
            "Mínimo de OC totales (Connexa + SGM directas) para rankear",
            min_value=0,
            value=20,
            step=10,
        )
    with colB:
        topn = st.slider("Top N", min_value=5, max_value=50, value=20, step=5)

    base = df[df["oc_total_actividad"] >= min_oc].copy()

    st.markdown("### 1) Menor %Connexa (prioridad de adopción)")
    rk1 = base.sort_values(["pct_connexa", "oc_total_actividad"], ascending=[True, False]).head(topn)
    st.dataframe(
        rk1[[
            "comprador_label",
            "oc_connexa",
            "oc_sgm_directas",
            "oc_total_actividad",
            "pct_connexa",
            "prov_total_actividad",
        ]],
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### 2) Mayor volumen de OC SGM directas (impacto inmediato)")
    rk2 = base.sort_values(["oc_sgm_directas", "oc_total_actividad"], ascending=[False, False]).head(topn)
    st.dataframe(
        rk2[[
            "comprador_label",
            "oc_sgm_directas",
            "oc_connexa",
            "pct_connexa",
            "prov_sgm_directos",
            "prov_connexa",
        ]],
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("### 3) Mayor amplitud de proveedores SGM directos (resistencia por cartera)")
    rk3 = base.sort_values(["prov_sgm_directos", "oc_sgm_directas"], ascending=[False, False]).head(topn)
    st.dataframe(
        rk3[[
            "comprador_label",
            "prov_sgm_directos",
            "prov_connexa",
            "oc_sgm_directas",
            "oc_connexa",
            "pct_connexa",
        ]],
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------
# TAB 3 — Detalle exportable
# -----------------------------
with tab3:
    st.subheader("Detalle por comprador (auditable y exportable)")

    df_out = df.sort_values(["pct_connexa", "oc_total_actividad"], ascending=[True, False]).copy()

    st.dataframe(df_out, use_container_width=True, hide_index=True)

    st.download_button(
        "Descargar CSV (detalle compradores)",
        data=df_out.to_csv(index=False).encode("utf-8"),
        file_name="gestion_compradores_connexa_vs_sgm.csv",
        mime="text/csv",
    )


# -----------------------------
# TAB 4 — Evolución mensual + drill-down por proveedor
# -----------------------------
with tab4:
    st.subheader("Evolución mensual de adopción (Connexa vs SGM directas)")

    df_cx_m = load_connexa_mensual(desde, hasta)
    df_sgm_m = load_sgm_mensual(desde, hasta)

    # Merge mensual robusto: por (mes, c_comprador)
    # Nota: mes_dt se recalcula luego para evitar problemas por diferencias de tipo/precisión
    df_m = pd.merge(
        df_cx_m.drop(columns=["mes_dt"], errors="ignore"),
        df_sgm_m.drop(columns=["mes_dt"], errors="ignore"),
        on=["mes", "c_comprador"],
        how="outer",
    )

    df_m = _ensure_numeric_cols(
        df_m,
        [
            "oc_connexa", "prov_connexa", "bultos_connexa",
            "oc_sgm_total", "oc_sgm_desde_connexa", "oc_sgm_directas",
            "prov_sgm_total", "prov_sgm_desde_connexa", "prov_sgm_directos",
        ],
    )
    df_m = _to_int64(df_m, "c_comprador")

    # mes_dt normalizado
    df_m["mes_dt"] = pd.to_datetime(df_m.get("mes"), errors="coerce") # type: ignore

    # Enriquecer con nombres
    if df_dim is not None and not df_dim.empty:
        df_m = df_m.merge(df_dim, on="c_comprador", how="left")
    else:
        df_m["n_comprador"] = None

    df_m["comprador_label"] = df_m.apply(
        lambda r: _label_comprador_ui(r.get("c_comprador"), r.get("n_comprador")),
        axis=1,
    )

    df_m["oc_total_actividad"] = df_m["oc_connexa"] + df_m["oc_sgm_directas"]
    df_m["pct_connexa"] = df_m.apply(
        lambda r: (r["oc_connexa"] / r["oc_total_actividad"]) if r["oc_total_actividad"] else 0.0,
        axis=1,
    )

    df_m = df_m[df_m["mes_dt"].notna()].copy()
    df_m = df_m.sort_values(["mes_dt", "comprador_label"])

    if df_m.empty:
        st.info("No hay datos mensuales para el rango seleccionado.")
    else:
        # Defaults: top 5 por actividad total del período (según master)
        top_default_labels = (
            df.sort_values("oc_total_actividad", ascending=False)["comprador_label"].head(5).tolist()
            if not df.empty else []
        )
        compradores_labels = sorted(df_m["comprador_label"].dropna().unique().tolist())

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            sel_labels = st.multiselect(
                "Seleccionar compradores (para evolución)",
                options=compradores_labels,
                default=top_default_labels if top_default_labels else compradores_labels[:5],
            )
        with col2:
            modo = st.radio(
                "Vista",
                options=["%Connexa", "OC (Connexa vs SGM directas)"],
                horizontal=False,
            )
        with col3:
            min_oc_mes = st.number_input(
                "Mínimo OC totales/mes (filtra ruido)",
                min_value=0,
                value=0,
                step=5,
            )

        base = df_m[df_m["comprador_label"].isin(sel_labels)].copy() if sel_labels else df_m.copy()
        if min_oc_mes > 0:
            base = base[base["oc_total_actividad"] >= min_oc_mes].copy()

        if base.empty:
            st.warning("Con los filtros actuales no quedan datos para graficar.")
        else:
            if modo == "%Connexa":
                fig = px.line(
                    base,
                    x="mes_dt",
                    y="pct_connexa",
                    color="comprador_label",
                    markers=True,
                    title="Evolución mensual del %Connexa por comprador",
                )
                fig.update_layout(
                    xaxis_title="Mes",
                    yaxis_title="% Connexa (OC Connexa / (OC Connexa + OC SGM directas))",
                )
                st.plotly_chart(fig, use_container_width=True)

                st.markdown("#### Resumen mensual (ponderado por OC)")
                agg = base.groupby("mes_dt", as_index=False).agg(
                    oc_connexa=("oc_connexa", "sum"),
                    oc_sgm_directas=("oc_sgm_directas", "sum"),
                )
                agg["oc_total"] = agg["oc_connexa"] + agg["oc_sgm_directas"]
                agg["pct_connexa_ponderado"] = agg.apply(
                    lambda r: (r["oc_connexa"] / r["oc_total"]) if r["oc_total"] else 0.0,
                    axis=1,
                )
                st.dataframe(agg, use_container_width=True, hide_index=True)
            else:
                fig2 = px.bar(
                    base,
                    x="mes_dt",
                    y=["oc_connexa", "oc_sgm_directas"],
                    barmode="group",
                    title="OC mensuales (Connexa vs SGM directas)",
                )
                fig2.update_layout(
                    xaxis_title="Mes",
                    yaxis_title="Cantidad de OC",
                )
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("#### Detalle mensual por comprador")
            st.dataframe(
                base[[
                    "mes_dt",
                    "comprador_label",
                    "oc_connexa",
                    "oc_sgm_directas",
                    "oc_total_actividad",
                    "pct_connexa",
                    "prov_connexa",
                    "prov_sgm_directos",
                ]].sort_values(["mes_dt", "comprador_label"]),
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "Descargar CSV (evolución mensual)",
                data=base.to_csv(index=False).encode("utf-8"),
                file_name="gestion_compradores_evolucion_mensual.csv",
                mime="text/csv",
            )

        st.divider()

        # -------------------------------------------------------
        # Drill-down: seleccionar comprador → ver proveedores (Connexa y SGM directo)
        # -------------------------------------------------------
        st.subheader("Drill-down: Proveedores por comprador (Connexa vs SGM directas)")

        # Mapping label -> código (clave técnica para parametrizar queries)
        map_label_to_code = (
            df[["comprador_label", "c_comprador"]]
            .drop_duplicates()
            .dropna(subset=["c_comprador"])
            .set_index("comprador_label")["c_comprador"]
            .to_dict()
        )
        compradores_con_codigo = sorted(map_label_to_code.keys())

        colx, coly, colz = st.columns([2, 1, 1])
        with colx:
            sel_buy_label = st.selectbox(
                "Seleccionar comprador (para proveedores)",
                options=compradores_con_codigo if compradores_con_codigo else ["(sin compradores con código)"],
                index=0,
            )
        with coly:
            top_prov = st.slider("Top proveedores (en el rango)", min_value=10, max_value=200, value=50, step=10)
        with colz:
            modo_prov = st.radio(
                "Ordenar ranking por",
                options=["Total", "Connexa", "SGM directas"],
                horizontal=False,
            )

        if not compradores_con_codigo:
            st.info("No hay compradores con código numérico para habilitar el drill-down por proveedores.")
        else:
            c_comprador = int(map_label_to_code[sel_buy_label])

            df_prov_cx, df_prov_sgm = load_proveedores_mensual_por_comprador(desde, hasta, c_comprador)

            # Normalización
            df_prov_cx = df_prov_cx[df_prov_cx["mes_dt"].notna()].copy()
            df_prov_sgm = df_prov_sgm[df_prov_sgm["mes_dt"].notna()].copy()

            df_prov = pd.merge(
                df_prov_cx[["mes_dt", "c_proveedor", "oc_connexa"]] if not df_prov_cx.empty else pd.DataFrame(columns=["mes_dt", "c_proveedor", "oc_connexa"]),
                df_prov_sgm[["mes_dt", "c_proveedor", "oc_sgm_directas"]] if not df_prov_sgm.empty else pd.DataFrame(columns=["mes_dt", "c_proveedor", "oc_sgm_directas"]),
                on=["mes_dt", "c_proveedor"],
                how="outer",
            )
            df_prov = _ensure_numeric_cols(df_prov, ["oc_connexa", "oc_sgm_directas"])
            df_prov["c_proveedor"] = pd.to_numeric(df_prov.get("c_proveedor"), errors="coerce").astype("Int64") # type: ignore

            # Ranking por proveedor (en todo el rango)
            tot_prov = df_prov.groupby("c_proveedor", as_index=False).agg(
                oc_connexa=("oc_connexa", "sum"),
                oc_sgm_directas=("oc_sgm_directas", "sum"),
            )
            tot_prov["oc_total"] = tot_prov["oc_connexa"] + tot_prov["oc_sgm_directas"]
            tot_prov["pct_connexa"] = tot_prov.apply(
                lambda r: (r["oc_connexa"] / r["oc_total"]) if r["oc_total"] else 0.0,
                axis=1,
            )

            if modo_prov == "Connexa":
                tot_prov = tot_prov.sort_values(["oc_connexa", "oc_total"], ascending=[False, False])
            elif modo_prov == "SGM directas":
                tot_prov = tot_prov.sort_values(["oc_sgm_directas", "oc_total"], ascending=[False, False])
            else:
                tot_prov = tot_prov.sort_values(["oc_total"], ascending=False)

            tot_prov = tot_prov.head(top_prov)

            df_prov_top = df_prov[df_prov["c_proveedor"].isin(tot_prov["c_proveedor"])].copy()

            st.markdown("### Ranking de proveedores (en el rango)")
            st.dataframe(
                tot_prov[["c_proveedor", "oc_connexa", "oc_sgm_directas", "oc_total", "pct_connexa"]],
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("### Evolución mensual por proveedor (Top seleccionados)")
            if df_prov_top.empty:
                st.info("No hay datos de proveedores para los filtros seleccionados.")
            else:
                met = st.radio("Métrica", ["OC Connexa", "OC SGM directas", "OC Total", "% Connexa"], horizontal=True)

                aux = df_prov_top.copy()
                aux["oc_total"] = aux["oc_connexa"] + aux["oc_sgm_directas"]
                aux["pct_connexa"] = aux.apply(
                    lambda r: (r["oc_connexa"] / r["oc_total"]) if r["oc_total"] else 0.0,
                    axis=1,
                )
                aux["proveedor_label"] = aux["c_proveedor"].astype("Int64").astype(str)

                ycol = {
                    "OC Connexa": "oc_connexa",
                    "OC SGM directas": "oc_sgm_directas",
                    "OC Total": "oc_total",
                    "% Connexa": "pct_connexa",
                }[met]

                figp = px.line(
                    aux.sort_values("mes_dt"),
                    x="mes_dt",
                    y=ycol,
                    color="proveedor_label",
                    markers=True,
                    title=f"Evolución mensual por proveedor — {met}",
                )
                figp.update_layout(
                    xaxis_title="Mes",
                    yaxis_title=met,
                    legend_title="Proveedor",
                )
                st.plotly_chart(figp, use_container_width=True)

                st.markdown("### Detalle mensual (proveedor × mes)")
                st.dataframe(
                    aux[["mes_dt", "c_proveedor", "oc_connexa", "oc_sgm_directas", "oc_total", "pct_connexa"]]
                    .sort_values(["mes_dt", "oc_total"], ascending=[True, False]),
                    use_container_width=True,
                    hide_index=True,
                )

                st.download_button(
                    "Descargar CSV (proveedores por mes)",
                    data=aux.to_csv(index=False).encode("utf-8"),
                    file_name=f"gestion_compradores_proveedores_mensual_comprador_{c_comprador}.csv",
                    mime="text/csv",
                )

# pages/01_Indicador_Uso_General.py
# Indicador 1 — Uso general del sistema
#
# Responde a:
#   a) Evolución mensual de los indicadores de utilización del sistema
#   b) Volumen total gestionado
#   c) Tiempo medio utilizado
#   d) Efectividad
#
# Mejora (Feb-2026):
#   - Se agrega "Matriz de adopción semanal — % Connexa (OC CNX / OC SGM)"
#     usando vistas SQL Server:
#       - [data-sync].[dbo].[V_USO_SEMANAL_COMPRADOR]
#       - [data-sync].[dbo].[V_USO_SEMANAL_PROVEEDOR]
#     donde SEMANA tiene formato 'YYYY-WW' (año ISO + semana ISO).
#   - Se agrega matriz por proveedor con:
#       - filtro mínimo de OC SGM (por semana y/o por rango)
#       - nombre de proveedor (desde SGM: DiarcoP.dbo.T020_PROVEEDOR)

from datetime import date, datetime
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from modules.db import (
    get_connexa_engine,   # connexa_platform_ms
    get_diarco_engine,    # diarco_data
    get_sqlserver_engine  # SGM (SQL Server)
)

from modules.ui import render_header, make_date_filters

from modules.queries.uso_general import (
    ensure_mon_objects,
    ensure_forecast_views,
    get_oc_generadas_mensual,
    get_forecast_propuesta_conversion_mensual,
    get_embudo_connexa_sgm_mensual,
    get_proporcion_ci_vs_sgm_mensual,
)


# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 1 — Uso general del sistema",
    page_icon="📈",
    layout="wide",
)

render_header("Indicador 1 — Uso general del sistema")

# Filtros de fecha
desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# =======================================================
# 1. Helpers (para matriz semanal)
# =======================================================
def _to_int64(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df is not None and not df.empty and col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _ensure_numeric_cols(df: pd.DataFrame, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def _label_comprador_ui(c_comprador, n_comprador) -> str:
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


def _label_proveedor_ui(c_proveedor, n_proveedor) -> str:
    n = None if pd.isna(n_proveedor) else str(n_proveedor).strip()
    if n:
        if pd.notna(c_proveedor):
            try:
                return f"{n} ({int(c_proveedor)})"
            except Exception:
                return f"{n} ({c_proveedor})"
        return n

    if pd.notna(c_proveedor):
        try:
            return str(int(c_proveedor))
        except Exception:
            return str(c_proveedor)

    return "- sin proveedor -"


def _iso_week_monday(yyyy_ww: str):
    """
    Convierte 'YYYY-WW' a fecha del lunes de esa semana ISO.
    Usa año ISO (%G) y semana ISO (%V).
    """
    if not yyyy_ww or not isinstance(yyyy_ww, str):
        return pd.NaT
    s = yyyy_ww.strip()
    try:
        year_str, week_str = s.split("-")
        year = int(year_str)
        week = int(week_str)
        dt = datetime.strptime(f"{year}-W{week:02d}-1", "%G-W%V-%u")  # lunes ISO = 1
        return pd.to_datetime(dt.date())
    except Exception:
        return pd.NaT


# =======================================================
# 2. Funciones auxiliares y de caché
# =======================================================
@st.cache_data(ttl=ttl)
def _init_mon(desde: date, hasta: date) -> bool:
    """
    Asegura que existan las vistas mon.* y mon.v_forecast_propuesta_base.
    """
    try:
        eng_d = get_diarco_engine()
        eng_c = get_connexa_engine()
        if eng_d is not None:
            ensure_mon_objects(eng_d)
        if eng_c is not None:
            ensure_forecast_views(eng_c)
        return True
    except Exception:
        return False


@st.cache_data(ttl=ttl)
def load_uso_general(desde: date, hasta: date):
    """
    Carga los principales DataFrames usados por el indicador de uso general:
      - df_oc     : OC generadas desde Connexa (mon.v_oc_generadas_mensual)
      - df_fp     : Forecast → Propuesta (serie mensual, conversión)
      - df_emb    : Embudo Connexa → SGM (pedidos vs OC, bultos)
      - df_prop   : Proporción de OC SGM originadas en Connexa
    """
    _ = _init_mon(desde, hasta)

    eng_d = get_diarco_engine()
    eng_c = get_connexa_engine()
    eng_s = get_sqlserver_engine()

    df_oc = get_oc_generadas_mensual(eng_d, desde, hasta) if eng_d else pd.DataFrame()
    df_fp = get_forecast_propuesta_conversion_mensual(eng_c, desde, hasta) if eng_c else pd.DataFrame()
    df_emb = get_embudo_connexa_sgm_mensual(eng_d, eng_s, desde, hasta) if (eng_d and eng_s) else pd.DataFrame()
    df_prop = get_proporcion_ci_vs_sgm_mensual(eng_s, desde, hasta) if eng_s else pd.DataFrame()

    return df_oc, df_fp, df_emb, df_prop


# --- SQL local para tiempos medios (sobre mon.v_forecast_propuesta_base) ---
SQL_TIEMPOS_MENSUALES = text("""
SELECT
  date_trunc('month', (base_ts AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
  COUNT(*)                                                AS propuestas,
  AVG(exec_time_min)                                      AS avg_exec_min,
  AVG(lead_open_min)                                      AS avg_lead_open_min,
  AVG(adjust_time_min)                                    AS avg_adjust_min,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY adjust_time_min) AS p50_adjust_min,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY adjust_time_min) AS p90_adjust_min
FROM mon.v_forecast_propuesta_base
WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
  AND pp_id IS NOT NULL
GROUP BY 1
ORDER BY 1;
""")


@st.cache_data(ttl=ttl)
def load_tiempos_mensuales(desde: date, hasta: date) -> pd.DataFrame:
    """
    Devuelve tiempos medios de uso del sistema (Forecast → Propuesta),
    agregados por mes a partir de mon.v_forecast_propuesta_base.
    """
    eng_c = get_connexa_engine()
    if eng_c is None:
        return pd.DataFrame()

    _ = _init_mon(desde, hasta)

    with eng_c.connect() as con:
        df = pd.read_sql(SQL_TIEMPOS_MENSUALES, con, params={"desde": desde, "hasta": hasta})

    if df.empty:
        return df

    df["mes"] = pd.to_datetime(df["mes"])

    for col in ("avg_exec_min", "avg_lead_open_min", "avg_adjust_min",
                "p50_adjust_min", "p90_adjust_min"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =======================================================
# 3. Matriz adopción semanal (SQL Server data-sync)
# =======================================================
SQL_PG_DIM_COMPRADORES = text("""
SELECT
  cod_comprador::int AS c_comprador,
  n_comprador
FROM src.m_9_compradores;
""")

SQL_SGM_USO_SEMANAL_COMPRADOR = text("""
SELECT
    CAST(C_COMPRADOR AS int)         AS c_comprador,
    CAST(SEMANA AS varchar(10))      AS semana_yyyyww,   -- 'YYYY-WW' (ISO year-week)
    CAST(Total_OC_CNX AS float)      AS total_oc_cnx,
    CAST(Total_OC_SGM AS float)      AS total_oc_sgm,
    CAST(Total_Prv_CNX AS float)     AS total_prv_cnx,
    CAST(Total_Prv_SGM AS float)     AS total_prv_sgm,
    CAST(Total_BULTOS_CNX AS float)  AS total_bultos_cnx,
    CAST(Total_BULTOS_SGM AS float)  AS total_bultos_sgm
FROM [data-sync].[dbo].[V_USO_SEMANAL_COMPRADOR];
""")

SQL_SGM_USO_SEMANAL_PROVEEDOR = text("""
SELECT
    CAST(C_PROVEEDOR AS int)         AS c_proveedor,
    CAST(SEMANA AS varchar(10))      AS semana_yyyyww,   -- 'YYYY-WW' (ISO year-week)
    CAST(Total_Pedidos_CNX AS float) AS total_pedidos_cnx,
    CAST(Total_OC_CNX AS float)      AS total_oc_cnx,
    CAST(Total_BULTOS_CNX AS float)  AS total_bultos_cnx,
    CAST(Total_OC_SGM AS float)      AS total_oc_sgm,
    CAST(Total_BULTOS_SGM AS float)  AS total_bultos_sgm
FROM [data-sync].[dbo].[V_USO_SEMANAL_PROVEEDOR];
""")

# Dim de proveedores (nombre) desde SGM (DiarcoP)
# Nota: si el nombre de columnas difiere en su instalación, se ajusta aquí.
SQL_SGM_DIM_PROVEEDORES = text("""
SELECT
    CAST(C_PROVEEDOR AS int) AS c_proveedor,
    LTRIM(RTRIM(COALESCE(N_PROVEEDOR, ''))) AS n_proveedor
FROM [DIARCOP001].[DiarcoP].[dbo].[T020_PROVEEDOR];
""")


@st.cache_data(ttl=ttl)
def load_dim_compradores() -> pd.DataFrame:
    eng_d = get_diarco_engine()
    if eng_d is None:
        return pd.DataFrame(columns=["c_comprador", "n_comprador"])
    with eng_d.connect() as con:
        df_dim = pd.read_sql(SQL_PG_DIM_COMPRADORES, con)
    df_dim = _to_int64(df_dim, "c_comprador")
    if "n_comprador" in df_dim.columns:
        df_dim["n_comprador"] = df_dim["n_comprador"].astype(str).str.strip()
    return df_dim


@st.cache_data(ttl=ttl)
def load_dim_proveedores() -> pd.DataFrame:
    """
    Dimension de proveedores (código → nombre) desde SGM (SQL Server DiarcoP).
    """
    eng_s = get_sqlserver_engine()
    if eng_s is None:
        return pd.DataFrame(columns=["c_proveedor", "n_proveedor"])
    try:
        with eng_s.connect() as con:
            dfp = pd.read_sql(SQL_SGM_DIM_PROVEEDORES, con)
        dfp = _to_int64(dfp, "c_proveedor")
        if "n_proveedor" in dfp.columns:
            dfp["n_proveedor"] = dfp["n_proveedor"].astype(str).str.strip()
        return dfp
    except Exception:
        # Fallback silencioso: se muestra sólo código si no se puede leer la dimensión
        return pd.DataFrame(columns=["c_proveedor", "n_proveedor"])


@st.cache_data(ttl=ttl)
def load_uso_semanal_comprador(desde: date, hasta: date) -> pd.DataFrame:
    """
    Trae la vista semanal consolidada desde SQL Server (data-sync):
      [data-sync].[dbo].[V_USO_SEMANAL_COMPRADOR]
    donde SEMANA tiene formato 'YYYY-WW' (año ISO + semana ISO).
    Filtra por rango usando el lunes ISO (semana_inicio).
    """
    eng_s = get_sqlserver_engine()
    if eng_s is None:
        return pd.DataFrame(columns=[
            "c_comprador", "semana_yyyyww", "semana_inicio",
            "total_oc_cnx", "total_oc_sgm",
            "total_prv_cnx", "total_prv_sgm",
            "total_bultos_cnx", "total_bultos_sgm",
            "pct_connexa_sem"
        ])

    with eng_s.connect() as con:
        dfu = pd.read_sql(SQL_SGM_USO_SEMANAL_COMPRADOR, con)

    dfu = _to_int64(dfu, "c_comprador")
    dfu["semana_yyyyww"] = dfu.get("semana_yyyyww", "").astype(str).str.strip() # type: ignore

    dfu = _ensure_numeric_cols(dfu, [
        "total_oc_cnx", "total_oc_sgm",
        "total_prv_cnx", "total_prv_sgm",
        "total_bultos_cnx", "total_bultos_sgm"
    ])

    dfu["semana_inicio"] = dfu["semana_yyyyww"].apply(_iso_week_monday) # type: ignore

    d0 = pd.to_datetime(desde)
    d1 = pd.to_datetime(hasta)

    dfu = dfu[dfu["semana_inicio"].notna()].copy()
    dfu = dfu[(dfu["semana_inicio"] >= d0) & (dfu["semana_inicio"] <= d1)].copy()

    dfu["pct_connexa_sem"] = dfu.apply(
        lambda r: (r["total_oc_cnx"] / r["total_oc_sgm"]) if r["total_oc_sgm"] else 0.0,
        axis=1
    )

    return dfu


@st.cache_data(ttl=ttl)
def load_uso_semanal_proveedor(desde: date, hasta: date) -> pd.DataFrame:
    """
    Trae la vista semanal por proveedor desde SQL Server (data-sync):
      [data-sync].[dbo].[V_USO_SEMANAL_PROVEEDOR]
    donde SEMANA tiene formato 'YYYY-WW' (año ISO + semana ISO).
    Filtra por rango usando el lunes ISO (semana_inicio).
    """
    eng_s = get_sqlserver_engine()
    if eng_s is None:
        return pd.DataFrame(columns=[
            "c_proveedor", "semana_yyyyww", "semana_inicio",
            "total_oc_cnx", "total_oc_sgm",
            "total_pedidos_cnx",
            "total_bultos_cnx", "total_bultos_sgm",
            "pct_connexa_sem"
        ])

    with eng_s.connect() as con:
        dfu = pd.read_sql(SQL_SGM_USO_SEMANAL_PROVEEDOR, con)

    dfu = _to_int64(dfu, "c_proveedor")
    dfu["semana_yyyyww"] = dfu.get("semana_yyyyww", "").astype(str).str.strip() # type: ignore

    dfu = _ensure_numeric_cols(dfu, [
        "total_oc_cnx", "total_oc_sgm",
        "total_pedidos_cnx",
        "total_bultos_cnx", "total_bultos_sgm",
    ])

    dfu["semana_inicio"] = dfu["semana_yyyyww"].apply(_iso_week_monday) # type: ignore

    d0 = pd.to_datetime(desde)
    d1 = pd.to_datetime(hasta)

    dfu = dfu[dfu["semana_inicio"].notna()].copy()
    dfu = dfu[(dfu["semana_inicio"] >= d0) & (dfu["semana_inicio"] <= d1)].copy()

    dfu["pct_connexa_sem"] = dfu.apply(
        lambda r: (r["total_oc_cnx"] / r["total_oc_sgm"]) if r["total_oc_sgm"] else 0.0,
        axis=1
    )

    return dfu


def build_weekly_matrix_comprador(dfu: pd.DataFrame, df_dim: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Devuelve:
      - df_pivot: comprador_label + columnas semana_yyyyww con pct_connexa_sem
      - df_global_week: semana_yyyyww + pct_global (sum oc_cnx / sum oc_sgm)
    """
    if dfu is None or dfu.empty:
        return pd.DataFrame(columns=["comprador_label"]), pd.DataFrame(columns=["semana_yyyyww", "pct_global"])

    # Enriquecer con nombres comprador
    if df_dim is not None and not df_dim.empty:
        dfu2 = dfu.merge(df_dim, on="c_comprador", how="left")
    else:
        dfu2 = dfu.copy()
        if "n_comprador" not in dfu2.columns:
            dfu2["n_comprador"] = None

    dfu2["comprador_label"] = dfu2.apply(
        lambda r: _label_comprador_ui(r.get("c_comprador"), r.get("n_comprador")),
        axis=1
    )

    # Orden semanas por fecha lunes ISO
    week_order = (
        dfu2[["semana_yyyyww", "semana_inicio"]]
        .drop_duplicates()
        .sort_values("semana_inicio")["semana_yyyyww"]
        .tolist()
    )

    # Pivot por comprador
    df_pivot = pd.pivot_table(
        dfu2,
        index="comprador_label",
        columns="semana_yyyyww",
        values="pct_connexa_sem",
        aggfunc="mean",
        fill_value=0.0
    ).reset_index()

    cols = ["comprador_label"] + [w for w in week_order if w in df_pivot.columns]
    df_pivot = df_pivot[cols]

    # Global semanal (sumatoria, más robusto que promedio simple)
    g = dfu2.groupby(["semana_yyyyww"], as_index=False).agg(
        total_oc_cnx=("total_oc_cnx", "sum"),
        total_oc_sgm=("total_oc_sgm", "sum"),
        semana_inicio=("semana_inicio", "min"),
    )
    g = g.sort_values("semana_inicio")
    g["pct_global"] = g.apply(
        lambda r: (r["total_oc_cnx"] / r["total_oc_sgm"]) if r["total_oc_sgm"] else 0.0,
        axis=1
    )

    return df_pivot, g[["semana_yyyyww", "pct_global", "total_oc_cnx", "total_oc_sgm"]]


def build_weekly_matrix_proveedor(
    dfu: pd.DataFrame,
    df_dim_prv: pd.DataFrame,
    min_oc_sgm_semana: int = 0,
    min_oc_sgm_rango: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Construye matriz semanal por proveedor con filtros:
      - min_oc_sgm_semana: elimina filas proveedor×semana con Total_OC_SGM < umbral
      - min_oc_sgm_rango : elimina proveedores cuyo Total_OC_SGM (sum en rango) < umbral

    Devuelve:
      - df_pivot: proveedor_label + columnas semana_yyyyww con pct_connexa_sem
      - df_global_week: semana_yyyyww + pct_global (sum oc_cnx / sum oc_sgm)
    """
    if dfu is None or dfu.empty:
        return pd.DataFrame(columns=["proveedor_label"]), pd.DataFrame(columns=["semana_yyyyww", "pct_global"])

    dfu2 = dfu.copy()

    # Filtro por mínimo OC SGM por semana (reduce ruido)
    if min_oc_sgm_semana and min_oc_sgm_semana > 0:
        dfu2 = dfu2[dfu2["total_oc_sgm"] >= float(min_oc_sgm_semana)].copy()

    if dfu2.empty:
        return pd.DataFrame(columns=["proveedor_label"]), pd.DataFrame(columns=["semana_yyyyww", "pct_global"])

    # Join nombre proveedor
    if df_dim_prv is not None and not df_dim_prv.empty:
        dfu2 = dfu2.merge(df_dim_prv, on="c_proveedor", how="left")
    else:
        dfu2["n_proveedor"] = None

    dfu2["proveedor_label"] = dfu2.apply(
        lambda r: _label_proveedor_ui(r.get("c_proveedor"), r.get("n_proveedor")),
        axis=1
    )

    # Filtro por mínimo OC SGM en todo el rango (por proveedor)
    if min_oc_sgm_rango and min_oc_sgm_rango > 0:
        tot = dfu2.groupby("proveedor_label", as_index=False).agg(
            total_oc_sgm=("total_oc_sgm", "sum")
        )
        keep = tot[tot["total_oc_sgm"] >= float(min_oc_sgm_rango)]["proveedor_label"]
        dfu2 = dfu2[dfu2["proveedor_label"].isin(keep)].copy()

    if dfu2.empty:
        return pd.DataFrame(columns=["proveedor_label"]), pd.DataFrame(columns=["semana_yyyyww", "pct_global"])

    # Recalcular indicador (por si se filtró)
    dfu2["pct_connexa_sem"] = dfu2.apply(
        lambda r: (r["total_oc_cnx"] / r["total_oc_sgm"]) if r["total_oc_sgm"] else 0.0,
        axis=1
    )

    # Orden semanas por lunes ISO
    week_order = (
        dfu2[["semana_yyyyww", "semana_inicio"]]
        .drop_duplicates()
        .sort_values("semana_inicio")["semana_yyyyww"]
        .tolist()
    )

    # Pivot por proveedor
    df_pivot = pd.pivot_table(
        dfu2,
        index="proveedor_label",
        columns="semana_yyyyww",
        values="pct_connexa_sem",
        aggfunc="mean",
        fill_value=0.0
    ).reset_index()

    cols = ["proveedor_label"] + [w for w in week_order if w in df_pivot.columns]
    df_pivot = df_pivot[cols]

    # Global semanal ponderado (sum oc_cnx / sum oc_sgm)
    g = dfu2.groupby(["semana_yyyyww"], as_index=False).agg(
        total_oc_cnx=("total_oc_cnx", "sum"),
        total_oc_sgm=("total_oc_sgm", "sum"),
        semana_inicio=("semana_inicio", "min"),
    )
    g = g.sort_values("semana_inicio")
    g["pct_global"] = g.apply(
        lambda r: (r["total_oc_cnx"] / r["total_oc_sgm"]) if r["total_oc_sgm"] else 0.0,
        axis=1
    )

    return df_pivot, g[["semana_yyyyww", "pct_global", "total_oc_cnx", "total_oc_sgm"]]


def _render_double_header_matrix(
    df_pivot: pd.DataFrame,
    index_col: str,
    header_label: str,
    file_name: str,
    caption: str,
):
    """
    Renderiza matriz (pivot) con doble encabezado:
      - Nivel 0: "Semana 1..N"
      - Nivel 1: "YYYY-WW"
    y export CSV con columnas aplanadas.
    """
    if df_pivot.empty or df_pivot.shape[1] <= 1:
        st.info("No hay datos suficientes para construir la matriz semanal.")
        return

    week_cols = [c for c in df_pivot.columns if c != index_col]

    # MultiIndex: nivel 0 = "Semana 1..N", nivel 1 = "YYYY-WW"
    multi_cols = [(header_label, "")]
    for i, w in enumerate(week_cols, start=1):
        multi_cols.append((f"Semana {i}", w))

    df_multi = df_pivot[[index_col] + week_cols].copy()
    df_multi.columns = pd.MultiIndex.from_tuples(multi_cols)

    # Asegurar numérico en semanas
    for col in df_multi.columns:
        if col[0] != header_label:
            df_multi[col] = pd.to_numeric(df_multi[col], errors="coerce").fillna(0.0)

    # Mostrar con formato % (preferencia Styler)
    try:
        styled = df_multi.style.format({col: "{:.0%}" for col in df_multi.columns if col[0] != header_label})
        st.dataframe(styled, use_container_width=True, hide_index=True)
    except Exception:
        df_txt = df_multi.copy()
        for col in df_txt.columns:
            if col[0] != header_label:
                df_txt[col] = df_txt[col].apply(lambda x: f"{float(x):.0%}")
        st.dataframe(df_txt, use_container_width=True, hide_index=True)

    # Export CSV aplanado
    flat_cols = [header_label] + [f"Semana {i} ({w})" for i, w in enumerate(week_cols, start=1)]
    df_export = df_pivot[[index_col] + week_cols].copy()
    df_export.columns = flat_cols

    st.download_button(
        "Descargar CSV (matriz adopción semanal — doble encabezado aplanado)",
        data=df_export.to_csv(index=False).encode("utf-8"),
        file_name=file_name,
        mime="text/csv",
    )

    st.caption(caption)


# =======================================================
# 4. Carga de datos
# =======================================================
df_oc, df_fp, df_emb, df_prop = load_uso_general(desde, hasta)
df_tiempos = load_tiempos_mensuales(desde, hasta)

df_dim = load_dim_compradores()
df_uso_sem = load_uso_semanal_comprador(desde, hasta)
df_pivot_sem, df_global_week = build_weekly_matrix_comprador(df_uso_sem, df_dim)

# Proveedores
df_dim_prv = load_dim_proveedores()
df_uso_sem_prv = load_uso_semanal_proveedor(desde, hasta)
# Nota: la matriz por proveedor se construye más abajo, porque incluye filtros UI.


# =======================================================
# 5. Evolución mensual de uso (visión integrada)
# =======================================================
st.markdown("## 1. Evolución mensual de los indicadores de utilización")

# Normalizaciones básicas
if not df_fp.empty and "mes" in df_fp.columns:
    df_fp["mes"] = pd.to_datetime(df_fp["mes"])

if not df_emb.empty and "mes" in df_emb.columns:
    df_emb["mes"] = pd.to_datetime(df_emb["mes"])

if not df_prop.empty and "mes" in df_prop.columns:
    df_prop["mes"] = pd.to_datetime(df_prop["mes"])

# Unir métricas clave por mes en un solo DataFrame
df_resumen = None
if not df_fp.empty:
    df_resumen = df_fp[["mes", "ejecuciones", "propuestas", "conversion"]].copy()

if df_emb is not None and not df_emb.empty:
    cols_emb = ["mes", "pedidos_connexa", "oc_sgm"]
    df_e = df_emb[cols_emb].copy()
    df_resumen = df_e if df_resumen is None else pd.merge(df_resumen, df_e, on="mes", how="outer")

if df_prop is not None and not df_prop.empty and "proporcion_ci" in df_prop.columns:
    df_p = df_prop[["mes", "proporcion_ci"]].copy()
    df_resumen = df_p if df_resumen is None else pd.merge(df_resumen, df_p, on="mes", how="outer")

if df_resumen is None or df_resumen.empty:
    st.info("No se encontraron datos suficientes para mostrar la evolución mensual de uso.")
else:
    df_resumen = df_resumen.sort_values("mes")

    if "conversion" in df_resumen.columns:
        df_resumen["conversion_pct"] = pd.to_numeric(df_resumen["conversion"], errors="coerce").fillna(0.0) * 100.0
    if "proporcion_ci" in df_resumen.columns:
        df_resumen["proporcion_ci_pct"] = pd.to_numeric(df_resumen["proporcion_ci"], errors="coerce").fillna(0.0) * 100.0

    total_ejec = int(pd.to_numeric(df_resumen.get("ejecuciones", 0), errors="coerce").fillna(0).sum())  # type: ignore
    total_prop = int(pd.to_numeric(df_resumen.get("propuestas", 0), errors="coerce").fillna(0).sum())    # type: ignore
    total_ped = int(pd.to_numeric(df_resumen.get("pedidos_connexa", 0), errors="coerce").fillna(0).sum())  # type: ignore
    total_oc = int(pd.to_numeric(df_resumen.get("oc_sgm", 0), errors="coerce").fillna(0).sum())            # type: ignore

    conv_global = (total_prop / total_ejec) if total_ejec > 0 else 0.0
    tasa_embudo = (total_oc / total_ped * 100.0) if total_ped > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Ejecuciones de forecast (rango)", value=total_ejec)
    with col2:
        st.metric("Propuestas generadas (rango)", value=total_prop)
    with col3:
        st.metric("Pedidos Connexa (NIPs)", value=total_ped)
    with col4:
        st.metric("OC SGM generadas", value=total_oc)

    col5, col6 = st.columns(2)
    with col5:
        st.metric("Tasa global Forecast → Propuesta", value=f"{conv_global * 100:,.1f} %")
    with col6:
        st.metric("Tasa global Pedidos Connexa → OC SGM", value=f"{tasa_embudo:,.1f} %")

    st.markdown("### 1.1 Volúmenes mensuales (Forecast, Propuestas, Pedidos, OC)")
    df_vol = df_resumen.copy()
    for c in ("ejecuciones", "propuestas", "pedidos_connexa", "oc_sgm"):
        if c in df_vol.columns:
            df_vol[c] = pd.to_numeric(df_vol[c], errors="coerce").fillna(0)

    series_vol = [c for c in ["ejecuciones", "propuestas", "pedidos_connexa", "oc_sgm"] if c in df_vol.columns]
    if series_vol:
        fig_vol = px.bar(
            df_vol,
            x="mes",
            y=series_vol,
            barmode="group",
            title="Evolución mensual de volúmenes (Forecast, Propuestas, Pedidos Connexa, OC SGM)",
        )
        fig_vol.update_layout(xaxis_title="Mes", yaxis_title="Cantidad", legend_title="Métrica")
        st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("### 1.2 Tasas de efectividad mensuales")
    df_eff = df_resumen[["mes"]].copy()
    if "conversion_pct" in df_resumen.columns:
        df_eff["Forecast→Propuesta (%)"] = df_resumen["conversion_pct"]
    if "proporcion_ci_pct" in df_resumen.columns:
        df_eff["OC SGM originadas en Connexa (%)"] = df_resumen["proporcion_ci_pct"]

    if len(df_eff.columns) > 1:
        df_eff_melt = df_eff.melt(id_vars="mes", var_name="indicador", value_name="valor")
        fig_eff = px.line(
            df_eff_melt,
            x="mes",
            y="valor",
            color="indicador",
            markers=True,
            title="Evolución mensual de tasas de efectividad",
        )
        fig_eff.update_layout(xaxis_title="Mes", yaxis_title="Porcentaje (%)")
        st.plotly_chart(fig_eff, use_container_width=True)

    with st.expander("Detalle de evolución mensual (tabla resumen)"):
        st.dataframe(df_resumen, use_container_width=True)


# =======================================================
# 6. Volumen total gestionado
# =======================================================
st.markdown("---")
st.markdown("## 2. Volumen total gestionado (bultos)")

if df_emb.empty:
    st.info("No se encontraron datos de embudo Connexa → SGM para el rango seleccionado.")
else:
    df_emb_plot = df_emb.copy()
    if "mes" in df_emb_plot.columns:
        df_emb_plot["mes"] = pd.to_datetime(df_emb_plot["mes"])

    for c in ("bultos_connexa", "bultos_sgm"):
        if c in df_emb_plot.columns:
            df_emb_plot[c] = pd.to_numeric(df_emb_plot[c], errors="coerce").fillna(0.0)

    total_bultos_connexa = float(df_emb_plot.get("bultos_connexa", 0).sum())  # type: ignore
    total_bultos_sgm = float(df_emb_plot.get("bultos_sgm", 0).sum())          # type: ignore

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Bultos gestionados en Connexa (rango)", value=f"{total_bultos_connexa:,.0f}")
    with col2:
        st.metric("Bultos embarcados en OC SGM desde Connexa (rango)", value=f"{total_bultos_sgm:,.0f}")

    if "bultos_connexa" in df_emb_plot.columns and "bultos_sgm" in df_emb_plot.columns:
        df_stack = df_emb_plot[["mes", "bultos_connexa", "bultos_sgm"]].melt(
            id_vars="mes", var_name="origen", value_name="bultos"
        )
        fig_bultos = px.bar(
            df_stack,
            x="mes",
            y="bultos",
            color="origen",
            title="Bultos gestionados mensuales (Connexa vs OC SGM)",
        )
        fig_bultos.update_layout(xaxis_title="Mes", yaxis_title="Bultos")
        st.plotly_chart(fig_bultos, use_container_width=True)

    with st.expander("Detalle mensual de bultos (tabla)"):
        st.dataframe(df_emb_plot, use_container_width=True)


# =======================================================
# 7. Tiempo medio utilizado (uso del sistema)
# =======================================================
st.markdown("---")
st.markdown("## 3. Tiempo medio utilizado (Forecast → Propuesta)")

if df_tiempos.empty:
    st.info("No se encontraron datos de tiempos de ejecución/ajuste para el rango seleccionado.")
else:
    kpi_p50 = float(df_tiempos["p50_adjust_min"].median()) if "p50_adjust_min" in df_tiempos.columns else 0.0
    kpi_p90 = float(df_tiempos["p90_adjust_min"].median()) if "p90_adjust_min" in df_tiempos.columns else 0.0
    kpi_avg_exec = float(df_tiempos["avg_exec_min"].mean()) if "avg_exec_min" in df_tiempos.columns else 0.0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Tiempo medio de ejecución forecast (promedio, min)", value=f"{kpi_avg_exec:,.1f}")
    with col2:
        st.metric("P50 tiempo de ajuste propuestas (min)", value=f"{kpi_p50:,.1f}")
    with col3:
        st.metric("P90 tiempo de ajuste propuestas (min)", value=f"{kpi_p90:,.1f}")

    if "avg_adjust_min" in df_tiempos.columns:
        fig_t = px.line(
            df_tiempos,
            x="mes",
            y="avg_adjust_min",
            markers=True,
            title="Tiempo medio de ajuste de propuestas (minutos, mensual)",
        )
        fig_t.update_layout(xaxis_title="Mes", yaxis_title="Minutos")
        st.plotly_chart(fig_t, use_container_width=True)

    if "p50_adjust_min" in df_tiempos.columns and "p90_adjust_min" in df_tiempos.columns:
        df_tp = df_tiempos[["mes", "p50_adjust_min", "p90_adjust_min"]].melt(
            id_vars="mes", var_name="percentil", value_name="minutos"
        )
        fig_tp = px.line(
            df_tp,
            x="mes",
            y="minutos",
            color="percentil",
            markers=True,
            title="Percentiles de tiempo de ajuste (P50 / P90)",
        )
        fig_tp.update_layout(xaxis_title="Mes", yaxis_title="Minutos")
        st.plotly_chart(fig_tp, use_container_width=True)

    with st.expander("Detalle mensual de tiempos (tabla)"):
        st.dataframe(df_tiempos, use_container_width=True)


# =======================================================
# 8. Efectividad (síntesis)
# =======================================================
st.markdown("---")
st.markdown("## 4. Efectividad del uso de la herramienta")

if df_resumen is None or df_resumen.empty:
    st.info("No se pueden calcular indicadores de efectividad sin series mensuales.")
else:
    conv_promedio = float(df_resumen["conversion_pct"].mean()) if "conversion_pct" in df_resumen.columns else 0.0
    prop_ci_prom = float(df_resumen["proporcion_ci_pct"].mean()) if "proporcion_ci_pct" in df_resumen.columns else 0.0

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Efectividad promedio Forecast → Propuesta", value=f"{conv_promedio:,.1f} %")
    with col2:
        st.metric("Efectividad promedio OC SGM originadas en Connexa", value=f"{prop_ci_prom:,.1f} %")

    st.caption(
        "Estos promedios se calculan como la media simple de las tasas mensuales dentro del rango seleccionado."
    )


# =======================================================
# 9. Matrices de adopción semanal (global + por comprador + por proveedor)
# =======================================================
st.markdown("---")
st.markdown("## 5. Matrices de adopción semanal — % Connexa (OC CNX / OC SGM)")

# -----------------------------
# 5.A Compradores
# -----------------------------
st.markdown("### 5.A Adopción semanal por comprador")

if df_pivot_sem.empty or df_pivot_sem.shape[1] <= 1:
    st.info("No se encontraron datos de adopción semanal en V_USO_SEMANAL_COMPRADOR para el rango seleccionado.")
else:
    st.markdown("#### 5.A.1 Adopción semanal global (ponderada por OC)")
    if df_global_week is not None and not df_global_week.empty:
        df_g = df_global_week.copy()
        df_g["pct_global"] = pd.to_numeric(df_g["pct_global"], errors="coerce").fillna(0.0)

        fig_g = px.line(
            df_g,
            x="semana_yyyyww",
            y="pct_global",
            markers=True,
            title="% Connexa semanal global (compradores) — Total_OC_CNX / Total_OC_SGM",
        )
        fig_g.update_layout(xaxis_title="Semana (YYYY-WW)", yaxis_title="% Connexa")
        st.plotly_chart(fig_g, use_container_width=True)

        st.dataframe(
            df_g.assign(pct_global_fmt=df_g["pct_global"].map(lambda x: f"{x:.1%}"))[
                ["semana_yyyyww", "pct_global_fmt", "total_oc_cnx", "total_oc_sgm"]
            ].rename(columns={"pct_global_fmt": "% Connexa"}),
            use_container_width=True,
            hide_index=True
        )

    st.divider()

    st.markdown("#### 5.A.2 Matriz por comprador — % Uso (exportable)")
    _render_double_header_matrix(
        df_pivot=df_pivot_sem,
        index_col="comprador_label",
        header_label="COMPRADOR",
        file_name="indicador_01_matriz_adopcion_semanal_comprador.csv",
        caption=(
            "Definición: % Connexa semanal = Total_OC_CNX / Total_OC_SGM, según [data-sync].[dbo].[V_USO_SEMANAL_COMPRADOR]. "
            "En la descarga se aplanan columnas para facilitar Excel."
        ),
    )

# -----------------------------
# 5.B Proveedores
# -----------------------------
st.divider()
st.markdown("### 5.B Adopción semanal por proveedor")

if df_uso_sem_prv is None or df_uso_sem_prv.empty:
    st.info("No se encontraron datos de adopción semanal en V_USO_SEMANAL_PROVEEDOR para el rango seleccionado.")
else:
    with st.expander("Filtros de estabilidad (proveedores)", expanded=True):
        colf1, colf2, colf3 = st.columns([1, 1, 2])
        with colf1:
            min_oc_sgm_semana = st.number_input(
                "Mínimo OC SGM por semana",
                min_value=0,
                value=5,
                step=1,
                help="Elimina filas proveedor×semana con Total_OC_SGM menor al umbral (reduce ruido por baja actividad).",
            )
        with colf2:
            min_oc_sgm_rango = st.number_input(
                "Mínimo OC SGM en el rango",
                min_value=0,
                value=20,
                step=5,
                help="Elimina proveedores con poca actividad total en el período (sumatoria de Total_OC_SGM).",
            )
        with colf3:
            top_matriz = st.slider(
                "Cantidad máxima de proveedores a mostrar (Top por OC SGM en el rango)",
                min_value=20,
                max_value=300,
                value=80,
                step=10,
            )

    # Construcción con filtros
    df_pivot_sem_prv, df_global_week_prv = build_weekly_matrix_proveedor(
        dfu=df_uso_sem_prv,
        df_dim_prv=df_dim_prv,
        min_oc_sgm_semana=int(min_oc_sgm_semana),
        min_oc_sgm_rango=int(min_oc_sgm_rango),
    )

    if df_pivot_sem_prv.empty or df_pivot_sem_prv.shape[1] <= 1:
        st.info("Con los filtros actuales no quedan datos suficientes para construir la matriz por proveedor.")
    else:
        # Top proveedores por OC SGM en el rango (post-filtro)
        df_rank = df_uso_sem_prv.copy()
        if int(min_oc_sgm_semana) > 0:
            df_rank = df_rank[df_rank["total_oc_sgm"] >= float(int(min_oc_sgm_semana))].copy()

        if df_dim_prv is not None and not df_dim_prv.empty:
            df_rank = df_rank.merge(df_dim_prv, on="c_proveedor", how="left")
        else:
            df_rank["n_proveedor"] = None

        df_rank["proveedor_label"] = df_rank.apply(
            lambda r: _label_proveedor_ui(r.get("c_proveedor"), r.get("n_proveedor")),
            axis=1
        )

        totp = df_rank.groupby("proveedor_label", as_index=False).agg(
            total_oc_sgm=("total_oc_sgm", "sum"),
            total_oc_cnx=("total_oc_cnx", "sum"),
        )
        totp["pct_connexa_rango"] = totp.apply(
            lambda r: (r["total_oc_cnx"] / r["total_oc_sgm"]) if r["total_oc_sgm"] else 0.0,
            axis=1
        )
        totp = totp.sort_values(["total_oc_sgm"], ascending=False).head(int(top_matriz))

        # Filtrar pivot al top seleccionado
        df_pivot_sem_prv_top = df_pivot_sem_prv[df_pivot_sem_prv["proveedor_label"].isin(totp["proveedor_label"])].copy()

        st.markdown("#### 5.B.1 Adopción semanal global (ponderada por OC)")
        if df_global_week_prv is not None and not df_global_week_prv.empty:
            df_g2 = df_global_week_prv.copy()
            df_g2["pct_global"] = pd.to_numeric(df_g2["pct_global"], errors="coerce").fillna(0.0)

            fig_g2 = px.line(
                df_g2,
                x="semana_yyyyww",
                y="pct_global",
                markers=True,
                title="% Connexa semanal global (proveedores) — Total_OC_CNX / Total_OC_SGM",
            )
            fig_g2.update_layout(xaxis_title="Semana (YYYY-WW)", yaxis_title="% Connexa")
            st.plotly_chart(fig_g2, use_container_width=True)

            st.dataframe(
                df_g2.assign(pct_global_fmt=df_g2["pct_global"].map(lambda x: f"{x:.1%}"))[
                    ["semana_yyyyww", "pct_global_fmt", "total_oc_cnx", "total_oc_sgm"]
                ].rename(columns={"pct_global_fmt": "% Connexa"}),
                use_container_width=True,
                hide_index=True
            )

        st.divider()

        st.markdown("#### 5.B.2 Ranking de proveedores (en el rango, post-filtro)")
        st.dataframe(
            totp.assign(pct_connexa_rango_fmt=totp["pct_connexa_rango"].map(lambda x: f"{x:.1%}"))[
                ["proveedor_label", "total_oc_sgm", "total_oc_cnx", "pct_connexa_rango_fmt"]
            ].rename(columns={
                "proveedor_label": "Proveedor",
                "total_oc_sgm": "OC SGM (rango)",
                "total_oc_cnx": "OC CNX (rango)",
                "pct_connexa_rango_fmt": "% Connexa (rango)"
            }),
            use_container_width=True,
            hide_index=True
        )

        st.divider()

        st.markdown("#### 5.B.3 Matriz por proveedor —  % Uso (exportable)")
        _render_double_header_matrix(
            df_pivot=df_pivot_sem_prv_top,
            index_col="proveedor_label",
            header_label="PROVEEDOR",
            file_name="indicador_01_matriz_adopcion_semanal_proveedor.csv",
            caption=(
                "Definición: % Connexa semanal = Total_OC_CNX / Total_OC_SGM, según [data-sync].[dbo].[V_USO_SEMANAL_PROVEEDOR]. "
                "Filtros aplicados para estabilizar el indicador (mínimos por semana y por rango)."
            ),
        )

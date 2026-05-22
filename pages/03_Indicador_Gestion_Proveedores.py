# pages/03_Indicador_Proveedores.py
# Indicador 3 — Incorporación de Proveedores
# Pregunta clave:
#   ¿Cuántos proveedores están siendo gestionados a través de Connexa
#   y qué proporción representan sobre el total de proveedores con OC en SGM?

import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from modules.ui import render_header, make_date_filters
from modules.db import get_pg_engine, get_sqlserver_engine

# Funciones de queries específicas de proveedores.
# Ajustar los nombres si en providers.py quedaron levemente distintos.
from modules.queries.proveedores import (
    get_resumen_proveedores_connexa,        # PG: SQL_PG_CONNEXA_PROV (por proveedor)
    get_resumen_proveedores_sgm_desde_ci,   # SQL Server: SQL_SGM_CONNEXA_PROV (por proveedor)
    get_proporcion_proveedores_ci_mensual,  # SQL_SGM_I4_PROV_MENSUAL (mensual)
    get_detalle_proveedores_ci,             # SQL_SGM_I4_PROV_DETALLE (detalle)
    get_proveedores_ci_sin_cabecera,        # SQL_SGM_I4_PROV_SIN_CABE (diagnóstico)
    get_ranking_proveedores_pg,                # SQL_PG_CONNEXA_PROV_RANKING (ranking Connexa)
    get_ranking_proveedores_resumen,          # SQL_SGM_I4_PROV_RANKING (ranking CI → SGM)
    # get_ventas_proveedor  # si luego quieren usarlo para drill-down
)

# Motores de base de datos
pg_diarco = get_pg_engine()
eng_sgm   = get_sqlserver_engine()

# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 3 — Incorporación de Proveedores",
    page_icon="🏭",
    layout="wide",
)

render_header("Indicador 3 — Incorporación de Proveedores")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# =======================================================
# 1. Helpers locales
# =======================================================

def _to_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype="float")
    return pd.to_numeric(df[col], errors="coerce").fillna(0)


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(_to_numeric(df, col).sum())


def _normalize_proveedor(df: pd.DataFrame, col: str = "c_proveedor") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


SQL_SGM_PROVEEDOR_COBERTURA = text("""
WITH cabe AS (
  SELECT
    TRY_CONVERT(date, C.F_ALTA_SIST) AS f_alta_date,
    C.C_PROVEEDOR AS c_proveedor,
    P.N_PROVEEDOR AS n_proveedor,
    CAST(C.U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(C.U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc,
    CONCAT(CAST(C.U_PREFIJO_OC AS varchar(32)), '-', CAST(C.U_SUFIJO_OC AS varchar(32))) AS oc_sgm
  FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_CABE] C
  LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T020_PROVEEDOR] P
    ON C.C_PROVEEDOR = P.C_PROVEEDOR
  WHERE TRY_CONVERT(date, C.F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, C.F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(C.U_PREFIJO_OC, 0) <> 0
    AND ISNULL(C.U_SUFIJO_OC, 0) <> 0
),
t874 AS (
  SELECT DISTINCT
    CAST(U_PREFIJO_OC AS varchar(32)) AS u_prefijo_oc,
    CAST(U_SUFIJO_OC  AS varchar(32)) AS u_sufijo_oc
  FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
  WHERE TRY_CONVERT(date, F_ALTA_SIST) >= :desde
    AND TRY_CONVERT(date, F_ALTA_SIST) <  DATEADD(day, 1, :hasta)
    AND ISNULL(U_PREFIJO_OC, 0) <> 0
    AND ISNULL(U_SUFIJO_OC, 0) <> 0
),
marca AS (
  SELECT
    c.*,
    CASE WHEN t.u_prefijo_oc IS NULL THEN 0 ELSE 1 END AS es_connexa
  FROM cabe c
  LEFT JOIN t874 t
    ON t.u_prefijo_oc = c.u_prefijo_oc
   AND t.u_sufijo_oc = c.u_sufijo_oc
)
SELECT
  c_proveedor,
  COALESCE(NULLIF(LTRIM(RTRIM(MAX(n_proveedor))), ''), CAST(c_proveedor AS varchar(32))) AS n_proveedor,
  COUNT(DISTINCT oc_sgm) AS oc_sgm_total,
  COUNT(DISTINCT CASE WHEN es_connexa = 1 THEN oc_sgm END) AS oc_sgm_desde_connexa,
  COUNT(DISTINCT CASE WHEN es_connexa = 0 THEN oc_sgm END) AS oc_sgm_directas,
  CAST(
    1.0 * COUNT(DISTINCT CASE WHEN es_connexa = 1 THEN oc_sgm END)
    / NULLIF(COUNT(DISTINCT oc_sgm), 0)
    AS decimal(9,6)
  ) AS pct_cobertura_connexa
FROM marca
GROUP BY c_proveedor
ORDER BY oc_sgm_total DESC, oc_sgm_desde_connexa DESC;
""")


def get_ranking_proveedores_cobertura_sgm(sqlserver_engine, desde: date, hasta: date, topn: int = 30) -> pd.DataFrame:
    if sqlserver_engine is None:
        return pd.DataFrame()

    with sqlserver_engine.connect() as con:
        df = pd.read_sql(
            SQL_SGM_PROVEEDOR_COBERTURA,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    if "c_proveedor" in df.columns:
        df["c_proveedor"] = pd.to_numeric(df["c_proveedor"], errors="coerce").astype("Int64")

    for col in ("oc_sgm_total", "oc_sgm_desde_connexa", "oc_sgm_directas"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    if "pct_cobertura_connexa" in df.columns:
        df["pct_cobertura_connexa"] = pd.to_numeric(
            df["pct_cobertura_connexa"], errors="coerce"
        ).fillna(0.0)

    df["proveedor_label"] = df.apply(
        lambda r: f"{str(r.get('n_proveedor', '')).strip()} ({int(r['c_proveedor'])})"
        if pd.notna(r.get("c_proveedor")) and str(r.get("n_proveedor", "")).strip()
        else str(r.get("c_proveedor", "")),
        axis=1,
    )

    return df.sort_values(
        ["oc_sgm_total", "oc_sgm_desde_connexa"],
        ascending=[False, False],
    ).head(topn)


@st.cache_data(ttl=ttl)
def load_proveedores(desde: date, hasta: date):
    """
    Carga los conjuntos de datos necesarios para el indicador de proveedores:

      - df_resumen_pg: Proveedores vistos desde Connexa (PostgreSQL, diarco_data)
      - df_resumen_sgm_ci: Proveedores en SGM asociados a Connexa (T874)
      - df_prop_mensual: Proporción mensual de proveedores CI vs totales en SGM
      - df_detalle_ci: Detalle de proveedores CI → SGM
      - df_sin_cabecera: Casos con KIKKER pero sin cabecera en T080
    """
    pg_engine = get_pg_engine()
    sql_engine = get_sqlserver_engine()

    df_resumen_pg = get_resumen_proveedores_connexa(
        pg_engine, desde=desde, hasta=hasta
    ) if pg_engine is not None else pd.DataFrame()

    df_resumen_sgm_ci = get_resumen_proveedores_sgm_desde_ci(
        sql_engine, desde=desde, hasta=hasta
    ) if sql_engine is not None else pd.DataFrame()

    df_prop_mensual = get_proporcion_proveedores_ci_mensual(
        sql_engine, desde=desde, hasta=hasta
    ) if sql_engine is not None else pd.DataFrame()

    df_detalle_ci = get_detalle_proveedores_ci(
        sql_engine, desde=desde, hasta=hasta
    ) if sql_engine is not None else pd.DataFrame()

    df_sin_cabecera = get_proveedores_ci_sin_cabecera(
        sql_engine, desde=desde, hasta=hasta
    ) if sql_engine is not None else pd.DataFrame()

    return (
        df_resumen_pg,
        df_resumen_sgm_ci,
        df_prop_mensual,
        df_detalle_ci,
        df_sin_cabecera,
    )

@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_ranking_proveedores_connexa(desde: date, hasta: date, topn: int = 10) -> pd.DataFrame:
    if pg_diarco is None:
        return pd.DataFrame()
    return get_ranking_proveedores_pg(pg_diarco, desde, hasta, topn=topn)

@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_ranking_proveedores_ci(desde: date, hasta: date, topn: int = 10) -> pd.DataFrame:
    if eng_sgm is None:
        return pd.DataFrame()
    return get_ranking_proveedores_resumen(eng_sgm, desde, hasta, topn=topn)

@st.cache_data(ttl=ttl, show_spinner=False)
def _fetch_ranking_proveedores_cobertura(desde: date, hasta: date, topn: int = 30) -> pd.DataFrame:
    if eng_sgm is None:
        return pd.DataFrame()
    return get_ranking_proveedores_cobertura_sgm(eng_sgm, desde, hasta, topn=topn)

# =======================================================
# 2. Carga y preparación de datos
# =======================================================

(
    df_resumen_pg,
    df_resumen_sgm_ci,
    df_prop_mensual,
    df_detalle_ci,
    df_sin_cabecera,
) = load_proveedores(desde, hasta)

df_resumen_pg = _normalize_proveedor(df_resumen_pg, "c_proveedor")
df_resumen_sgm_ci = _normalize_proveedor(df_resumen_sgm_ci, "c_proveedor")

# df_resumen_pg se espera con columnas:
#   c_proveedor, pedidos_connexa, bultos_connexa
if df_resumen_pg is None or df_resumen_pg.empty:
    df_resumen_pg = pd.DataFrame(
        columns=["c_proveedor", "pedidos_connexa", "bultos_connexa"]
    )

for col in ["pedidos_connexa", "bultos_connexa"]:
    if col not in df_resumen_pg.columns:
        df_resumen_pg[col] = 0

df_resumen_pg["pedidos_connexa"] = _to_numeric(df_resumen_pg, "pedidos_connexa")
df_resumen_pg["bultos_connexa"] = _to_numeric(df_resumen_pg, "bultos_connexa")

# df_resumen_sgm_ci se espera con columnas:
#   c_proveedor, oc_sgm_generadas, bultos_sgm
if df_resumen_sgm_ci is None or df_resumen_sgm_ci.empty:
    df_resumen_sgm_ci = pd.DataFrame(
        columns=["c_proveedor", "oc_sgm_generadas", "bultos_sgm"]
    )

for col in ["oc_sgm_generadas", "bultos_sgm"]:
    if col not in df_resumen_sgm_ci.columns:
        df_resumen_sgm_ci[col] = 0

df_resumen_sgm_ci["oc_sgm_generadas"] = _to_numeric(df_resumen_sgm_ci, "oc_sgm_generadas")
df_resumen_sgm_ci["bultos_sgm"] = _to_numeric(df_resumen_sgm_ci, "bultos_sgm")

# Construir DF maestro por proveedor (solo Connexa + SGM asociado a CI)
df_master = pd.merge(
    df_resumen_pg,
    df_resumen_sgm_ci,
    on="c_proveedor",
    how="outer",
    suffixes=("_connexa", "_sgm"),
)

# Derivadas básicas
df_master["pedidos_connexa"] = _to_numeric(df_master, "pedidos_connexa")
df_master["oc_sgm_generadas"] = _to_numeric(df_master, "oc_sgm_generadas")
df_master["bultos_connexa"] = _to_numeric(df_master, "bultos_connexa")
df_master["bultos_sgm"] = _to_numeric(df_master, "bultos_sgm")

df_master["tiene_connexa"] = df_master["pedidos_connexa"] > 0

# =======================================================
# 3. Resumen ejecutivo
# =======================================================

st.markdown("## Resumen ejecutivo de incorporación de proveedores")

# Para el total de proveedores SGM en el rango, usamos df_prop_mensual si está disponible.
if df_prop_mensual is not None and not df_prop_mensual.empty:
    # df_prop_mensual esperado:
    # mes, prov_totales_sgm, prov_desde_ci, proporcion_ci_prov
    total_prov_sgm = int(_safe_sum(df_prop_mensual, "prov_totales_sgm"))
    total_prov_ci = int(_safe_sum(df_prop_mensual, "prov_desde_ci"))
else:
    # Fallback: aproximar por df_master (solo proveedores con algún rastro CI)
    total_prov_ci = int(df_master["c_proveedor"].nunique())
    total_prov_sgm = total_prov_ci  # No tenemos el total real sin una query SGM global
    # Esto se puede refinar cuando definan una consulta global de proveedores SGM en el rango.

total_prov_ci_flag = int(df_master[df_master["tiene_connexa"]]["c_proveedor"].nunique())

if total_prov_sgm > 0:
    prop_ci_prov = total_prov_ci / total_prov_sgm
else:
    prop_ci_prov = 0.0

# Métricas de volumen vía Connexa
total_bultos_connexa = int(_safe_sum(df_master, "bultos_connexa"))
total_bultos_sgm_ci = int(_safe_sum(df_master, "bultos_sgm"))

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Proveedores con actividad Connexa", value=total_prov_ci_flag)
with col2:
    st.metric("Total proveedores SGM (rango)*", value=total_prov_sgm)
with col3:
    st.metric(
        "% proveedores con Connexa (sobre SGM)*",
        value=f"{prop_ci_prov * 100:,.1f} %",
    )
with col4:
    st.metric(
        "Bultos gestionados vía Connexa (proveedores CI)",
        value=f"{total_bultos_connexa:,.0f}",
    )

st.caption(
    "*Cuando haya una consulta global de proveedores SGM por rango, "
    "estos valores pueden reemplazarse por el conteo exacto."
)

st.markdown("---")

# =======================================================
# 4. Tabs de análisis
# =======================================================

tab1, tab2, tab3 , tab4= st.tabs(
    ["📊 Visión general", "🏅 Ranking de proveedores", "🏅 Ranking Abastecimiento", "📋 Detalle y diagnóstico"]
)

# ---------------------------
# TAB 1: Visión general
# ---------------------------
with tab1:
    st.subheader("Proporción mensual de proveedores con Connexa")

    if df_prop_mensual is not None and not df_prop_mensual.empty:
        df_prop = df_prop_mensual.copy()
        if "mes" in df_prop.columns:
            df_prop["mes"] = pd.to_datetime(df_prop["mes"])

        df_prop["prov_totales_sgm"] = _to_numeric(df_prop, "prov_totales_sgm")
        df_prop["prov_desde_ci"] = _to_numeric(df_prop, "prov_desde_ci")

        # Gráfico 1: barras proveedores totales vs con CI
        df_prop_bar = df_prop.melt(
            id_vars=["mes"],
            value_vars=["prov_totales_sgm", "prov_desde_ci"],
            var_name="tipo",
            value_name="proveedores",
        )
        df_prop_bar["tipo"] = df_prop_bar["tipo"].map(
            {
                "prov_totales_sgm": "Proveedores totales SGM",
                "prov_desde_ci": "Proveedores con Connexa",
            }
        )

        fig_bar = px.bar(
            df_prop_bar,
            x="mes",
            y="proveedores",
            color="tipo",
            title="Proveedores totales SGM vs con Connexa (mensual)",
        )
        fig_bar.update_layout(
            xaxis_title="Mes",
            yaxis_title="Cantidad de proveedores",
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # Gráfico 2: línea de proporción
        if "proporcion_ci_prov" in df_prop.columns:
            df_prop["proporcion_pct"] = (
                pd.to_numeric(df_prop["proporcion_ci_prov"], errors="coerce")
                .fillna(0.0)
                * 100.0
            )
            fig_line = px.line(
                df_prop,
                x="mes",
                y="proporcion_pct",
                markers=True,
                title="% de proveedores SGM con Connexa (mensual)",
            )
            fig_line.update_layout(
                xaxis_title="Mes",
                yaxis_title="Proporción (%)",
            )
            st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info(
            "No se encontraron datos de proporción de proveedores CI vs SGM "
            "para el rango seleccionado."
        )

# ---------------------------
# TAB 2: Ranking de proveedores (volumen y adopción)
# ---------------------------
with tab2:
    st.subheader("Ranking de proveedores (volumen y adopción)")

    st.markdown("### Cobertura CONNeXA sobre OC SGM por proveedor")
    df_cov = _fetch_ranking_proveedores_cobertura(desde, hasta, topn=30)

    if df_cov.empty:
        st.info("Sin datos de OC SGM por proveedor para el rango seleccionado.")
    else:
        df_cov_plot = df_cov.sort_values("oc_sgm_total", ascending=False).head(20).copy()
        df_cov_plot = df_cov_plot.sort_values("oc_sgm_total", ascending=True)

        tot_oc_sgm = int(_safe_sum(df_cov, "oc_sgm_total"))
        tot_oc_connexa = int(_safe_sum(df_cov, "oc_sgm_desde_connexa"))
        pct_total = (tot_oc_connexa / tot_oc_sgm * 100.0) if tot_oc_sgm else 0.0

        c1, c2, c3 = st.columns(3)
        c1.metric("OC SGM total", f"{tot_oc_sgm:,.0f}")
        c2.metric("OC desde CONNeXA", f"{tot_oc_connexa:,.0f}")
        c3.metric("Cobertura CONNeXA", f"{pct_total:,.1f} %")

        fig_cov = px.bar(
            df_cov_plot,
            x=["oc_sgm_desde_connexa", "oc_sgm_directas"],
            y="proveedor_label",
            orientation="h",
            title="Top 20 proveedores por OC SGM: CONNeXA vs SGM directo",
            text_auto=True,
        )
        fig_cov.update_layout(
            xaxis_title="Cantidad de OC SGM",
            yaxis_title="Proveedor",
            legend_title_text="Origen",
        )
        fig_cov.for_each_trace(
            lambda t: t.update(
                name={
                    "oc_sgm_desde_connexa": "OC desde CONNeXA",
                    "oc_sgm_directas": "OC SGM directas",
                }.get(t.name, t.name)
            )
        )
        st.plotly_chart(fig_cov, use_container_width=True, key="tab2_cov_ranking")

        df_cov_table = df_cov.copy()
        df_cov_table["Cobertura CONNeXA"] = (
            pd.to_numeric(df_cov_table["pct_cobertura_connexa"], errors="coerce")
            .fillna(0.0)
            .mul(100.0)
            .map(lambda v: f"{v:,.1f} %")
        )
        df_cov_table = df_cov_table.rename(
            columns={
                "c_proveedor": "Código proveedor",
                "n_proveedor": "Proveedor",
                "oc_sgm_total": "OC SGM total",
                "oc_sgm_desde_connexa": "OC desde CONNeXA",
                "oc_sgm_directas": "OC SGM directas",
            }
        )
        st.dataframe(
            df_cov_table[
                [
                    "Código proveedor",
                    "Proveedor",
                    "OC SGM total",
                    "OC desde CONNeXA",
                    "OC SGM directas",
                    "Cobertura CONNeXA",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("---")
    st.markdown("### Top proveedores por bultos desde Connexa (PostgreSQL)")
    df_rk_pg = _fetch_ranking_proveedores_connexa(desde, hasta, topn=20)

    if df_rk_pg.empty:
        st.info("Sin datos de ranking de proveedores Connexa para el rango seleccionado.")
    else:
        df_plot = df_rk_pg.copy()
        df_plot = df_plot.sort_values("bultos_total", ascending=False)

        fig_pg = px.bar(
            df_plot,
            x="bultos_total",
            y="proveedor",
            orientation="h",
            title="Top 10 proveedores por bultos desde Connexa",
            text="bultos_total",
        )
        fig_pg.update_layout(
            xaxis_title="Bultos Connexa",
            yaxis_title="Proveedor (Connexa)",
        )
        # clave única para este gráfico
        st.plotly_chart(fig_pg, use_container_width=True, key="tab4_pg_ranking")

        with st.expander("Ver tabla completa — Ranking Connexa"):
            st.dataframe(df_plot, use_container_width=True, hide_index=True)

# ---------------------------
# TAB 3: Ranking de proveedores abastecidos desde Connexa
# ---------------------------
with tab3:
    st.subheader("Ranking de proveedores abastecidos desde Connexa")

    df_rank = df_master.copy()
    df_rank["bultos_connexa"] = _to_numeric(df_rank, "bultos_connexa")
    df_rank["pedidos_connexa"] = _to_numeric(df_rank, "pedidos_connexa")

    # Por ahora usamos solo el código de proveedor como etiqueta;
    # luego se puede enriquecer con nombre de proveedor desde src.m_10_proveedores.
    df_rank["proveedor_label"] = df_rank["c_proveedor"].astype(str)

    df_rank = df_rank.sort_values(
        ["bultos_connexa", "pedidos_connexa"],
        ascending=False,
    ).head(20)

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("#### Top proveedores por bultos desde Connexa")
        if not df_rank.empty:
            fig_bultos = px.bar(
                df_rank,
                x="bultos_connexa",
                y="proveedor_label",
                orientation="h",
                text="bultos_connexa",
                title="Top 20 proveedores por bultos gestionados vía Connexa",
            )
            fig_bultos.update_layout(
                xaxis_title="Bultos Connexa",
                yaxis_title="Proveedor",
            )
            st.plotly_chart(fig_bultos, use_container_width=True)
        else:
            st.info("No hay datos de bultos Connexa para ranking de proveedores.")

    with col_r:
        st.markdown("#### Top proveedores por cantidad de pedidos Connexa")
        if not df_rank.empty:
            fig_pedidos = px.bar(
                df_rank,
                x="pedidos_connexa",
                y="proveedor_label",
                orientation="h",
                text="pedidos_connexa",
                title="Top 20 proveedores por pedidos Connexa",
            )
            fig_pedidos.update_layout(
                xaxis_title="Pedidos Connexa",
                yaxis_title="Proveedor",
            )
            st.plotly_chart(fig_pedidos, use_container_width=True)
        else:
            st.info("No hay datos de pedidos Connexa para ranking de proveedores.")

# ---------------------------
# TAB 4: Detalle y diagnóstico
# ---------------------------
with tab4:
    st.subheader("Detalle de proveedores gestionados vía Connexa → SGM")

    st.markdown("### Detalle de proveedores con Connexa y OC en SGM")

    if df_detalle_ci is not None and not df_detalle_ci.empty:
        # df_detalle_ci esperado desde SQL_SGM_I4_PROV_DETALLE
        st.dataframe(
            df_detalle_ci.sort_values("f_alta_sgm", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No hay detalle de proveedores CI → SGM en el rango seleccionado.")

    st.markdown("---")
    st.markdown("### Proveedores con movimientos CI pero sin cabecera T080 (diagnóstico)")

    if df_sin_cabecera is not None and not df_sin_cabecera.empty:
        st.dataframe(
            df_sin_cabecera.sort_values("f_alta_date", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(
            "Estos casos indican que existen registros en T874 (Connexa) con prefijo/sufijo "
            "no-cero que no encuentran su cabecera en T080; es un insumo para análisis de "
            "posibles inconsistencias de interface."
        )
    else:
        st.info(
            "No se detectaron casos de proveedores con registros CI sin cabecera T080 "
            "en el rango seleccionado."
        )


# 04_Indicador_4_Proveedores.py
# Indicador 4 — Proveedores CONNEXA → SGM
# - Resumen mensual de proveedores en SGM vs con OC originadas en CONNEXA
# - Ranking de proveedores que usan CONNEXA (OC y bultos)
# - Proveedores con pendientes (T874 sin cabecera T080)
# - Conversión CONNEXA → SGM por proveedor (pedidos vs OC)

import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_sqlserver_engine, get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_SGM_I4_PROV_MENSUAL,
    SQL_SGM_I4_PROV_DETALLE,
    SQL_SGM_I4_PROV_SIN_CABE,
    SQL_PG_CONNEXA_PROV,
    SQL_SGM_CONNEXA_PROV,
)

# -------------------------------------------------------
# Configuración general
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 4 — Proveedores CONNEXA → SGM",
    page_icon="🏪",
    layout="wide",
)

render_header("Indicador 4 — Proveedores CONNEXA y OC en SGM")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# -------------------------------------------------------
# Acceso a datos
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_prov_mensual(desde: date, hasta: date) -> pd.DataFrame:
    """
    Proveedores en SGM vs proveedores con OC originadas en CONNEXA, por mes.
    Basado en SQL_SGM_I4_PROV_MENSUAL.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_I4_PROV_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    if df.empty:
        return df

    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"])

    for col in ("prov_totales_sgm", "prov_desde_ci"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")

    if "proporcion_ci_prov" in df.columns:
        df["proporcion_ci_prov"] = pd.to_numeric(
            df["proporcion_ci_prov"], errors="coerce"
        ).fillna(0.0)

    return df


@st.cache_data(ttl=ttl)
def fetch_prov_detalle(desde: date, hasta: date) -> pd.DataFrame:
    """
    Detalle de OC SGM originadas en CONNEXA por proveedor (para ranking y drill-down).
    Espera columnas típicas:
      - f_alta_sgm
      - c_proveedor
      - oc_sgm
      - q_bultos_ci
    Basado en SQL_SGM_I4_PROV_DETALLE.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_I4_PROV_DETALLE,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    if df.empty:
        return df

    if "f_alta_sgm" in df.columns:
        df["f_alta_sgm"] = pd.to_datetime(df["f_alta_sgm"])

    if "c_proveedor" in df.columns:
        df["c_proveedor"] = pd.to_numeric(
            df["c_proveedor"], errors="coerce"
        ).astype("Int64")

    if "q_bultos_ci" in df.columns:
        df["q_bultos_ci"] = pd.to_numeric(
            df["q_bultos_ci"], errors="coerce"
        ).fillna(0.0)

    return df


@st.cache_data(ttl=ttl)
def fetch_sin_cabe(desde: date, hasta: date) -> pd.DataFrame:
    """
    Proveedores presentes en T874 (CONNEXA→SGM) sin cabecera en T080 en el rango.
    Basado en SQL_SGM_I4_PROV_SIN_CABE.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_I4_PROV_SIN_CABE,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    if df.empty:
        return df

    if "f_alta_date" in df.columns:
        df["f_alta_date"] = pd.to_datetime(df["f_alta_date"])

    if "c_proveedor" in df.columns:
        df["c_proveedor"] = pd.to_numeric(
            df["c_proveedor"], errors="coerce"
        ).astype("Int64")

    return df


@st.cache_data(ttl=ttl)
def fetch_dim_proveedores() -> pd.DataFrame:
    """
    Dimensión de proveedores desde PostgreSQL (public.m_10_proveedores).
    Se usa para enriquecer rankings con nombre, tipo, estado, país, etc.
    """
    eng = get_pg_engine()
    if eng is None:
        return pd.DataFrame()

    query = """
        SELECT 
            c_proveedor,
            TRIM(n_proveedor)              AS n_proveedor,
            c_tipo_proveedor,
            c_tipo_proveedor_diarco,
            m_activo,
            c_plazo_entrega1,
            m_baja,
            m_compra_paletizado,
            c_origen_proveedor,
            n_pais
        FROM public.m_10_proveedores;
    """
    with eng.connect() as con:
        df = pd.read_sql(query, con)

    if df.empty:
        return df

    df["c_proveedor"] = pd.to_numeric(
        df["c_proveedor"], errors="coerce"
    ).astype("Int64")

    return df


@st.cache_data(ttl=ttl)
def fetch_connexa_prov(desde: date, hasta: date) -> pd.DataFrame:
    """
    Pedidos CONNEXA por proveedor (c_compra_connexa distintos y bultos totales),
    desde PostgreSQL (t080_oc_precarga_connexa).
    Basado en SQL_PG_CONNEXA_PROV.
    """
    eng = get_pg_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_PG_CONNEXA_PROV,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    if df.empty:
        return df

    if "c_proveedor" in df.columns:
        df["c_proveedor"] = pd.to_numeric(
            df["c_proveedor"], errors="coerce"
        ).astype("Int64")

    for col in ("pedidos_connexa", "bultos_connexa"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


@st.cache_data(ttl=ttl)
def fetch_sgm_prov(desde: date, hasta: date) -> pd.DataFrame:
    """
    OC SGM por proveedor originadas en CONNEXA (intentas por proveedor y bultos),
    desde SQL Server (T874 + T080).
    Basado en SQL_SGM_CONNEXA_PROV.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_CONNEXA_PROV,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    if df.empty:
        return df

    if "c_proveedor" in df.columns:
        df["c_proveedor"] = pd.to_numeric(
            df["c_proveedor"], errors="coerce"
        ).astype("Int64")

    for col in ("oc_sgm_generadas", "bultos_sgm"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def enriquecer_con_dim(df: pd.DataFrame, dim: pd.DataFrame, how: str = "left") -> pd.DataFrame:
    """
    Join por c_proveedor con la dimensión de proveedores, si está disponible.
    """
    if df.empty or dim.empty or "c_proveedor" not in df.columns:
        return df
    out = df.merge(dim, on="c_proveedor", how=how)
    return out


# -------------------------------------------------------
# Layout principal
# -------------------------------------------------------
tab_resumen, tab_ranking, tab_pend, tab_conv = st.tabs(
    [
        "📈 Resumen mensual de proveedores",
        "🏆 Ranking de proveedores CONNEXA",
        "🧾 Proveedores con pendientes en SGM",
        "🔁 Conversión CONNEXA → SGM por proveedor",
    ]
)


# -------------------------------------------------------
# TAB 1: Resumen mensual de proveedores
# -------------------------------------------------------
with tab_resumen:
    st.subheader("Proveedores SGM vs proveedores con OC originadas en CONNEXA")

    df_m = fetch_prov_mensual(desde, hasta)

    if df_m.empty:
        st.info("No se encontraron datos de proveedores en SGM para el rango seleccionado.")
    else:
        # Promedios mensuales (no suma por proveedor para evitar doble conteo)
        prom_totales = float(df_m["prov_totales_sgm"].mean())
        prom_connexa = float(df_m["prov_desde_ci"].mean())
        prom_prop = float(df_m["proporcion_ci_prov"].mean()) if "proporcion_ci_prov" in df_m.columns else 0.0

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Proveedores con OC en SGM (prom. mensual)", value=f"{prom_totales:,.1f}")
        with col2:
            st.metric("Proveedores con OC desde CONNEXA (prom. mensual)", value=f"{prom_connexa:,.1f}")
        with col3:
            st.metric("% de proveedores con CONNEXA", value=f"{prom_prop * 100:,.1f} %")

        # Serie mensual: proveedores totales vs con CONNEXA
        df_counts = df_m[["mes", "prov_totales_sgm", "prov_desde_ci"]].copy()
        df_counts = df_counts.melt(
            id_vars="mes",
            value_vars=["prov_totales_sgm", "prov_desde_ci"],
            var_name="tipo",
            value_name="cantidad",
        )
        df_counts["tipo"] = df_counts["tipo"].map(
            {
                "prov_totales_sgm": "Proveedores con OC en SGM",
                "prov_desde_ci": "Proveedores con OC desde CONNEXA",
            }
        )

        fig1 = px.bar(
            df_counts,
            x="mes",
            y="cantidad",
            color="tipo",
            barmode="group",
            title="Proveedores con OC en SGM vs con OC originadas en CONNEXA (mensual)",
        )
        fig1.update_layout(xaxis_title="Mes", yaxis_title="Cantidad de proveedores")
        st.plotly_chart(fig1, use_container_width=True)

        # Serie mensual: proporción de proveedores con CONNEXA
        if "proporcion_ci_prov" in df_m.columns:
            df_prop = df_m.copy()
            df_prop["proporcion_pct"] = df_prop["proporcion_ci_prov"] * 100

            fig2 = px.line(
                df_prop,
                x="mes",
                y="proporcion_pct",
                markers=True,
                title="% de proveedores SGM con OC originadas en CONNEXA (mensual)",
            )
            fig2.update_layout(xaxis_title="Mes", yaxis_title="Proporción de proveedores (%)")
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander("Ver tabla mensual detallada"):
            st.dataframe(df_m, use_container_width=True, hide_index=True)
            st.download_button(
                "Descargar CSV resumen mensual de proveedores",
                data=df_m.to_csv(index=False).encode("utf-8"),
                file_name="resumen_proveedores_connexa_sgm_mensual.csv",
                mime="text/csv",
            )


# -------------------------------------------------------
# TAB 2: Ranking de proveedores CONNEXA
# -------------------------------------------------------
with tab_ranking:
    st.subheader("Ranking de proveedores por uso de CONNEXA (OC y bultos)")

    dfd = fetch_prov_detalle(desde, hasta)
    dim_prov = fetch_dim_proveedores()

    if dfd.empty:
        st.info("No hay detalle de OC originadas en CONNEXA para el rango seleccionado.")
    else:
        # Agregado por proveedor
        rk = (
            dfd.groupby("c_proveedor", dropna=False)
               .agg(
                   oc_distintas=("oc_sgm", "nunique"),
                   bultos_total=("q_bultos_ci", "sum"),
               )
               .reset_index()
        )

        rk = enriquecer_con_dim(rk, dim_prov, how="left")

        # Filtros del ranking
        col_fr1, col_fr2, col_fr3 = st.columns([1, 1, 2])
        with col_fr1:
            topn = st.slider("Top N", min_value=5, max_value=50, value=20, step=5)
        with col_fr2:
            ordenar_por = st.selectbox(
                "Ordenar por",
                options=["oc_distintas", "bultos_total"],
                index=0,
                help="Criterio principal para el ranking.",
            )
        with col_fr3:
            solo_activos = st.checkbox("Mostrar solo proveedores activos (m_activo = 'S')", value=False)

        if solo_activos and "m_activo" in rk.columns:
            rk_fil = rk[rk["m_activo"] == "S"].copy()
        else:
            rk_fil = rk.copy()

        if rk_fil.empty:
            st.warning("No hay proveedores que cumplan los filtros seleccionados.")
        else:
            rk_fil = rk_fil.sort_values(ordenar_por, ascending=False)
            rk_top = rk_fil.head(topn).copy()

            # Etiqueta para gráfico: código + nombre si existe
            if "n_proveedor" in rk_top.columns:
                rk_top["label"] = rk_top.apply(
                    lambda row: f"{row['c_proveedor']} - {row['n_proveedor']}"
                    if pd.notna(row.get("n_proveedor"))
                    else str(row["c_proveedor"]),
                    axis=1,
                )
            else:
                rk_top["label"] = rk_top["c_proveedor"].astype(str)

            fig_r = px.bar(
                rk_top.sort_values(ordenar_por),
                x=ordenar_por,
                y="label",
                orientation="h",
                title=f"Top {topn} proveedores por {ordenar_por.replace('_', ' ').title()} (OC desde CONNEXA)",
                text=ordenar_por,
            )
            fig_r.update_layout(
                yaxis_title="Proveedor",
                xaxis_title=ordenar_por.replace("_", " ").title(),
            )
            st.plotly_chart(fig_r, use_container_width=True)

            st.markdown("### Detalle de ranking")
            st.dataframe(rk_fil, use_container_width=True, hide_index=True)
            st.download_button(
                "Descargar CSV ranking de proveedores",
                data=rk_fil.to_csv(index=False).encode("utf-8"),
                file_name="ranking_proveedores_connexa.csv",
                mime="text/csv",
            )


# -------------------------------------------------------
# TAB 3: Proveedores con pendientes en SGM
# -------------------------------------------------------
with tab_pend:
    st.subheader("Proveedores CONNEXA con prefijo/sufijo sin cabecera en SGM (posibles pendientes)")

    miss = fetch_sin_cabe(desde, hasta)
    dim_prov = fetch_dim_proveedores()

    if miss.empty:
        st.success("No se detectaron proveedores CONNEXA sin cabecera en SGM en el rango.")
    else:
        miss_enr = enriquecer_con_dim(miss, dim_prov, how="left")

        # Resumen por proveedor: cantidad de registros pendientes
        if "c_proveedor" in miss_enr.columns:
            agg = (
                miss_enr.groupby("c_proveedor", dropna=False)
                        .agg(pendientes=("c_proveedor", "size"))
                        .reset_index()
            )
            agg = enriquecer_con_dim(agg, dim_prov, how="left")

            st.markdown("### Resumen de proveedores con pendientes")
            st.dataframe(agg, use_container_width=True, hide_index=True)

        st.markdown("### Detalle de pendientes")
        st.dataframe(miss_enr, width="stretch", hide_index=True)
        st.download_button(
            "Descargar CSV de proveedores con pendientes",
            data=miss_enr.to_csv(index=False).encode("utf-8"),
            file_name="proveedores_connexa_sin_cabecera.csv",
            mime="text/csv",
        )


# -------------------------------------------------------
# TAB 4: Conversión CONNEXA → SGM por proveedor
# -------------------------------------------------------
with tab_conv:
    st.subheader("Conversión CONNEXA → SGM por proveedor (pedidos vs OC)")

    df_pg = fetch_connexa_prov(desde, hasta)   # pedidos CONNEXA por proveedor
    df_sgm = fetch_sgm_prov(desde, hasta)      # OC SGM originadas en CONNEXA por proveedor
    dim = fetch_dim_proveedores()

    if df_pg.empty and df_sgm.empty:
        st.info("No se encontraron datos ni en CONNEXA ni en SGM para el rango seleccionado.")
    else:
        # Normalizar nombres de columnas
        if "bultos_connexa" in df_pg.columns:
            df_pg = df_pg.rename(columns={"bultos_connexa": "bultos_pg"})
        if "bultos_sgm" in df_sgm.columns:
            df_sgm = df_sgm.rename(columns={"bultos_sgm": "bultos_sgm_oc"})

        df_merge = df_pg.merge(df_sgm, on="c_proveedor", how="outer")

        for col in ("pedidos_connexa", "oc_sgm_generadas", "bultos_pg", "bultos_sgm_oc"):
            if col in df_merge.columns:
                df_merge[col] = pd.to_numeric(df_merge[col], errors="coerce").fillna(0)

        # Tasa de conversión por proveedor
        df_merge["tasa_conversion"] = df_merge.apply(
            lambda r: (r["oc_sgm_generadas"] / r["pedidos_connexa"] * 100)
            if r.get("pedidos_connexa", 0) > 0
            else 0.0,
            axis=1,
        )

        df_final = enriquecer_con_dim(df_merge, dim)

        # Filtros para el gráfico de ranking por conversión
        col_c1, col_c2 = st.columns([1, 1])
        with col_c1:
            topn_conv = st.slider("Top N (por tasa de conversión)", 5, 50, 20, 5)
        with col_c2:
            solo_activos_conv = st.checkbox(
                "Solo proveedores activos (m_activo = 'S')", value=False
            )

        df_plot = df_final.copy()
        if solo_activos_conv and "m_activo" in df_plot.columns:
            df_plot = df_plot[df_plot["m_activo"] == "S"]

        if df_plot.empty:
            st.warning("No hay proveedores que cumplan las condiciones seleccionadas.")
        else:
            df_plot = df_plot.sort_values("tasa_conversion", ascending=False)
            df_top = df_plot.head(topn_conv).copy()

            # Etiqueta: código + nombre si está disponible
            if "n_proveedor" in df_top.columns:
                df_top["label"] = df_top.apply(
                    lambda row: f"{row['c_proveedor']} - {row['n_proveedor']}"
                    if pd.notna(row.get("n_proveedor"))
                    else str(row["c_proveedor"]),
                    axis=1,
                )
            else:
                df_top["label"] = df_top["c_proveedor"].astype(str)

            fig_conv = px.bar(
                df_top.sort_values("tasa_conversion"),
                x="tasa_conversion",
                y="label",
                orientation="h",
                title=f"Top {topn_conv} proveedores por tasa de conversión CONNEXA → SGM",
                text="tasa_conversion",
            )
            fig_conv.update_layout(
                xaxis_title="Tasa de conversión (%)",
                yaxis_title="Proveedor",
            )
            st.plotly_chart(fig_conv, use_container_width=True)

        st.markdown("### Tabla completa de KPIs por proveedor")
        st.dataframe(df_final, use_container_width=True, hide_index=True)
        st.download_button(
            "Descargar CSV KPIs proveedores CONNEXA → SGM",
            data=df_final.to_csv(index=False).encode("utf-8"),
            file_name="kpis_proveedores_connexa_sgm.csv",
            mime="text/csv",
        )

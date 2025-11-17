import os
from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_sqlserver_engine, get_pg_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_SGM_KIKKER_VS_OC_MENSUAL,
    SQL_SGM_KIKKER_DETALLE,
    SQL_SGM_KIKKER_DUP,
    SQL_PG_KIKKER_MENSUAL,    # ahora basado en t080_oc_precarga_connexa
    SQL_C_COMPRA_KIKKER_GEN,  # ahora devuelve c_compra_connexa AS kikker
)

# -------------------------------------------------------
# Configuración general
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 2 — Embudo CONNEXA → SGM (Pedidos vs OC)",
    page_icon="🔁",
    layout="wide",
)
render_header("Indicador 2 — Embudo CONNEXA → SGM (Pedidos CONNEXA vs OC SGM)")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# -------------------------------------------------------
# Funciones de acceso a datos (cacheadas)
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_pg_connexa_mensual(desde, hasta) -> pd.DataFrame:
    """
    Pedidos CONNEXA mensuales desde PostgreSQL (t080_oc_precarga_connexa).
    Usa SQL_PG_KIKKER_MENSUAL, pero renombra las columnas a términos de negocio.
    """
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_PG_KIKKER_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    df["mes"] = pd.to_datetime(df["mes"])
    df.rename(
        columns={
            "kikker_distintos_pg": "pedidos_connexa",
            "total_bultos_pg": "bultos_connexa",
        },
        inplace=True,
    )
    df["pedidos_connexa"] = pd.to_numeric(df["pedidos_connexa"], errors="coerce").fillna(0).astype("Int64")
    df["bultos_connexa"] = pd.to_numeric(df["bultos_connexa"], errors="coerce").fillna(0.0)
    return df


@st.cache_data(ttl=ttl)
def fetch_sgm_mensual(desde, hasta) -> pd.DataFrame:
    """
    KIKKER y OC asociadas en SGM (T874 + T080), agregadas por mes.
    Aquí se mantiene el nombre técnico C_COMPRA_KIKKER porque es la columna en SGM.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_KIKKER_VS_OC_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    df["mes"] = pd.to_datetime(df["mes"])
    df.rename(
        columns={
            "kikker_distintos": "nips_sgm",
            "oc_sgm_distintas": "oc_sgm",
            "total_bultos": "bultos_sgm",
        },
        inplace=True,
    )
    df["nips_sgm"] = pd.to_numeric(df["nips_sgm"], errors="coerce").fillna(0).astype("Int64")
    df["oc_sgm"] = pd.to_numeric(df["oc_sgm"], errors="coerce").fillna(0).astype("Int64")
    df["bultos_sgm"] = pd.to_numeric(df["bultos_sgm"], errors="coerce").fillna(0.0)
    return df


@st.cache_data(ttl=ttl)
def fetch_pg_compra_connexa(desde, hasta) -> pd.DataFrame:
    """
    Lista de compras CONNEXA (c_compra_connexa) generadas en el rango.
    SQL_C_COMPRA_KIKKER_GEN retorna AS kikker, aquí se respeta para poder cruzar con SGM.
    """
    hasta_mas_1 = hasta + timedelta(days=1)
    eng = get_pg_engine()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_C_COMPRA_KIKKER_GEN,
            con,
            params={"desde": desde, "hasta_mas_1": hasta_mas_1},
        )
    if df.empty:
        return df
    df["kikker"] = df["kikker"].astype(str)
    return df[["kikker"]].drop_duplicates()


@st.cache_data(ttl=ttl)
def fetch_sgm_detalle(desde, hasta) -> pd.DataFrame:
    """
    Detalle de KIKKER y OC en SGM (T874) en el rango.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_KIKKER_DETALLE,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    if df.empty:
        return df

    if "f_alta_date" in df.columns:
        df["f_alta_date"] = pd.to_datetime(df["f_alta_date"])
    if "C_COMPRA_KIKKER" in df.columns:
        df["C_COMPRA_KIKKER"] = df["C_COMPRA_KIKKER"].astype(str)

    return df


@st.cache_data(ttl=ttl)
def fetch_sgm_dup(desde, hasta) -> pd.DataFrame:
    """
    Compras CONNEXA (C_COMPRA_KIKKER) que terminan en más de una OC distinta en SGM.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()
    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_KIKKER_DUP,
            con,
            params={"desde": desde, "hasta": hasta},
        )
    if df.empty:
        return df

    df["C_COMPRA_KIKKER"] = df["C_COMPRA_KIKKER"].astype(str)
    df["oc_sgm_unicas"] = pd.to_numeric(df["oc_sgm_unicas"], errors="coerce").fillna(0).astype("Int64")
    return df


# -------------------------------------------------------
# Tabs principales
# -------------------------------------------------------
tab_embudo, tab_recon, tab_apr = st.tabs(
    [
        "📈 Embudo CONNEXA → SGM",
        "🧮 Reconciliación por compra CONNEXA",
        "✅ Aprobación de OC (pendiente)",
    ]
)

# -------------------------------------------------------
# TAB 1: Embudo CONNEXA → SGM
# -------------------------------------------------------
with tab_embudo:
    st.subheader("Embudo CONNEXA → SGM (Pedidos CONNEXA vs OC SGM)")

    df_pg_m = fetch_pg_connexa_mensual(desde, hasta)
    df_sgm_m = fetch_sgm_mensual(desde, hasta)

    if df_pg_m.empty and df_sgm_m.empty:
        st.info("No se encontraron datos ni en CONNEXA ni en SGM para el rango seleccionado.")
    else:
        df_embudo = pd.merge(
            df_pg_m,
            df_sgm_m,
            on="mes",
            how="outer",
        ).sort_values("mes")

        for col in ["pedidos_connexa", "nips_sgm", "oc_sgm", "bultos_connexa", "bultos_sgm"]:
            if col in df_embudo.columns:
                df_embudo[col] = pd.to_numeric(df_embudo[col], errors="coerce").fillna(0)

        df_embudo["tasa_conv_pedidos_oc"] = df_embudo.apply(
            lambda row: (row["oc_sgm"] / row["pedidos_connexa"] * 100)
            if row.get("pedidos_connexa", 0) not in (0, None)
            else 0.0,
            axis=1,
        )

        total_pedidos = int(df_embudo["pedidos_connexa"].sum() if "pedidos_connexa" in df_embudo else 0)
        total_oc = int(df_embudo["oc_sgm"].sum() if "oc_sgm" in df_embudo else 0)
        total_bultos_pg = float(df_embudo["bultos_connexa"].sum() if "bultos_connexa" in df_embudo else 0.0)
        total_bultos_sgm = float(df_embudo["bultos_sgm"].sum() if "bultos_sgm" in df_embudo else 0.0)
        tasa_global = (total_oc / total_pedidos * 100) if total_pedidos > 0 else 0.0

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Pedidos CONNEXA (NIPs)", value=total_pedidos)
        with col2:
            st.metric("OC SGM generadas desde CONNEXA", value=total_oc)
        with col3:
            st.metric("Tasa global de conversión", value=f"{tasa_global:,.1f}%")

        col4, col5 = st.columns(2)
        with col4:
            st.metric("Bultos CONNEXA (rango)", value=f"{total_bultos_pg:,.0f}")
        with col5:
            st.metric("Bultos en OC SGM (rango)", value=f"{total_bultos_sgm:,.0f}")

        # Pedidos vs OC
        df_counts = df_embudo[["mes", "pedidos_connexa", "oc_sgm"]].copy()
        df_counts = df_counts.melt(
            id_vars="mes",
            value_vars=["pedidos_connexa", "oc_sgm"],
            var_name="tipo",
            value_name="cantidad",
        )
        df_counts["tipo"] = df_counts["tipo"].map(
            {
                "pedidos_connexa": "Pedidos CONNEXA",
                "oc_sgm": "OC SGM generadas",
            }
        )

        fig1 = px.bar(
            df_counts,
            x="mes",
            y="cantidad",
            color="tipo",
            barmode="group",
            title="Pedidos CONNEXA vs OC SGM (mensual)",
        )
        fig1.update_layout(xaxis_title="Mes", yaxis_title="Cantidad")
        st.plotly_chart(fig1, use_container_width=True)

        # Bultos
        if "bultos_connexa" in df_embudo and "bultos_sgm" in df_embudo:
            df_bultos = df_embudo[["mes", "bultos_connexa", "bultos_sgm"]].copy()
            df_bultos = df_bultos.melt(
                id_vars="mes",
                value_vars=["bultos_connexa", "bultos_sgm"],
                var_name="tipo",
                value_name="bultos",
            )
            df_bultos["tipo"] = df_bultos["tipo"].map(
                {
                    "bultos_connexa": "Bultos CONNEXA",
                    "bultos_sgm": "Bultos en OC SGM",
                }
            )
            fig2 = px.bar(
                df_bultos,
                x="mes",
                y="bultos",
                color="tipo",
                barmode="group",
                title="Bultos CONNEXA vs Bultos en OC SGM (mensual)",
            )
            fig2.update_layout(xaxis_title="Mes", yaxis_title="Bultos")
            st.plotly_chart(fig2, use_container_width=True)

        # Tasa de conversión
        fig3 = px.line(
            df_embudo,
            x="mes",
            y="tasa_conv_pedidos_oc",
            markers=True,
            title="Tasa de conversión Pedidos CONNEXA → OC SGM (mensual, %)",
        )
        fig3.update_layout(xaxis_title="Mes", yaxis_title="Tasa de conversión (%)")
        st.plotly_chart(fig3, use_container_width=True)

        with st.expander("Ver tabla mensual consolidada (Embudo CONNEXA → SGM)"):
            st.dataframe(df_embudo, use_container_width=True)
            st.download_button(
                "Descargar CSV embudo mensual",
                data=df_embudo.to_csv(index=False).encode("utf-8"),
                file_name="embudo_connexa_sgm_mensual.csv",
                mime="text/csv",
            )

# -------------------------------------------------------
# TAB 2: Reconciliación por compra CONNEXA
# -------------------------------------------------------
with tab_recon:
    st.subheader("Reconciliación de compras CONNEXA entre CONNEXA y SGM")

    df_pg_k = fetch_pg_compra_connexa(desde, hasta)
    df_sgm_det = fetch_sgm_detalle(desde, hasta)
    df_dup = fetch_sgm_dup(desde, hasta)

    if df_pg_k.empty and df_sgm_det.empty:
        st.info("No se encontraron compras CONNEXA ni en CONNEXA ni en SGM para el rango seleccionado.")
    else:
        if df_sgm_det.empty:
            df_sgm_k = pd.DataFrame(columns=["kikker"])
        else:
            df_sgm_k = (
                df_sgm_det[["C_COMPRA_KIKKER"]]
                .dropna()
                .drop_duplicates()
                .rename(columns={"C_COMPRA_KIKKER": "kikker"})
            )
            df_sgm_k["kikker"] = df_sgm_k["kikker"].astype(str)

        recon = df_sgm_k.merge(df_pg_k, on="kikker", how="outer", indicator=True)

        estado_map = {
            "both": "OK (1:1)",
            "left_only": "SGM sin CONNEXA",
            "right_only": "CONNEXA sin SGM",
        }
        recon["estado"] = recon["_merge"].map(estado_map)

        kpis = (
            recon["estado"]
            .value_counts()
            .rename_axis("estado")
            .reset_index(name="compras_conneXa")
        )

        colA, colB = st.columns([1, 2])
        with colA:
            st.markdown("### KPIs de reconciliación")
            st.dataframe(kpis, use_container_width=True, hide_index=True)
            if not df_dup.empty:
                st.metric("Compras CONNEXA con > 1 OC SGM", int(df_dup.shape[0]))
        with colB:
            if not recon.empty:
                fig = px.bar(
                    kpis,
                    x="estado",
                    y="compras_conneXa",
                    title="Distribución de compras CONNEXA por estado de reconciliación",
                )
                fig.update_layout(xaxis_title="", yaxis_title="Compras CONNEXA")
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Para mostrar al usuario final, renombramos 'kikker' a 'compra_connexa'
        recon_display = recon[["kikker", "estado"]].rename(columns={"kikker": "compra_connexa"})

        with st.expander("Listado de reconciliación por compra CONNEXA"):
            st.dataframe(recon_display, use_container_width=True, hide_index=True)
            st.download_button(
                "Descargar CSV de reconciliación",
                data=recon_display.to_csv(index=False).encode("utf-8"),
                file_name="reconciliacion_compras_connexa.csv",
                mime="text/csv",
            )

        with st.expander("Detalle de compras CONNEXA con más de 1 OC SGM"):
            if df_dup.empty:
                st.info("No se encontraron compras CONNEXA con más de 1 OC distinta en SGM en el rango seleccionado.")
            else:
                st.dataframe(df_dup, use_container_width=True, hide_index=True)
                st.download_button(
                    "Descargar CSV compras CONNEXA duplicadas",
                    data=df_dup.to_csv(index=False).encode("utf-8"),
                    file_name="compras_connexa_multiples_oc.csv",
                    mime="text/csv",
                )

        with st.expander("Detalle T874 KIKKER (muestra técnica)"):
            if df_sgm_det.empty:
                st.info("No hay detalle en SGM para el rango seleccionado.")
            else:
                st.dataframe(df_sgm_det.head(500), use_container_width=True, hide_index=True)
                st.caption("Se muestran hasta 500 filas como muestra técnica.")
                st.download_button(
                    "Descargar CSV detalle T874",
                    data=df_sgm_det.to_csv(index=False).encode("utf-8"),
                    file_name="detalle_t874_compras_connexa.csv",
                    mime="text/csv",
                )

# -------------------------------------------------------
# TAB 3: Aprobación de OC (pendiente)
# -------------------------------------------------------
with tab_apr:
    st.info(
        "Pendiente conectar con la tabla/campos reales de aprobación en SGM "
        "(fecha de aprobación de OC). Cuando definan la fuente se podrá "
        "agregar aquí el análisis de tasa de aprobación y tiempos de ciclo."
    )

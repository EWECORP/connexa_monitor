import os
from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_sqlserver_engine
from modules.ui import render_header, make_date_filters
from modules.queries import (
    SQL_SGM_I3_MENSUAL,
    SQL_SGM_I3_SIN_CABE,
)

# -------------------------------------------------------
# Configuración general
# -------------------------------------------------------
st.set_page_config(
    page_title="Indicador 3 — % de OC SGM originadas en CONNEXA",
    page_icon="🧮",
    layout="wide",
)

render_header("Indicador 3 — Proporción de OC SGM originadas en CONNEXA")

desde, hasta = make_date_filters()
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# -------------------------------------------------------
# Acceso a datos (SQL Server)
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_proporcion(desde: date, hasta: date) -> pd.DataFrame:
    """
    Recupera, desde SQL Server, la proporción mensual de OC SGM originadas en CONNEXA.
    Se apoya en SQL_SGM_I3_MENSUAL, que expone:
      - mes
      - oc_totales_sgm
      - oc_desde_ci        (OC SGM originadas en CONNEXA)
      - proporcion_ci      (decimal 0–1)
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()

    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_I3_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    # Normalización de tipos
    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"])
    for col in ["oc_totales_sgm", "oc_desde_ci"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("Int64")
    if "proporcion_ci" in df.columns:
        df["proporcion_ci"] = pd.to_numeric(df["proporcion_ci"], errors="coerce").fillna(0.0)

    return df


@st.cache_data(ttl=ttl)
def fetch_sin_cabe(desde: date, hasta: date) -> pd.DataFrame:
    """
    Recupera, desde SQL Server, las compras CONNEXA (T874) que tienen prefijo/sufijo
    pero no encuentran cabecera correspondiente en T080 en el rango seleccionado.
    """
    eng = get_sqlserver_engine()
    if eng is None:
        return pd.DataFrame()

    with eng.connect() as con:
        df = pd.read_sql(
            SQL_SGM_I3_SIN_CABE,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    # Normalizaciones mínimas
    if "f_alta_date" in df.columns:
        df["f_alta_date"] = pd.to_datetime(df["f_alta_date"])
    # Alias técnicos esperables: oc_sgm, C_COMPRA_KIKKER, etc.
    return df


# -------------------------------------------------------
# Layout: Tabs
# -------------------------------------------------------
tab_prop, tab_det = st.tabs(
    [
        "📈 Proporción mensual CONNEXA / Total SGM",
        "🧾 Compras CONNEXA sin cabecera en SGM",
    ]
)

# -------------------------------------------------------
# TAB 1: Proporción mensual
# -------------------------------------------------------
with tab_prop:
    st.subheader("Proporción mensual de OC SGM originadas en CONNEXA")

    df = fetch_proporcion(desde, hasta)

    if df.empty:
        st.info("No se encontraron datos de OC SGM para el rango seleccionado.")
    else:
        # Métricas de rango
        total_oc = int(df["oc_totales_sgm"].sum())
        total_oc_connexa = int(df["oc_desde_ci"].sum())
        proporcion_global = (
            (total_oc_connexa / total_oc) if total_oc > 0 else 0.0
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("OC totales SGM (rango)", value=total_oc)
        with col2:
            st.metric("OC SGM originadas en CONNEXA (rango)", value=total_oc_connexa)
        with col3:
            st.metric(
                "% global originado en CONNEXA",
                value=f"{proporcion_global * 100:,.1f} %",
            )

        # Gráfico 1: OC totales vs OC desde CONNEXA
        df_counts = df[["mes", "oc_totales_sgm", "oc_desde_ci"]].copy()
        df_counts = df_counts.melt(
            id_vars="mes",
            value_vars=["oc_totales_sgm", "oc_desde_ci"],
            var_name="tipo",
            value_name="cantidad",
        )
        df_counts["tipo"] = df_counts["tipo"].map(
            {
                "oc_totales_sgm": "OC totales SGM",
                "oc_desde_ci": "OC SGM originadas en CONNEXA",
            }
        )

        fig1 = px.bar(
            df_counts,
            x="mes",
            y="cantidad",
            color="tipo",
            barmode="group",
            title="OC totales SGM vs OC originadas en CONNEXA (mensual)",
        )
        fig1.update_layout(xaxis_title="Mes", yaxis_title="Cantidad de OC")
        st.plotly_chart(fig1, use_container_width=True)

        # Gráfico 2: Proporción mensual (%)
        if "proporcion_ci" in df.columns:
            df_prop = df.copy()
            df_prop["proporcion_pct"] = df_prop["proporcion_ci"] * 100

            fig2 = px.line(
                df_prop,
                x="mes",
                y="proporcion_pct",
                markers=True,
                title="% de OC SGM originadas en CONNEXA (mensual)",
            )
            fig2.update_layout(
                xaxis_title="Mes",
                yaxis_title="Proporción (%)",
            )
            st.plotly_chart(fig2, use_container_width=True)

        with st.expander("Ver tabla mensual detallada"):
            st.dataframe(df, width="stretch", hide_index=True)
            st.download_button(
                "Descargar CSV (proporción mensual)",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="proporcion_oc_connexa_vs_sgm_mensual.csv",
                mime="text/csv",
            )

# -------------------------------------------------------
# TAB 2: Compras CONNEXA sin cabecera en SGM
# -------------------------------------------------------
with tab_det:
    st.subheader("Compras CONNEXA con prefijo/sufijo sin cabecera en SGM (posibles pendientes)")

    df_miss = fetch_sin_cabe(desde, hasta)

    if df_miss.empty:
        st.success("No se detectaron compras CONNEXA con prefijo/sufijo sin cabecera en SGM en el rango.")
    else:
        # Para mostrar a negocio, conviene resaltar OC SGM y la compra CONNEXA que las origina
        cols_principales = []
        for c in ["f_alta_date", "oc_sgm", "C_COMPRA_KIKKER", "u_prefijo_oc", "u_sufijo_oc"]:
            if c in df_miss.columns:
                cols_principales.append(c)

        st.dataframe(
            df_miss[cols_principales] if cols_principales else df_miss,
            width="stretch",
            hide_index=True,
        )
        st.download_button(
            "Descargar CSV (compras CONNEXA sin cabecera)",
            data=df_miss.to_csv(index=False).encode("utf-8"),
            file_name="compras_connexa_sin_cabecera.csv",
            mime="text/csv",
        )

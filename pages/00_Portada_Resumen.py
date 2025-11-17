# 00_Portada_Resumen.py
# Portada — Monitor CONNEXA → SGM
# Resumen ejecutivo:
#   - Forecast → Propuesta
#   - Pedidos CONNEXA → OC SGM
#   - % OC SGM originadas en CONNEXA
#   - Top compradores / proveedores

import os
import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import (
    get_pg_engine,         # diarco_data
    get_sqlserver_engine,  # SGM
    get_pgp_engine,        # connexa_platform_ms (PRODUCCIÓN CONNEXA)
)
from modules.ui import render_header, make_date_filters
from modules.queries import (
    # Forecast → Propuesta (nuevo esquema supply_planning)
    ensure_forecast_views,
    SQL_FP_CONVERSION_MENSUAL,
    SQL_FP_RANKING_COMPRADOR,
    # Embudo CONNEXA → SGM
    SQL_PG_KIKKER_MENSUAL,
    SQL_SGM_KIKKER_VS_OC_MENSUAL,
    # % OC SGM originadas en CONNEXA
    SQL_SGM_I3_MENSUAL,
    # Ranking compradores (OC CONNEXA)
    SQL_RANKING_COMPRADORES_NOMBRE,
    # Proveedores CONNEXA → SGM
    SQL_SGM_I4_PROV_DETALLE,
)

# -------------------------------------------------------
# Configuración general de la portada
# -------------------------------------------------------
st.set_page_config(
    page_title="Portada — Monitor CONNEXA → SGM",
    page_icon="📊",
    layout="wide",
)

render_header("Portada — Monitor CONNEXA → SGM")

desde, hasta = make_date_filters()

# ==============================================
# Sidebar — Estado de conexiones y origen real
# ==============================================
from sqlalchemy import text

with st.sidebar:
    st.subheader("🔌 Fuentes de datos")

    conn_connexa = conn_diarco = conn_sgm = False
    info_connexa = info_diarco = info_sgm = "—"

    # Connexa Platform (connexa_platform_ms)
    try:
        eng_cnx = get_pgp_engine()
        with eng_cnx.connect() as con:
            db = con.execute(text("select current_database()")).scalar()
            ip = con.execute(text("select host(inet_server_addr())")).scalar()
        conn_connexa = True
        info_connexa = f"{ip} · DB: {db}"
    except Exception as e:
        st.error(f"❌ Connexa Platform MS no disponible\n\n{e}")

    # Diarco Data (diarco_data)
    try:
        eng_diarco = get_pg_engine()
        with eng_diarco.connect() as con:
            db = con.execute(text("select current_database()")).scalar()
            ip = con.execute(text("select host(inet_server_addr())")).scalar()
        conn_diarco = True
        info_diarco = f"{ip} · DB: {db}"
    except Exception as e:
        st.error(f"❌ Diarco Data (PostgreSQL) sin conexión\n\n{e}")

    # SQL Server SGM
    try:
        eng_sgm = get_sqlserver_engine()
        with eng_sgm.connect() as con:
            row = con.execute(text("""
                SELECT 
                    DB_NAME() AS db,
                    CONNECTIONPROPERTY('local_net_address') AS ip
            """)).fetchone()
        conn_sgm = True
        info_sgm = f"{row.ip} · DB: {row.db}"
    except Exception:
        st.warning("⚠️ SQL Server SGM no disponible")

    st.markdown("---")
    st.markdown("### Estado actual")

    st.write(
        f"**Connexa Platform:** {'✅ OK' if conn_connexa else '❌ ERROR'}  \n"
        f"<small>{info_connexa}</small>",
        unsafe_allow_html=True,
    )
    st.write(
        f"**Diarco Data:** {'✅ OK' if conn_diarco else '❌ ERROR'}  \n"
        f"<small>{info_diarco}</small>",
        unsafe_allow_html=True,
    )
    st.write(
        f"**SGM (SQL Server):** {'✅ OK' if conn_sgm else '⚠️ No disponible'}  \n"
        f"<small>{info_sgm}</small>",
        unsafe_allow_html=True,
    )
# =======================================================

ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))




# -------------------------------------------------------
# Bloque Forecast → Propuesta (Indicador 6, resumen)
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_forecast_conversion(desde, hasta) -> pd.DataFrame:
    """
    Serie mensual de ejecuciones de forecast vs propuestas generadas.
    Usa mon.v_forecast_propuesta_base (ensure_forecast_views).
    """
    eng_cnx = get_pgp_engine()
    if eng_cnx is None:
        return pd.DataFrame()

    # Garantizar existencia/actualización de la vista
    ensure_forecast_views(eng_cnx)

    with eng_cnx.connect() as con:
        df = pd.read_sql(
            SQL_FP_CONVERSION_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"])
    for c in ("ejecuciones", "propuestas"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("Int64")
    if "conversion" in df.columns:
        df["conversion"] = pd.to_numeric(df["conversion"], errors="coerce").fillna(0.0)

    return df


@st.cache_data(ttl=ttl)
def fetch_forecast_ranking(desde, hasta, topn: int = 5) -> pd.DataFrame:
    """
    Ranking de compradores por propuestas generadas (Forecast → Propuesta).
    """
    eng_cnx = get_pgp_engine()
    if eng_cnx is None:
        return pd.DataFrame()

    ensure_forecast_views(eng_cnx)

    with eng_cnx.connect() as con:
        df = pd.read_sql(
            SQL_FP_RANKING_COMPRADOR,
            con,
            params={"desde": desde, "hasta": hasta, "topn": topn},
        )

    return df


# -------------------------------------------------------
# Bloque Embudo CONNEXA → SGM (Indicador 2, resumen)
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_embudo_connexa_sgm(desde, hasta) -> pd.DataFrame:
    """
    Pedidos CONNEXA (PostgreSQL) vs OC SGM generadas desde CONNEXA (SQL Server), por mes.
    """
    eng_pg = get_pg_engine()
    eng_ss = get_sqlserver_engine()
    if eng_pg is None or eng_ss is None:
        return pd.DataFrame()

    # Pedidos CONNEXA (PG)
    with eng_pg.connect() as con_pg:
        df_pg = pd.read_sql(
            SQL_PG_KIKKER_MENSUAL,
            con_pg,
            params={"desde": desde, "hasta": hasta},
        )

    if not df_pg.empty:
        df_pg["mes"] = pd.to_datetime(df_pg["mes"])
        df_pg.rename(
            columns={
                "kikker_distintos_pg": "pedidos_connexa",
                "total_bultos_pg": "bultos_connexa",
            },
            inplace=True,
        )
        df_pg["pedidos_connexa"] = pd.to_numeric(
            df_pg["pedidos_connexa"], errors="coerce"
        ).fillna(0).astype("Int64")
        df_pg["bultos_connexa"] = pd.to_numeric(
            df_pg["bultos_connexa"], errors="coerce"
        ).fillna(0.0)
    else:
        df_pg = pd.DataFrame(columns=["mes", "pedidos_connexa", "bultos_connexa"])

    # OC SGM (SQL Server)
    with eng_ss.connect() as con_ss:
        df_sgm = pd.read_sql(
            SQL_SGM_KIKKER_VS_OC_MENSUAL,
            con_ss,
            params={"desde": desde, "hasta": hasta},
        )

    if not df_sgm.empty:
        df_sgm["mes"] = pd.to_datetime(df_sgm["mes"])
        df_sgm.rename(
            columns={
                "kikker_distintos": "nips_sgm",
                "oc_sgm_distintas": "oc_sgm",
                "total_bultos": "bultos_sgm",
            },
            inplace=True,
        )
        for c in ("nips_sgm", "oc_sgm"):
            if c in df_sgm.columns:
                df_sgm[c] = pd.to_numeric(df_sgm[c], errors="coerce").fillna(0).astype("Int64")
        df_sgm["bultos_sgm"] = pd.to_numeric(
            df_sgm["bultos_sgm"], errors="coerce"
        ).fillna(0.0)
    else:
        df_sgm = pd.DataFrame(columns=["mes", "nips_sgm", "oc_sgm", "bultos_sgm"])

    # Embudo
    df = pd.merge(df_pg, df_sgm, on="mes", how="outer").sort_values("mes")

    for c in ("pedidos_connexa", "oc_sgm", "bultos_connexa", "bultos_sgm"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df["tasa_conv_pedidos_oc"] = df.apply(
        lambda r: (r["oc_sgm"] / r["pedidos_connexa"] * 100)
        if r.get("pedidos_connexa", 0) > 0
        else 0.0,
        axis=1,
    )

    return df


# -------------------------------------------------------
# Bloque % OC SGM originadas en CONNEXA (Indicador 3, resumen)
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_prop_oc_connexa(desde, hasta) -> pd.DataFrame:
    """
    Proporción mensual de OC SGM originadas en CONNEXA.
    Basado en SQL_SGM_I3_MENSUAL.
    """
    eng_ss = get_sqlserver_engine()
    if eng_ss is None:
        return pd.DataFrame()

    with eng_ss.connect() as con:
        df = pd.read_sql(
            SQL_SGM_I3_MENSUAL,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    if "mes" in df.columns:
        df["mes"] = pd.to_datetime(df["mes"])

    for c in ("oc_totales_sgm", "oc_desde_connexa"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("Int64")

    # Se asume que la query ya trae 'proporcion_ci' o similar; si no, podemos calcularla.
    if "proporcion_ci" in df.columns:
        df["proporcion_ci"] = pd.to_numeric(
            df["proporcion_ci"], errors="coerce"
        ).fillna(0.0)
    else:
        df["proporcion_ci"] = df.apply(
            lambda r: (r["oc_desde_connexa"] / r["oc_totales_sgm"])
            if r.get("oc_totales_sgm", 0) > 0
            else 0.0,
            axis=1,
        )

    return df


# -------------------------------------------------------
# Bloque ranking de compradores y proveedores
# -------------------------------------------------------
@st.cache_data(ttl=ttl)
def fetch_ranking_compradores_oc(desde, hasta, topn: int = 5) -> pd.DataFrame:
    """
    Top compradores por OC generadas en CONNEXA (Indicador 1, resumen).
    """
    eng_pg = get_pg_engine()
    if eng_pg is None:
        return pd.DataFrame()

    with eng_pg.connect() as con:
        df = pd.read_sql(
            SQL_RANKING_COMPRADORES_NOMBRE,
            con,
            params={"desde": desde, "hasta": hasta, "topn": topn},
        )

    return df


@st.cache_data(ttl=ttl)
def fetch_ranking_proveedores(desde, hasta, topn: int = 5) -> pd.DataFrame:
    """
    Top proveedores por OC SGM originadas en CONNEXA (usando detalle de T874).
    """
    eng_ss = get_sqlserver_engine()
    if eng_ss is None:
        return pd.DataFrame()

    with eng_ss.connect() as con:
        df = pd.read_sql(
            SQL_SGM_I4_PROV_DETALLE,
            con,
            params={"desde": desde, "hasta": hasta},
        )

    if df.empty:
        return df

    if "c_proveedor" in df.columns:
        df["c_proveedor"] = pd.to_numeric(
            df["c_proveedor"], errors="coerce"
        ).astype("Int64")
    if "q_bultos_ci" in df.columns:
        df["q_bultos_ci"] = pd.to_numeric(
            df["q_bultos_ci"], errors="coerce"
        ).fillna(0.0)

    # Agregado simple por proveedor
    rk = (
        df.groupby("c_proveedor", dropna=False)
          .agg(
              oc_distintas=("oc_sgm", "nunique"),
              bultos_total=("q_bultos_ci", "sum"),
          )
          .reset_index()
    )
    rk = rk.sort_values("bultos_total", ascending=False).head(topn)
    return rk


# =======================================================
# RENDER PORTADA
# =======================================================

# -------------------------
# Sección 1: Forecast → Propuesta
# -------------------------
st.markdown("## 1. Forecast → Propuesta de compra (CONNEXA)")

df_fp = fetch_forecast_conversion(desde, hasta)

if df_fp.empty:
    st.info("No se encontraron ejecuciones de forecast ni propuestas en el rango seleccionado.")
else:
    total_ejec = int(df_fp["ejecuciones"].sum())
    total_prop = int(df_fp["propuestas"].sum())
    conv_global = (
        total_prop / total_ejec if total_ejec > 0 else 0.0
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Ejecuciones de forecast (rango)", value=total_ejec)
    with col2:
        st.metric("Propuestas generadas (rango)", value=total_prop)
    with col3:
        st.metric("Tasa global Forecast → Propuesta", value=f"{conv_global * 100:,.1f} %")

    fig_fp = px.line(
        df_fp,
        x="mes",
        y="conversion",
        markers=True,
        title="Tasa de conversión Forecast → Propuesta (mensual)",
    )
    fig_fp.update_layout(xaxis_title="Mes", yaxis_title="Conversión (ratio)")
    st.plotly_chart(fig_fp, width='content')

    with st.expander("Detalle mensual Forecast → Propuesta"):
        st.dataframe(df_fp, width='content', hide_index=True)

# -------------------------
# Sección 2: Pedidos CONNEXA → OC SGM (Embudo)
# -------------------------
st.markdown("---")
st.markdown("## 2. Embudo CONNEXA → SGM (Pedidos vs OC)")

df_emb = fetch_embudo_connexa_sgm(desde, hasta)

if df_emb.empty:
    st.info("No se encontraron datos de CONNEXA ni SGM para el rango seleccionado.")
else:
    total_pedidos = int(df_emb["pedidos_connexa"].sum())
    total_oc = int(df_emb["oc_sgm"].sum())
    total_bultos_pg = float(df_emb["bultos_connexa"].sum())
    total_bultos_sgm = float(df_emb["bultos_sgm"].sum())
    tasa_global = total_oc / total_pedidos * 100 if total_pedidos > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Pedidos CONNEXA (NIPs)", value=total_pedidos)
    with col2:
        st.metric("OC SGM generadas desde CONNEXA", value=total_oc)
    with col3:
        st.metric("Bultos CONNEXA (rango)", value=f"{total_bultos_pg:,.0f}")
    with col4:
        st.metric("Bultos en OC SGM (rango)", value=f"{total_bultos_sgm:,.0f}")

    fig_emb1 = px.bar(
        df_emb,
        x="mes",
        y=["pedidos_connexa", "oc_sgm"],
        barmode="group",
        title="Pedidos CONNEXA vs OC SGM generadas (mensual)",
    )
    fig_emb1.update_layout(
        xaxis_title="Mes",
        yaxis_title="Cantidad",
        legend_title="Serie",
    )
    st.plotly_chart(fig_emb1, width='content')

    fig_emb2 = px.line(
        df_emb,
        x="mes",
        y="tasa_conv_pedidos_oc",
        markers=True,
        title="Tasa de conversión Pedidos CONNEXA → OC SGM (mensual, %)",
    )
    fig_emb2.update_layout(xaxis_title="Mes", yaxis_title="Conversión (%)")
    st.plotly_chart(fig_emb2, width='content')

# -------------------------
# Sección 3: % OC SGM originadas en CONNEXA
# -------------------------
st.markdown("---")
st.markdown("## 3. % de OC SGM originadas en CONNEXA")

df_prop = fetch_prop_oc_connexa(desde, hasta)


if df_prop.empty:
    st.info("No se encontraron datos de OC SGM para el rango seleccionado.")
else:
    # Validación de columnas esperadas
    columnas_esperadas = ["oc_totales_sgm", "oc_desde_connexa", "proporcion_ci", "mes"]
    columnas_faltantes = [col for col in columnas_esperadas if col not in df_prop.columns]

    if columnas_faltantes:
        st.error(f"Faltan las siguientes columnas en el DataFrame: {', '.join(columnas_faltantes)}")
        st.stop()

    total_sgm = int(df_prop["oc_totales_sgm"].sum())
    total_connexa = int(df_prop["oc_desde_connexa"].sum())
    prop_global = total_connexa / total_sgm if total_sgm > 0 else 0.0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("OC totales SGM (rango)", value=total_sgm)
    with col2:
        st.metric("OC SGM originadas en CONNEXA (rango)", value=total_connexa)
    with col3:
        st.metric("% global originado en CONNEXA", value=f"{prop_global * 100:,.1f} %")

    df_prop["proporcion_pct"] = df_prop["proporcion_ci"] * 100

    fig_prop = px.line(
        df_prop,
        x="mes",
        y="proporcion_pct",
        markers=True,
        title="% de OC SGM originadas en CONNEXA (mensual)",
    )
    fig_prop.update_layout(xaxis_title="Mes", yaxis_title="Proporción (%)")
    st.plotly_chart(fig_prop, width='content')

# -------------------------
# Sección 4: Top compradores y proveedores
# -------------------------
st.markdown("---")
st.markdown("## 4. Top compradores y proveedores (uso de CONNEXA)")

col_left, col_right = st.columns(2)

with col_left:
    st.markdown("### Top compradores por OC CONNEXA")
    df_rk_comp = fetch_ranking_compradores_oc(desde, hasta, topn=5)
    if df_rk_comp.empty:
        st.info("Sin datos de ranking de compradores para el rango seleccionado.")
    else:
        y_col = "comprador" if "comprador" in df_rk_comp.columns else "c_comprador"
        fig_c = px.bar(
            df_rk_comp.sort_values("oc_total"),
            x="oc_total",
            y=y_col,
            orientation="h",
            title="Top 5 compradores por OC CONNEXA",
            text="oc_total",
        )
        fig_c.update_layout(xaxis_title="# OC CONNEXA", yaxis_title="")
        st.plotly_chart(fig_c, width='content')

with col_right:
    st.markdown("### Top proveedores por OC SGM desde CONNEXA")
    df_rk_prov = fetch_ranking_proveedores(desde, hasta, topn=5)
    if df_rk_prov.empty:
        st.info("Sin datos de proveedores para el rango seleccionado.")
    else:
        df_rk_prov["label"] = df_rk_prov["c_proveedor"].astype(str)
        fig_p = px.bar(
            df_rk_prov.sort_values("bultos_total"),
            x="bultos_total",
            y="label",
            orientation="h",
            title="Top 5 proveedores por bultos en OC SGM desde CONNEXA",
            text="bultos_total",
        )
        fig_p.update_layout(xaxis_title="Bultos", yaxis_title="Proveedor")
        st.plotly_chart(fig_p, width='content')

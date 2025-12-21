# pages/00_Portada_Resumen.py
# Portada — Monitor CONNEXA → SGM (visión gerencial)
#
# Ejes principales:
#   1) Uso general del sistema
#   2) Gestión de compradores
#   3) Incorporación de proveedores

from datetime import date
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

from modules.queries.compradores import (
    get_ranking_compradores_resumen,
    get_ranking_comprador_forecast,
    get_productividad_comprador_mensual,
)

from modules.queries.proveedores import (
    get_ranking_proveedores_resumen,
    get_proveedores_ci_vs_sgm_mensual,
)


# -------------------------------------------------------
# Configuración general de la página
# -------------------------------------------------------
st.set_page_config(
    page_title="Portada — Monitor CONNEXA → SGM",
    page_icon="📊",
    layout="wide",
)

render_header("Portada — Monitor CONNEXA → SGM")

# Filtros de fecha (rango común para todos los bloques)
desde, hasta = make_date_filters()

# TTL de caché configurable por entorno
ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# =======================================================
# 1. Sidebar — Estado de conexiones
# =======================================================
with st.sidebar:
    st.subheader("🔌 Fuentes de datos")

    conn_connexa = conn_diarco = conn_sgm = False
    info_connexa = info_diarco = info_sgm = "—"

    # Connexa Platform (PostgreSQL)
    try:
        eng_cnx = get_connexa_engine()
        with eng_cnx.connect() as con:
            db = con.execute(text("select current_database()")).scalar()
            ip = con.execute(text("select host(inet_server_addr())")).scalar()
        conn_connexa = True
        info_connexa = f"{ip} · DB: {db} HOST: {eng_cnx.url.host}"
    except Exception as e:
        st.error(f"❌ Connexa Platform no disponible\n\n{e}")

    # Diarco Data (PostgreSQL)
    try:
        eng_diarco = get_diarco_engine()
        with eng_diarco.connect() as con:
            db = con.execute(text("select current_database()")).scalar()
            ip = con.execute(text("select host(inet_server_addr())")).scalar()

        conn_diarco = True
        info_diarco = f"{ip} · DB: {db} HOST: {eng_diarco.url.host}"
    except Exception as e:
        st.error(f"❌ Diarco Data (PostgreSQL) sin conexión\n\n{e}")

    # SQL Server SGM
    try:
        eng_sgm = get_sqlserver_engine()
        if eng_sgm is not None:
            with eng_sgm.connect() as con:
                row = con.execute(text("""
                    SELECT 
                        DB_NAME() AS db,
                        CONNECTIONPROPERTY('local_net_address') AS ip
                """)).fetchone()
            conn_sgm = True
            info_sgm = f"{row.ip} · DB: {row.db} "
        else:
            st.warning("⚠️ Parámetros de conexión a SQL Server incompletos.")
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
# 2. Funciones de carga (capa de caché)
# =======================================================

@st.cache_data(ttl=ttl)
def _init_mon_objects(desde: date, hasta: date) -> bool:
    """
    Inicializa/asegura vistas mon.* y mon.v_forecast_propuesta_base.
    Es idempotente; se invoca una vez por rango en la portada.
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
    Carga los DataFrames principales de USO GENERAL del sistema:
      - oc_generadas_mensual     (Diarco Data)
      - forecast_propuesta       (Connexa)
      - embudo_connexa_sgm       (PG + SQL Server)
      - proporcion_ci_vs_sgm     (SQL Server)
    """
    _ = _init_mon_objects(desde, hasta)

    eng_d = get_diarco_engine()
    eng_c = get_connexa_engine()
    eng_s = get_sqlserver_engine()

    df_oc = get_oc_generadas_mensual(eng_d, desde, hasta) if eng_d else pd.DataFrame()
    df_fp = get_forecast_propuesta_conversion_mensual(eng_c, desde, hasta) if eng_c else pd.DataFrame()
    df_emb = get_embudo_connexa_sgm_mensual(eng_d, eng_s, desde, hasta) if (eng_d and eng_s) else pd.DataFrame()
    df_prop = get_proporcion_ci_vs_sgm_mensual(eng_s, desde, hasta) if eng_s else pd.DataFrame()

    return df_oc, df_fp, df_emb, df_prop


@st.cache_data(ttl=ttl)
def load_compradores(desde: date, hasta: date):
    """
    Carga los DataFrames relacionados con compradores:
      - ranking_compradores_oc      (Diarco Data)
      - ranking_compradores_fp      (Connexa)
      - productividad_comprador     (Connexa, serie mensual)
    """
    eng_d = get_diarco_engine()
    eng_c = get_connexa_engine()

    df_rk_oc = get_ranking_compradores_resumen(eng_d, desde, hasta, topn=20) if eng_d else pd.DataFrame()
    df_rk_fp = get_ranking_comprador_forecast(eng_c, desde, hasta, topn=20) if eng_c else pd.DataFrame()
    df_prod  = get_productividad_comprador_mensual(eng_c, desde, hasta) if eng_c else pd.DataFrame()

    return df_rk_oc, df_rk_fp, df_prod


@st.cache_data(ttl=ttl)
def load_proveedores(desde: date, hasta: date):
    """
    Carga los DataFrames relacionados con proveedores:
      - ranking_proveedores_ci       (SQL Server, vía CI)
      - proveedores_ci_vs_sgm_mensual (SQL Server)
    """
    eng_s = get_sqlserver_engine()

    df_rk_prov = get_ranking_proveedores_resumen(eng_s, desde, hasta, topn=10) if eng_s else pd.DataFrame()
    df_prop_prov = get_proveedores_ci_vs_sgm_mensual(eng_s, desde, hasta) if eng_s else pd.DataFrame()

    return df_rk_prov, df_prop_prov


# =======================================================
# 3. Sección USO GENERAL DEL SISTEMA
# =======================================================
st.markdown("## 1. Uso general del sistema")

df_oc, df_fp, df_emb, df_prop = load_uso_general(desde, hasta)

# -------------------------
# 1.1 Forecast → Propuesta
# -------------------------
st.markdown("### 1.1 Forecast → Propuesta de compra (Connexa)")

if df_fp.empty:
    st.info("No se encontraron ejecuciones de forecast ni propuestas en el rango seleccionado.")
else:
    # Normalización de columnas esperadas
    if "mes" in df_fp.columns:
        df_fp["mes"] = pd.to_datetime(df_fp["mes"])

    total_ejec = int(pd.to_numeric(df_fp.get("ejecuciones", 0), errors="coerce").fillna(0).sum())
    total_prop = int(pd.to_numeric(df_fp.get("propuestas", 0), errors="coerce").fillna(0).sum())
    conv_global = (total_prop / total_ejec) if total_ejec > 0 else 0.0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Ejecuciones de forecast (rango)", value=total_ejec)
    with col2:
        st.metric("Propuestas generadas (rango)", value=total_prop)
    with col3:
        st.metric("Tasa global Forecast → Propuesta", value=f"{conv_global * 100:,.1f} %")

    if "conversion" in df_fp.columns:
        df_fp["conversion_pct"] = pd.to_numeric(df_fp["conversion"], errors="coerce").fillna(0.0) * 100.0
        fig_fp = px.line(
            df_fp,
            x="mes",
            y="conversion_pct",
            markers=True,
            title="Tasa de conversión Forecast → Propuesta (mensual, %)",
        )
        fig_fp.update_layout(xaxis_title="Mes", yaxis_title="Conversión (%)")
        st.plotly_chart(fig_fp, width='stretch')

    with st.expander("Detalle mensual Forecast → Propuesta"):
        st.dataframe(df_fp, width='stretch')


# -------------------------
# 1.2 Embudo CONNEXA → SGM
# -------------------------
st.markdown("---")
st.markdown("### 1.2 Embudo CONNEXA → SGM (Pedidos vs OC)")

if df_emb.empty:
    st.info("No se encontraron datos de CONNEXA ni SGM para el rango seleccionado.")
else:
    # Normalización
    if "mes" in df_emb.columns:
        df_emb["mes"] = pd.to_datetime(df_emb["mes"])

    for c in ("pedidos_connexa", "oc_sgm", "bultos_connexa", "bultos_sgm"):
        if c in df_emb.columns:
            df_emb[c] = pd.to_numeric(df_emb[c], errors="coerce").fillna(0.0)

    total_pedidos = int(df_emb.get("pedidos_connexa", 0).sum())
    total_oc = int(df_emb.get("oc_sgm", 0).sum())
    total_bultos_pg = float(df_emb.get("bultos_connexa", 0).sum())
    total_bultos_sgm = float(df_emb.get("bultos_sgm", 0).sum())
    tasa_global = (total_oc / total_pedidos * 100.0) if total_pedidos > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Pedidos CONNEXA (NIPs)", value=total_pedidos)
    with col2:
        st.metric("OC SGM generadas desde CONNEXA", value=total_oc)
    with col3:
        st.metric("Bultos CONNEXA (rango)", value=f"{total_bultos_pg:,.0f}")
    with col4:
        st.metric("Bultos en OC SGM (rango)", value=f"{total_bultos_sgm:,.0f}")

    # Gráfico de cantidad NIPs vs OC
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
    st.plotly_chart(fig_emb1, width='stretch')

    # Conversión pedidos → OC
    if "tasa_conv_pedidos_oc" in df_emb.columns:
        df_emb["tasa_conv_pedidos_oc"] = pd.to_numeric(
            df_emb["tasa_conv_pedidos_oc"], errors="coerce"
        ).fillna(0.0)
        fig_emb2 = px.line(
            df_emb,
            x="mes",
            y="tasa_conv_pedidos_oc",
            markers=True,
            title="Tasa de conversión Pedidos CONNEXA → OC SGM (mensual, %)",
        )
        fig_emb2.update_layout(xaxis_title="Mes", yaxis_title="Conversión (%)")
        st.plotly_chart(fig_emb2, width='stretch')


# -------------------------
# 1.3 % OC SGM originadas en CONNEXA
# -------------------------
st.markdown("---")
st.markdown("### 1.3 % de OC SGM originadas en CONNEXA")

if df_prop.empty:
    st.info("No se encontraron datos de OC SGM para el rango seleccionado.")
else:
    if "mes" in df_prop.columns:
        df_prop["mes"] = pd.to_datetime(df_prop["mes"])

    for c in ("oc_totales_sgm", "oc_desde_connexa"):
        if c in df_prop.columns:
            df_prop[c] = pd.to_numeric(df_prop[c], errors="coerce").fillna(0.0)

    total_sgm = int(df_prop.get("oc_totales_sgm", 0).sum())
    total_connexa = int(df_prop.get("oc_desde_connexa", 0).sum())
    prop_global = (total_connexa / total_sgm) if total_sgm > 0 else 0.0

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("OC totales SGM (rango)", value=total_sgm)
    with col2:
        st.metric("OC SGM originadas en CONNEXA (rango)", value=total_connexa)
    with col3:
        st.metric("% global originado en CONNEXA", value=f"{prop_global * 100:,.1f} %")

    # Serie mensual
    if "proporcion_ci" in df_prop.columns:
        df_prop["proporcion_pct"] = pd.to_numeric(
            df_prop["proporcion_ci"], errors="coerce"
        ).fillna(0.0) * 100.0
        fig_prop = px.line(
            df_prop,
            x="mes",
            y="proporcion_pct",
            markers=True,
            title="% de OC SGM originadas en CONNEXA (mensual)",
        )
        fig_prop.update_layout(xaxis_title="Mes", yaxis_title="Proporción (%)")
        st.plotly_chart(fig_prop, width='stretch')


# =======================================================
# 4. Sección GESTIÓN DE LOS COMPRADORES
# =======================================================
st.markdown("---")
st.markdown("## 2. Gestión de los compradores")

df_rk_comp_oc, df_rk_comp_fp, df_prod_comp = load_compradores(desde, hasta)

col_left, col_right = st.columns(2)

# -------------------------
# 2.1 Ranking por OC Connexa
# -------------------------
with col_left:
    st.markdown("### 2.1 Ranking por OC generadas en CONNEXA")
    if df_rk_comp_oc.empty:
        st.info("Sin datos de ranking de compradores por OC CONNEXA.")
    else:
        df_plot = df_rk_comp_oc.copy()

        # Normalizar nombres de columnas: minúsculas y sin espacios
        df_plot.columns = [c.strip().lower() for c in df_plot.columns]

        # Intentar identificar la columna de comprador
        if "comprador" not in df_plot.columns:
            if "n_comprador" in df_plot.columns:
                df_plot["comprador"] = df_plot["n_comprador"].astype(str)
            else:
                # fallback por si la query no trae un código claro
                df_plot["comprador"] = df_plot.index.astype(str)

        # Buscar candidatos de columna "total_oc"
        oc_candidates = [
            c for c in df_plot.columns
            if c in ("total_oc", "oc", "oc_sgm", "oc_connexa", "oc_total", "oc_distintas", "total_pedidos")
        ]

        # Buscar candidatos de columna "total_bultos"
        bultos_candidates = [
            c for c in df_plot.columns
            if c in ("total_bultos", "bultos", "bultos_sgm", "bultos_connexa", "bultos_total")
        ]

        if not oc_candidates:
            st.warning(
                "No se encontró una columna equivalente a 'total_oc' en el ranking de compradores.\n\n"
                f"Columnas disponibles: {list(df_plot.columns)}"
            )
            with st.expander("Ver datos crudos de ranking compradores OC"):
                st.dataframe(df_plot, width='stretch')
        else:
            oc_col = oc_candidates[0]
            # Renombrar a nombres estándar internos
            rename_map = {oc_col: "total_pedidos"}
            if bultos_candidates:
                rename_map[bultos_candidates[0]] = "total_bultos"
            df_plot = df_plot.rename(columns=rename_map)

            # Asegurar que 'total_pedidos' es numérico
            df_plot["total_pedidos"] = pd.to_numeric(df_plot["total_pedidos"], errors="coerce").fillna(0)

            fig_c = px.bar(
                df_plot.sort_values("total_pedidos"),
                x="total_pedidos",
                y="comprador",
                orientation="h",
                title="Top compradores por # OC CONNEXA",
                text="total_pedidos",
            )
            fig_c.update_layout(xaxis_title="# OC CONNEXA", yaxis_title="")
            st.plotly_chart(fig_c, width='stretch')

            with st.expander("Detalle ranking OC CONNEXA"):
                st.dataframe(df_plot, width='stretch')


# -------------------------
# 2.2 Ranking por Forecast → Propuesta
# -------------------------
with col_right:
    st.markdown("### 2.2 Ranking por uso de Forecast → Propuesta")
    if df_rk_comp_fp.empty:
        st.info("Sin datos de ranking de compradores por propuestas.")
    else:
        # Espera columnas: comprador, propuestas, monto_total, p50_ajuste_min, p90_ajuste_min
        df_plot = df_rk_comp_fp.copy()
        if "comprador" not in df_plot.columns:
            df_plot["comprador"] = df_plot.index.astype(str)

        fig_fp_rk = px.bar(
            df_plot.sort_values("propuestas"),
            x="propuestas",
            y="comprador",
            orientation="h",
            title="Top compradores por # Propuestas",
            text="propuestas",
        )
        fig_fp_rk.update_layout(xaxis_title="# Propuestas", yaxis_title="")
        st.plotly_chart(fig_fp_rk, width='stretch')

        with st.expander("Detalle ranking Forecast → Propuesta"):
            st.dataframe(df_plot, width='stretch')


# -------------------------
# 2.3 Productividad mensual por comprador (tiempos)
# -------------------------
st.markdown("### 2.3 Productividad y tiempos de gestión por comprador (mensual)")

if df_prod_comp.empty:
    st.info("Sin datos de productividad de compradores en el rango seleccionado.")
else:
    # Espera columnas: mes, comprador, propuestas, monto_total, p50_ajuste_min, p90_ajuste_min, p50_lead_min, avg_exec_min
    if "mes" in df_prod_comp.columns:
        df_prod_comp["mes"] = pd.to_datetime(df_prod_comp["mes"])

    # Resumen global: promedio de P50/P90 de ajuste
    df_kpi = df_prod_comp.copy()
    for c in ("p50_ajuste_min", "p90_ajuste_min", "p50_lead_min", "avg_exec_min"):
        if c in df_kpi.columns:
            df_kpi[c] = pd.to_numeric(df_kpi[c], errors="coerce")

    kpi_p50 = float(df_kpi["p50_ajuste_min"].median()) if "p50_ajuste_min" in df_kpi.columns else 0.0
    kpi_p90 = float(df_kpi["p90_ajuste_min"].median()) if "p90_ajuste_min" in df_kpi.columns else 0.0

    col_a, col_b = st.columns(2)
    with col_a:
        st.metric("P50 tiempo de ajuste (min)", value=f"{kpi_p50:,.1f}")
    with col_b:
        st.metric("P90 tiempo de ajuste (min)", value=f"{kpi_p90:,.1f}")

    # Gráfico opcional: propuestas por mes y comprador
    fig_prod = px.bar(
        df_prod_comp,
        x="mes",
        y="propuestas",
        color="comprador",
        title="Propuestas por comprador (mensual)",
    )
    fig_prod.update_layout(xaxis_title="Mes", yaxis_title="# Propuestas")
    st.plotly_chart(fig_prod, width='stretch')

    with st.expander("Detalle productividad compradores (mensual)"):
        st.dataframe(df_prod_comp, width='stretch')


# =======================================================
# 5. Sección INCORPORACIÓN DE PROVEEDORES
# =======================================================
st.markdown("---")
st.markdown("## 3. Incorporación de proveedores")

df_rk_prov, df_prop_prov = load_proveedores(desde, hasta)

with st.expander("Debug: columnas ranking proveedores"):
    st.write(list(df_rk_prov.columns))
    st.dataframe(df_rk_prov.head(20), width="stretch")


col_p_left, col_p_right = st.columns(2)

# -------------------------
# 3.1 Ranking de proveedores abastecidos vía CONNEXA
# -------------------------
with col_p_left:
    st.markdown("### 3.1 Top proveedores abastecidos vía CONNEXA → SGM")
    if df_rk_prov.empty:
        st.info("Sin datos de proveedores abastecidos vía CONNEXA en el rango seleccionado.")
    else:
        # Espera columnas: c_proveedor, n_proveedor, oc_distintas, total_bultos, label
        df_plot = df_rk_prov.copy()
        if "label" not in df_plot.columns and "n_proveedor" in df_plot.columns:
            df_plot["label"] = df_plot["n_proveedor"].astype(str)
        
        fig_prov = px.bar(
            df_plot.sort_values("bultos_total"),
            x="bultos_total",
            y="label",
            orientation="h",
            title="Top proveedores por bultos en OC SGM desde CONNEXA",
            text="bultos_total",
        )       
        
        fig_prov.update_layout(xaxis_title="Bultos", yaxis_title="Proveedor")
        st.plotly_chart(fig_prov, width='stretch')

        with st.expander("Detalle ranking de proveedores (CI → SGM)"):
            st.dataframe(df_plot, width='stretch')


# -------------------------
# 3.2 % de proveedores gestionados vía CONNEXA
# -------------------------
with col_p_right:
    st.markdown("### 3.2 % de proveedores gestionados vía CONNEXA sobre total SGM")

    if df_prop_prov.empty:
        st.info("Sin datos de proveedores en SGM / CONNEXA para el rango seleccionado.")
    else:
        if "mes" in df_prop_prov.columns:
            df_prop_prov["mes"] = pd.to_datetime(df_prop_prov["mes"])

        for c in ("prov_totales_sgm", "prov_desde_ci"):
            if c in df_prop_prov.columns:
                df_prop_prov[c] = pd.to_numeric(df_prop_prov[c], errors="coerce").fillna(0)

        total_prov_sgm = int(df_prop_prov.get("prov_totales_sgm", 0).sum())
        total_prov_ci = int(df_prop_prov.get("prov_desde_ci", 0).sum())
        prop_global_prov = (total_prov_ci / total_prov_sgm) if total_prov_sgm > 0 else 0.0

        st.metric(
            "% global de proveedores gestionados vía CONNEXA",
            value=f"{prop_global_prov * 100:,.1f} %",
        )

        if "proporcion_ci_prov" in df_prop_prov.columns:
            df_prop_prov["proporcion_ci_prov"] = pd.to_numeric(
                df_prop_prov["proporcion_ci_prov"], errors="coerce"
            ).fillna(0.0)

            df_prop_prov["proporcion_pct"] = df_prop_prov["proporcion_ci_prov"] * 100.0

            fig_prop_prov = px.line(
                df_prop_prov,
                x="mes",
                y="proporcion_pct",
                markers=True,
                title="% de proveedores gestionados vía CONNEXA (mensual)",
            )
            fig_prop_prov.update_layout(
                xaxis_title="Mes",
                yaxis_title="Proporción (%)",
            )
            st.plotly_chart(fig_prop_prov, width='stretch')

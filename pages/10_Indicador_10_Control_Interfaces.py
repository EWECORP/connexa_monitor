#10_Indicador_10_Control_Interfaces.py

# -*- coding: utf-8 -*-
import os
from datetime import date
import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_pg_engine
from modules.ui import render_header
from modules.control_interfaces import (
    obtener_control_interfaces,
    obtener_trend_tabla,
    sugerir_indices,
)

st.set_page_config(page_title="Indicador 10 — Control de Interfaces", page_icon="🧩", layout="wide")
render_header("Indicador 10 — Control de Interfaces (PostgreSQL)")

# ---------- Configuración de tablas y campos ----------
TABLAS_FECHAS_ESTANDAR = [
    "base_productos_vigentes", "base_stock_sucursal", "t020_proveedor",
    "m_3_articulos", "t050_articulos", "t051_articulos_sucursal",
    "t060_stock", "t080_oc_cabe", "t081_oc_deta", "t100_empresa_suc",
    "t052_articulos_proveedor", "t710_estadis_oferta_folder", "t710_estadis_precios",
    "t710_estadis_reposicion", "t117_compradores", "t114_rubros",
    "base_forecast_oc_demoradas","t080_oc_pendientes","base_transferencias_pendientes",
    "t020_proveedor_dias_entrega_deta","t020_proveedor_dias_entrega_cabe"

]
tablas_dict_1 = {t: "fecha_extraccion" for t in TABLAS_FECHAS_ESTANDAR}

tablas_dict_2 = {
    "m_91_sucursales": "f_proc",
    "m_92_depositos": "f_proc",
    "m_93_sustitutos": "f_proc",
    "m_94_alternativos": "f_proc",
    "m_95_sensibles": "f_proc",
    "m_96_stock_seguridad": "f_proc",
    "t702_est_vtas_por_articulo": "f_venta",
    "t702_est_vtas_por_articulo_dbarrio": "f_venta",
    "base_ventas_extendida": "fecha_procesado"
}
TABLAS_TOTAL = {**tablas_dict_1, **tablas_dict_2}

# Parámetros de control
TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))
DIAS_TREND = 14
UMBRAL_ATRASO_DIAS = 1  # >1 día de atraso = alerta

# ---------- Datos ----------
@st.cache_data(ttl=TTL)
def fetch_control_df():
    eng = get_pg_engine()
    return obtener_control_interfaces(eng, TABLAS_TOTAL)

df = fetch_control_df()

# ---------- KPIs y tabla ----------
if df.empty:
    st.info("Sin resultados. Verifiquen conexión a PostgreSQL y permisos de lectura sobre esquema `src`.")
else:
    # Aseguramos tipos amigables y cálculo de atraso (en días) respecto de hoy
    hoy = pd.Timestamp(date.today())
    df["ultima_fecha_extraccion_local"] = pd.to_datetime(
        df["ultima_fecha_extraccion"], errors="coerce"
    ).dt.tz_localize(None)
    df["atraso_dias"] = (hoy - df["ultima_fecha_extraccion_local"].dt.floor("D")).dt.days # type: ignore
    df.loc[df["ultima_fecha_extraccion_local"].isna(), "atraso_dias"] = pd.NA

    total_tablas = len(df)
    con_dato = int(df["ultima_fecha_extraccion_local"].notna().sum())
    en_error = int(df["error"].notna().sum())
    en_alerta = int(((df["atraso_dias"].fillna(999) > UMBRAL_ATRASO_DIAS)).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tablas controladas", total_tablas)
    c2.metric("Con última fecha válida", con_dato)
    c3.metric("En alerta (atraso)", en_alerta)
    c4.metric("Con error/columna faltante", en_error)

    st.divider()
    st.subheader("Estado de interfaces (último día disponible)")

    # ---- construir el dataframe a mostrar (con nombre final de columna) ----
    show = df.assign(
        ultima_fecha_extraccion=df["ultima_fecha_extraccion_local"]
    )[["tabla", "campo_fecha", "ultima_fecha_extraccion", "cantidad_registros", "atraso_dias", "error"]]

    # ---- estilos por fila (corregido: usar nombres reales de 'show') ----
    def _highlight(row):
        val = row.get("ultima_fecha_extraccion")
        atraso = row.get("atraso_dias")
        atraso_val = None if pd.isna(atraso) else int(atraso)
        # Toda la fila en rojo si falta la última fecha
        if pd.isna(val):
            return ["background-color: #ff0000; color: white"] * len(row)
        # Toda la fila en amarillo si supera el umbral de atraso
        if atraso_val is not None and atraso_val > UMBRAL_ATRASO_DIAS:
            return ["background-color: #fff4cc; color: black"] * len(row)
        # Sin estilo si está OK
        return [""] * len(row)

    st.dataframe(
        show.style.apply(_highlight, axis=1),
        width='stretch',
        hide_index=True,
    )

    st.download_button(
        "Descargar control (CSV)",
        data=show.to_csv(index=False).encode("utf-8"),
        file_name="control_interfaces.csv",
        mime="text/csv",
    )

    st.divider()
    st.subheader("Trend por tabla (últimos 14 días relativos al último día con datos)")
    sel_tabla = st.selectbox("Elegir tabla para inspeccionar:", options=sorted(TABLAS_TOTAL.keys()))
    if sel_tabla:
        campo = TABLAS_TOTAL[sel_tabla]
        eng = get_pg_engine()
        trend = obtener_trend_tabla(eng, sel_tabla, campo, days=DIAS_TREND)
        if trend.empty:
            st.warning("No se pudo recuperar la serie de días para la tabla seleccionada.")
        else:
            fig = px.bar(trend, x="fecha", y="cantidad", title=f"{sel_tabla} — registros por día")
            st.plotly_chart(fig, width="stretch")
            with st.expander("Ver datos"):
                st.dataframe(trend, width="stretch", hide_index=True)

    with st.expander("Sugerencias de índices (copiar/pegar en PostgreSQL)"):
        stmts = sugerir_indices(TABLAS_TOTAL, schema="src")
        st.code("\n".join(stmts), language="sql")

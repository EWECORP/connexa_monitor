# -*- coding: utf-8 -*-
"""Exportacion de parametros logisticos por comprador, proveedor y grupo."""

from __future__ import annotations

from copy import copy
from datetime import datetime
from io import BytesIO
import os

import pandas as pd
import streamlit as st

from modules.db import get_connexa_engine, get_diarco_engine
from modules.queries.datos_logisticos import (
    SQL_CATALOGO_FILTROS,
    SQL_CATALOGO_PROVEEDORES,
    SQL_GRUPOS_SUCURSALES,
    construir_consulta_datos_logisticos,
)
from modules.ui import render_header


st.set_page_config(
    page_title="Exportacion de Datos Logisticos",
    page_icon="📤",
    layout="wide",
)
render_header("Exportacion de Datos Logisticos")
st.caption(
    "Parametros de productos vigentes con abastecimiento 3 para sucursales desde la 300. "
    "Los filtros son opcionales y admiten seleccion multiple."
)

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))


@st.cache_data(ttl=TTL, show_spinner=False)
def cargar_catalogo_filtros() -> pd.DataFrame:
    with get_diarco_engine().connect() as con:
        return pd.read_sql(SQL_CATALOGO_FILTROS, con)


@st.cache_data(ttl=TTL, show_spinner=False)
def cargar_catalogo_proveedores() -> pd.DataFrame:
    with get_diarco_engine().connect() as con:
        return pd.read_sql(SQL_CATALOGO_PROVEEDORES, con)


@st.cache_data(ttl=TTL, show_spinner=False)
def cargar_grupos_sucursales() -> pd.DataFrame:
    with get_connexa_engine().connect() as con:
        return pd.read_sql(SQL_GRUPOS_SUCURSALES, con)


@st.cache_data(ttl=TTL, show_spinner=True)
def cargar_datos(
    compradores: tuple[int, ...],
    proveedores: tuple[int, ...],
    sucursales: tuple[int, ...],
) -> pd.DataFrame:
    consulta, params = construir_consulta_datos_logisticos(
        compradores, proveedores, sucursales
    )
    with get_diarco_engine().connect() as con:
        return pd.read_sql(consulta, con, params=params)


@st.cache_data(show_spinner=False)
def generar_excel(df: pd.DataFrame) -> bytes:
    salida = BytesIO()
    exportable = df.copy()
    for columna in exportable.select_dtypes(include=["datetimetz"]).columns:
        exportable[columna] = exportable[columna].dt.tz_localize(None)

    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        exportable.to_excel(writer, sheet_name="Datos logisticos", index=False)
        hoja = writer.sheets["Datos logisticos"]
        hoja.freeze_panes = "A2"
        hoja.auto_filter.ref = hoja.dimensions

        for celda in hoja[1]:
            fuente = copy(celda.font)
            fuente.bold = True
            celda.font = fuente

        for indice, columna in enumerate(exportable.columns, start=1):
            valores = exportable[columna].dropna().astype(str)
            largo_datos = int(valores.str.len().max()) if not valores.empty else 0
            hoja.column_dimensions[hoja.cell(1, indice).column_letter].width = min(
                max(len(str(columna)) + 2, largo_datos + 2), 45
            )

    return salida.getvalue()


def etiqueta(codigo, nombre) -> str:
    return f"{int(codigo)} — {str(nombre).strip()}"


try:
    catalogo = cargar_catalogo_filtros()
    catalogo_proveedores = cargar_catalogo_proveedores()
    grupos = cargar_grupos_sucursales()
except Exception as exc:
    st.error(f"No fue posible cargar los filtros del reporte: {exc}")
    st.stop()

if catalogo.empty:
    st.warning("No hay datos disponibles para el universo configurado.")
    st.stop()

compradores_df = (
    catalogo[["cod_comprador", "n_comprador"]]
    .dropna(subset=["cod_comprador"])
    .drop_duplicates(subset=["cod_comprador"])
    .sort_values(["n_comprador", "cod_comprador"])
)
proveedores_df = (
    catalogo_proveedores[["c_proveedor", "n_proveedor"]]
    .dropna(subset=["c_proveedor"])
    .drop_duplicates(subset=["c_proveedor"])
    .sort_values(["n_proveedor", "c_proveedor"])
)

comprador_labels = {
    etiqueta(row.cod_comprador, row.n_comprador): int(row.cod_comprador)
    for row in compradores_df.itertuples(index=False)
}
proveedor_labels = {
    etiqueta(row.c_proveedor, row.n_proveedor): int(row.c_proveedor)
    for row in proveedores_df.itertuples(index=False)
}

grupos_validos = grupos.dropna(subset=["grupo_id"]).copy()
grupos_validos["codigo_sucursal_num"] = pd.to_numeric(
    grupos_validos["codigo_sucursal"], errors="coerce"
).astype("Int64")
conteos_grupo = (
    grupos_validos.dropna(subset=["codigo_sucursal_num"])
    .groupby(["grupo_id", "grupo"], as_index=False)["codigo_sucursal_num"]
    .nunique()
)
grupo_labels = {
    f"{row.grupo} ({int(row.codigo_sucursal_num)} sucursales)": str(row.grupo_id)
    for row in conteos_grupo.itertuples(index=False)
}

st.subheader("Filtros")
col1, col2, col3 = st.columns(3)
with col1:
    compradores_elegidos = st.multiselect(
        "Compradores",
        options=list(comprador_labels),
        placeholder="Todos los compradores",
    )
with col2:
    todos_proveedores = st.checkbox(
        "Seleccionar todos los proveedores",
        value=False,
        help="Incluye todos los proveedores con datos logisticos.",
    )
    limite_proveedores = st.number_input(
        "Cantidad maxima de proveedores",
        min_value=1,
        max_value=max(len(proveedor_labels), 1),
        value=min(50, max(len(proveedor_labels), 1)),
        step=1,
        disabled=todos_proveedores,
        help="Ingrese cuantos proveedores desea poder seleccionar.",
    )
    if (
        not todos_proveedores
        and len(st.session_state.get("logistica_proveedores_ui", []))
        > int(limite_proveedores)
    ):
        st.session_state["logistica_proveedores_ui"] = st.session_state[
            "logistica_proveedores_ui"
        ][: int(limite_proveedores)]
        st.warning(
            "La seleccion se redujo para respetar la nueva cantidad maxima."
        )
    proveedores_elegidos = st.multiselect(
        "Proveedores",
        options=list(proveedor_labels),
        placeholder="Busque por codigo o nombre",
        max_selections=None if todos_proveedores else int(limite_proveedores),
        disabled=todos_proveedores,
        key="logistica_proveedores_ui",
    )
with col3:
    grupos_elegidos = st.multiselect(
        "Grupos de sucursales",
        options=list(grupo_labels),
        placeholder="Todas las sucursales",
        help="Si selecciona varios grupos, se incluye la union de sus sucursales.",
    )

compradores_ids = tuple(sorted(comprador_labels[x] for x in compradores_elegidos))
proveedores_ids = (
    ()
    if todos_proveedores
    else tuple(sorted(proveedor_labels[x] for x in proveedores_elegidos))
)
grupos_ids = {grupo_labels[x] for x in grupos_elegidos}
sucursales_ids = tuple(
    sorted(
        grupos_validos.loc[
            grupos_validos["grupo_id"].astype(str).isin(grupos_ids),
            "codigo_sucursal_num",
        ]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
) if grupos_ids else ()

if grupos_elegidos and not sucursales_ids:
    st.warning("Los grupos seleccionados no tienen sucursales con codigo numerico asociado.")

proveedores_invalidos = not todos_proveedores and not proveedores_ids
if proveedores_invalidos:
    st.info("Seleccione al menos un proveedor o active la opcion para incluirlos a todos.")

col_accion, col_info = st.columns([1, 3])
with col_accion:
    ejecutar = st.button(
        "Generar reporte",
        type="primary",
        use_container_width=True,
        disabled=bool(
            proveedores_invalidos or (grupos_elegidos and not sucursales_ids)
        ),
    )
with col_info:
    st.caption(
        f"Universo disponible: {len(proveedor_labels)} proveedores. "
        "Los filtros de compradores y grupos vacios incluyen todos sus valores."
    )

if ejecutar:
    st.session_state["logistica_filtros"] = (
        compradores_ids,
        proveedores_ids,
        sucursales_ids,
    )

filtros_aplicados = st.session_state.get("logistica_filtros")
if filtros_aplicados is None:
    st.info("Seleccione los filtros deseados y presione Generar reporte.")
    st.stop()

try:
    df = cargar_datos(*filtros_aplicados)
except Exception as exc:
    st.error(f"No fue posible generar el reporte: {exc}")
    st.stop()

st.divider()
if df.empty:
    st.warning("No se encontraron registros para los filtros aplicados.")
    st.stop()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Registros", f"{len(df):,}".replace(",", "."))
m2.metric("Compradores", df["cod_comprador"].nunique())
m3.metric("Proveedores", df["c_proveedor_primario"].nunique())
m4.metric("Sucursales", df["c_sucu_empr"].nunique())

st.subheader("Vista previa")
st.dataframe(df, width="stretch", hide_index=True, height=500)

archivo = generar_excel(df)
marca_tiempo = datetime.now().strftime("%Y%m%d_%H%M%S")
st.download_button(
    "Descargar Excel",
    data=archivo,
    file_name=f"datos_logisticos_{marca_tiempo}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    type="primary",
)

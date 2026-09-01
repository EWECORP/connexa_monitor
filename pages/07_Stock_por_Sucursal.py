# -*- coding: utf-8 -*-
"""Consulta y exportación del stock de sucursales."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
import os

from openpyxl.styles import Font
import pandas as pd
import streamlit as st

from modules.db import get_diarco_engine
from modules.queries.stock_sucursal import (
    SQL_PROVEEDORES_STOCK,
    SQL_SUCURSALES_STOCK,
    construir_consulta_stock,
)
from modules.ui import render_header


st.set_page_config(
    page_title="Stock por Sucursal",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)
render_header("Stock por Sucursal")
st.caption(
    "Consulta directa de src.base_stock_sucursal. Seleccione una sucursal, "
    "opcionalmente filtre proveedores y exporte el resultado completo a Excel."
)

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))
MAX_FILAS_POR_HOJA = 1_048_575


@st.cache_data(ttl=TTL, show_spinner=False)
def cargar_sucursales() -> pd.DataFrame:
    with get_diarco_engine().connect() as connection:
        return pd.read_sql(SQL_SUCURSALES_STOCK, connection)


@st.cache_data(ttl=TTL, show_spinner=False)
def cargar_proveedores(sucursal: int) -> pd.DataFrame:
    with get_diarco_engine().connect() as connection:
        return pd.read_sql(
            SQL_PROVEEDORES_STOCK,
            connection,
            params={"sucursal": int(sucursal)},
        )


@st.cache_data(ttl=TTL, max_entries=50, show_spinner=False)
def cargar_stock(
    sucursal: int,
    proveedores: tuple[int, ...],
) -> pd.DataFrame:
    consulta, parametros = construir_consulta_stock(sucursal, proveedores)
    with get_diarco_engine().connect() as connection:
        return pd.read_sql(consulta, connection, params=parametros)


@st.cache_data(show_spinner=False)
def generar_excel(df: pd.DataFrame) -> bytes:
    """Genera un XLSX completo y divide los datos si exceden el límite de Excel."""
    salida = BytesIO()
    exportable = df.copy()

    for columna in exportable.select_dtypes(include=["datetimetz"]).columns:
        exportable[columna] = exportable[columna].dt.tz_localize(None)

    with pd.ExcelWriter(salida, engine="openpyxl") as writer:
        cantidad_hojas = max(
            1,
            (len(exportable) + MAX_FILAS_POR_HOJA - 1) // MAX_FILAS_POR_HOJA,
        )
        for indice_hoja in range(cantidad_hojas):
            inicio = indice_hoja * MAX_FILAS_POR_HOJA
            fin = inicio + MAX_FILAS_POR_HOJA
            bloque = exportable.iloc[inicio:fin]
            nombre_hoja = f"Stock {indice_hoja + 1}"
            bloque.to_excel(writer, sheet_name=nombre_hoja, index=False)

            hoja = writer.sheets[nombre_hoja]
            hoja.freeze_panes = "A2"
            hoja.auto_filter.ref = hoja.dimensions
            for celda in hoja[1]:
                celda.font = Font(bold=True)

            muestra = bloque.head(2_000)
            for indice_columna, columna in enumerate(exportable.columns, start=1):
                valores = muestra[columna].dropna().astype(str)
                largo = int(valores.str.len().max()) if not valores.empty else 0
                letra = hoja.cell(1, indice_columna).column_letter
                hoja.column_dimensions[letra].width = min(
                    max(len(str(columna)) + 2, largo + 2),
                    45,
                )

    return salida.getvalue()


def etiqueta(codigo, nombre) -> str:
    return f"{int(codigo)} — {str(nombre).strip()}"


try:
    sucursales_df = cargar_sucursales()
except Exception as exc:
    st.error(f"No fue posible consultar las sucursales: {exc}")
    st.stop()

if sucursales_df.empty:
    st.warning("src.base_stock_sucursal no contiene sucursales disponibles.")
    st.stop()

sucursales = {
    etiqueta(fila.codigo_sucursal, fila.nombre_sucursal): int(fila.codigo_sucursal)
    for fila in sucursales_df.itertuples(index=False)
}

st.subheader("Filtros de consulta")
with st.container(border=True):
    sucursal_label = st.selectbox(
        "Sucursal",
        options=list(sucursales),
        index=None,
        placeholder="Seleccione una sucursal",
        help="La lista contiene las sucursales con registros de stock disponibles.",
    )

if sucursal_label is None:
    st.info("Seleccione una sucursal para habilitar la consulta de stock.")
    st.stop()

sucursal = sucursales[sucursal_label]

try:
    proveedores_df = cargar_proveedores(sucursal)
except Exception as exc:
    st.error(f"No fue posible consultar los proveedores de la sucursal: {exc}")
    st.stop()

proveedores = {
    etiqueta(fila.codigo_proveedor, fila.nombre_proveedor): int(fila.codigo_proveedor)
    for fila in proveedores_df.itertuples(index=False)
}

with st.form("filtros_stock", border=True):
    proveedor_labels = st.multiselect(
        "Proveedor",
        options=list(proveedores),
        placeholder="Todos los proveedores",
        help="Sin selección se consultan todos los proveedores de la sucursal.",
    )
    consultar = st.form_submit_button(
        "Consultar stock",
        type="primary",
        width="content",
    )

proveedores_seleccionados = tuple(
    sorted(proveedores[label] for label in proveedor_labels)
)
filtro_actual = (sucursal, proveedores_seleccionados)

if consultar:
    try:
        with st.spinner("Consultando src.base_stock_sucursal..."):
            st.session_state["stock_sucursal_df"] = cargar_stock(
                sucursal,
                proveedores_seleccionados,
            )
            st.session_state["stock_sucursal_filtro"] = filtro_actual
    except Exception as exc:
        st.error(f"No fue posible consultar el stock: {exc}")
        st.stop()

if st.session_state.get("stock_sucursal_filtro") != filtro_actual:
    st.info("Presione «Consultar stock» para aplicar los filtros seleccionados.")
    st.stop()

stock_df = st.session_state.get("stock_sucursal_df", pd.DataFrame())
if stock_df.empty:
    st.warning("No se encontraron registros para los filtros seleccionados.")
    st.stop()

stock_numerico = pd.to_numeric(stock_df.get("stock", 0), errors="coerce").fillna(0)
reserva_numerica = pd.to_numeric(
    stock_df.get("stock_reserva", 0), errors="coerce"
).fillna(0)
costo_numerico = pd.to_numeric(
    stock_df.get("precio_costo", 0), errors="coerce"
).fillna(0)
stock_total = stock_numerico + reserva_numerica

metrica_1, metrica_2, metrica_3, metrica_4 = st.columns(4)
metrica_1.metric("Registros", f"{len(stock_df):,}")
metrica_2.metric("Artículos", f"{stock_df['codigo_articulo'].nunique():,}")
metrica_3.metric("Unidades de stock", f"{stock_total.sum():,.2f}")
metrica_4.metric("Valor a costo", f"$ {float((stock_total * costo_numerico).sum()):,.2f}")

st.subheader("Detalle de stock")
st.dataframe(
    stock_df,
    width="stretch",
    hide_index=True,
    height=620,
)

nombre_archivo = f"stock_sucursal_{sucursal}_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
st.download_button(
    "Descargar todos los datos en Excel",
    data=generar_excel(stock_df),
    file_name=nombre_archivo,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    type="primary",
)
st.caption(
    "La descarga incluye todas las columnas de src.base_stock_sucursal y el "
    "nombre del proveedor, sin limitarse a las filas visibles en pantalla."
)

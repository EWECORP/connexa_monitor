# -*- coding: utf-8 -*-
"""Carga masiva de parametros logisticos desde una planilla Excel."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO

import pandas as pd
import streamlit as st

from modules.db import get_diarco_engine
from modules.logistic_parameter_import import (
    apply_updates,
    preview_updates,
    read_and_validate_excel,
)
from modules.ui import render_header


st.set_page_config(
    page_title="Actualizacion de Parametros Logisticos",
    page_icon="📥",
    layout="wide",
)
render_header("Actualizacion de Parametros Logisticos")
st.caption(
    "Actualiza palletizado, cobertura y preparacion en base_productos_vigentes "
    "y base_stock_sucursal usando proveedor + sucursal + articulo."
)

st.info(
    "Mapeo: PISO PALLET → cajas por capa; ALTURA PALLET → cantidad de capas. "
    "Las demas columnas de la planilla se conservan, pero no se modifican en esta utilidad."
)

uploaded = st.file_uploader("Planilla Excel", type=["xlsx", "xlsm"])
if uploaded is None:
    st.stop()

file_bytes = uploaded.getvalue()
file_digest = sha256(file_bytes).hexdigest()
try:
    with st.spinner(
        "Leyendo y validando la planilla. En archivos grandes puede demorar unos minutos..."
    ):
        validation = read_and_validate_excel(BytesIO(file_bytes))
except Exception as exc:
    st.error(f"No fue posible validar la planilla: {exc}")
    st.stop()

if validation.duplicate_rows_removed:
    st.warning(
        f"Se quitaron {validation.duplicate_rows_removed} filas repetidas identicas."
    )

if not validation.errors.empty:
    st.error(f"Hay {len(validation.errors)} filas con errores. Corrija la planilla para continuar.")
    st.dataframe(validation.errors, width="stretch", hide_index=True)
    st.stop()

if validation.valid_rows.empty:
    st.warning("La planilla no contiene filas para actualizar.")
    st.stop()

try:
    engine = get_diarco_engine()
    with st.spinner(
        f"Comparando {len(validation.valid_rows):,} filas con CONNEXA..."
    ):
        preview = preview_updates(engine, validation.valid_rows)
except Exception as exc:
    st.error(f"No fue posible comparar la planilla con CONNEXA: {exc}")
    st.stop()

ready = preview["Estado"].eq("LISTO")
m1, m2, m3 = st.columns(3)
m1.metric("Filas validas", len(preview))
m2.metric("Listas para actualizar", int(ready.sum()))
m3.metric("Omitidas por clave", int((~ready).sum()))

st.subheader("Vista previa")
st.dataframe(preview, width="stretch", hide_index=True, height=500)

previous_result = st.session_state.get("ultima_actualizacion_logistica")
already_applied = bool(
    previous_result and previous_result.get("archivo") == file_digest
)
if already_applied:
    st.success(
        "Esta planilla ya fue procesada correctamente en esta sesion: "
        f"{previous_result['filas_aplicadas']} filas actualizadas en cada tabla."
    )

if not ready.all():
    st.warning(
        "Las filas que no tengan una unica coincidencia en ambas tablas se omitiran. "
        "Puede corregir la planilla antes de confirmar."
    )

confirmation = st.checkbox(
    "Confirmo que revise la vista previa y deseo actualizar ambas tablas",
    disabled=already_applied,
)
execute = st.button(
    "Aplicar actualizacion",
    type="primary",
    disabled=already_applied or not confirmation or not ready.any(),
)

if execute:
    try:
        with st.spinner(
            f"Actualizando {int(ready.sum()):,} filas en ambas tablas. "
            "No cierre esta pagina..."
        ):
            result = apply_updates(engine, validation.valid_rows)
    except Exception as exc:
        st.error(f"La actualizacion fue revertida: {exc}")
        st.stop()

    st.session_state["ultima_actualizacion_logistica"] = {
        **result,
        "archivo": file_digest,
    }
    st.success(
        f"Actualizacion completada: {result['filas_aplicadas']} filas en cada tabla."
    )

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_connexa_engine, get_diarco_engine, get_sqlserver_engine
from modules.ui import render_header
from modules.queries.transfer_monitor import (
    build_current_pending_snapshot,
    build_history_snapshot,
    load_supplier_dim,
)


st.set_page_config(
    page_title="Monitoreo Transferencias SGM / Valkimia",
    page_icon="🚚",
    layout="wide",
)

render_header("Monitoreo de Transferencias Connexa -> SGM -> Valkimia")

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))

HELP_BULTOS_BASE = (
    "Se calcula desde `src.base_stock_sucursal` como "
    "`floor((stock + transfer_pendiente) / factor_venta)`. "
    "No es stock fisico crudo: ya contempla el ajuste por `transfer_pendiente`."
)

HELP_RESERVA_ACO = (
    "Surge de Valkimia, tabla `VALKIMIA.dbo.IntNecIN`, sumando `INICnt1` "
    "para registros con `INIEst = 'ACO'`. En esta logica se imputa solo al CD 41."
)

HELP_STOCK_NETO = (
    "Se calcula como `Bultos base neteados - Reserva ACO VK`, con piso en 0. "
    "Es el saldo operativo que usa el flujo para decidir si una linea es publicable."
)


def _supplier_label(row: pd.Series) -> str:
    codigo = row.get("c_proveedor")
    nombre = str(row.get("n_proveedor") or "").strip()
    if pd.notna(codigo):
        try:
            codigo_txt = str(int(codigo))
        except Exception:
            codigo_txt = str(codigo)
    else:
        codigo_txt = "-"

    if nombre:
        return f"{nombre} ({codigo_txt})"
    return codigo_txt


def _count_mask(df: pd.DataFrame, mask: pd.Series) -> int:
    if df.empty:
        return 0
    return int(mask.fillna(False).sum())


def _sum_col(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _build_estado_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "estado_operativo" not in df.columns:
        return pd.DataFrame(columns=["estado_operativo", "cantidad"])

    out = (
        df["estado_operativo"]
        .fillna("SIN_ESTADO")
        .value_counts(dropna=False)
        .rename_axis("estado_operativo")
        .reset_index(name="cantidad")
    )
    return out


def _bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].fillna(False).astype(bool)
    return pd.Series(False, index=df.index, dtype="bool")


def _upper_text_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col].fillna("").astype(str).str.upper()
    return pd.Series("", index=df.index, dtype="object")


def _filter_by_supplier(df: pd.DataFrame, proveedor: int) -> pd.DataFrame:
    if df.empty or "c_proveedor" not in df.columns:
        return pd.DataFrame(columns=df.columns)

    mask = pd.to_numeric(df["c_proveedor"], errors="coerce") == int(proveedor)
    return df[mask].copy()


def _build_stock_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["linea_bloqueada"] = (~tmp["publicable"].fillna(False)) & (~tmp["ya_publicado"].fillna(False))
    tmp["linea_publicable"] = tmp["publicable_ahora"].fillna(False)

    out = (
        tmp.groupby(
            ["origin_cd_num", "item_code_num", "item_description"],
            dropna=False,
            as_index=False,
        )
        .agg(
            lineas_total=("connexa_detail_uuid", "count"),
            bultos_solicitados=("qty_requested_num", "sum"),
            bultos_asignados=("q_bultos_asignado", "sum"),
            lineas_publicables=("linea_publicable", "sum"),
            lineas_bloqueadas=("linea_bloqueada", "sum"),
            stock_fisico_unidades=("stock_unidades", "max"),
            transfer_pendiente_unidades=("transfer_pendiente_unidades", "max"),
            pedido_pendiente_unidades=("pedido_pendiente_unidades", "max"),
            factor_venta=("factor_venta", "max"),
            bultos_base_neteados=("q_bultos_disponible_base", "max"),
            reserva_aco_vk=("bultos_aco_valkimia", "max"),
            stock_neto_disponible=("q_bultos_disponible", "max"),
            saldo_final_grupo=("saldo_despues", "min"),
        )
        .sort_values(["lineas_bloqueadas", "bultos_solicitados"], ascending=[False, False])
    )

    return out


@st.cache_data(ttl=TTL, show_spinner=False)
def _load_suppliers() -> pd.DataFrame:
    pg_engine = get_diarco_engine()
    return load_supplier_dim(pg_engine)


@st.cache_data(ttl=TTL, show_spinner=True)
def _load_current_snapshot(proveedor: int) -> pd.DataFrame:
    connexa_engine = get_connexa_engine()
    pg_engine = get_diarco_engine()
    sql_engine = get_sqlserver_engine()

    if connexa_engine is None or pg_engine is None or sql_engine is None:
        return pd.DataFrame()

    return build_current_pending_snapshot(connexa_engine, pg_engine, sql_engine, proveedor)


@st.cache_data(ttl=TTL, show_spinner=True)
def _load_history_snapshot(proveedor: int, desde: date, hasta: date) -> pd.DataFrame:
    connexa_engine = get_connexa_engine()
    pg_engine = get_diarco_engine()
    sql_engine = get_sqlserver_engine()

    if connexa_engine is None or pg_engine is None or sql_engine is None:
        return pd.DataFrame()

    return build_history_snapshot(connexa_engine, pg_engine, sql_engine, desde, hasta, proveedor)


st.markdown(
    """
Panel operativo para cruzar:
1. transferencias generadas en Connexa,
2. disponibilidad actual en CD,
3. staging / publicación en SGM,
4. último estado informado por Valkimia.
"""
)

st.info(
    "Criterio de proveedor: el filtro se asigna desde `src.base_productos_vigentes` "
    "usando `articulo + sucursal destino` para mapear el proveedor primario."
)

suppliers = _load_suppliers()
if suppliers.empty:
    st.warning("No se pudo cargar la dimensión de proveedores para transferencias.")
    st.stop()

supplier_options = suppliers.copy()
supplier_options["label"] = supplier_options.apply(_supplier_label, axis=1)
label_to_supplier = dict(zip(supplier_options["label"], supplier_options["c_proveedor"]))

default_from = date.today() - timedelta(days=30)
f1, f2, f3 = st.columns([2, 1, 1])
with f1:
    proveedor_label = st.selectbox("Proveedor", sorted(label_to_supplier.keys()))
with f2:
    desde = st.date_input("Desde", value=default_from)
with f3:
    hasta = st.date_input("Hasta", value=date.today())

proveedor_sel = label_to_supplier[proveedor_label]

current_snapshot = _load_current_snapshot(int(proveedor_sel))
history_snapshot = _load_history_snapshot(int(proveedor_sel), desde, hasta)

if current_snapshot.empty and history_snapshot.empty:
    st.warning("No se pudieron cargar datos del circuito de transferencias con las conexiones actuales.")
    st.stop()

df_current = current_snapshot.copy()
df_history = history_snapshot.copy()

df_current["requested_at"] = pd.to_datetime(df_current.get("requested_at"), errors="coerce")
df_history["requested_at"] = pd.to_datetime(df_history.get("requested_at"), errors="coerce")

stock_summary = _build_stock_summary(df_current)
estado_current = _build_estado_counts(df_current)
estado_history = _build_estado_counts(df_history)

current_blocked_mask = (~_bool_series(df_current, "publicable")) & (~_bool_series(df_current, "ya_publicado"))
current_dmz_mask = _bool_series(df_current, "ya_publicado")
current_vk_ok_mask = (
    _upper_text_series(df_current, "vk_INIEst").eq("ACO")
    | _upper_text_series(df_current, "dmz_estado_vk").eq("PROCESADO")
)

h_dmz_ok_mask = _upper_text_series(df_history, "dmz_estado").eq("PROCESADO")
h_dmz_error_mask = _upper_text_series(df_history, "dmz_estado").eq("ERROR")
h_dmz_dup_mask = _upper_text_series(df_history, "dmz_estado").eq("DUPLICADO")
h_vk_err_mask = _upper_text_series(df_history, "dmz_estado_vk").eq("ERROR")

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Líneas actuales en PRECARGA", f"{len(df_current):,}")
c2.metric("Bultos actuales", f"{_sum_col(df_current, 'qty_requested_num'):,.0f}")
c3.metric("Publicables ahora", f"{_count_mask(df_current, _bool_series(df_current, 'publicable_ahora')):,}")
c4.metric("Bloqueadas por stock", f"{_count_mask(df_current, current_blocked_mask):,}")
c5.metric("Ya insertadas en DMZ", f"{_count_mask(df_current, current_dmz_mask):,}")
c6.metric("Con traza VK", f"{_count_mask(df_current, current_vk_ok_mask):,}")

h1, h2, h3, h4, h5 = st.columns(5)
h1.metric("Líneas en rango", f"{len(df_history):,}")
h2.metric("SGM procesadas", f"{_count_mask(df_history, h_dmz_ok_mask):,}")
h3.metric("SGM con error", f"{_count_mask(df_history, h_dmz_error_mask):,}")
h4.metric("SGM duplicadas", f"{_count_mask(df_history, h_dmz_dup_mask):,}")
h5.metric("VK con error", f"{_count_mask(df_history, h_vk_err_mask):,}")

tab_resumen, tab_actual, tab_hist = st.tabs(
    ["Resumen", "Pipeline actual", "Seguimiento por rango"]
)

with tab_resumen:
    g1, g2 = st.columns(2)

    with g1:
        st.subheader("Estados del pipeline actual")
        if estado_current.empty:
            st.info("No hay líneas actuales en `PRECARGA_CONNEXA` para este proveedor.")
        else:
            fig = px.bar(
                estado_current,
                x="cantidad",
                y="estado_operativo",
                orientation="h",
                text="cantidad",
                title="Estado operativo actual",
            )
            fig.update_layout(height=380, yaxis_title="", xaxis_title="Cantidad de líneas")
            st.plotly_chart(fig, use_container_width=True)

    with g2:
        st.subheader("Estados en el rango seleccionado")
        if estado_history.empty:
            st.info("No hay historial para el rango seleccionado.")
        else:
            fig = px.bar(
                estado_history,
                x="cantidad",
                y="estado_operativo",
                orientation="h",
                text="cantidad",
                title="Estado operativo por rango",
            )
            fig.update_layout(height=380, yaxis_title="", xaxis_title="Cantidad de líneas")
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Foto de stock actual en CD para las líneas pendientes")
    if stock_summary.empty:
        st.info("No hay líneas pendientes actuales para construir la foto de stock.")
    else:
        st.dataframe(
            stock_summary.rename(
                columns={
                    "origin_cd_num": "CD origen",
                    "item_code_num": "Artículo",
                    "item_description": "Descripción",
                    "lineas_total": "Líneas",
                    "bultos_solicitados": "Bultos solicitados",
                    "bultos_asignados": "Bultos asignados",
                    "lineas_publicables": "Líneas publicables",
                    "lineas_bloqueadas": "Líneas bloqueadas",
                    "stock_fisico_unidades": "Stock físico",
                    "transfer_pendiente_unidades": "Transfer pendiente",
                    "pedido_pendiente_unidades": "Pedido pendiente",
                    "factor_venta": "Factor venta",
                    "bultos_base_neteados": "Bultos base neteados",
                    "reserva_aco_vk": "Reserva ACO VK",
                    "stock_neto_disponible": "Stock neto disponible",
                    "saldo_final_grupo": "Saldo final grupo",
                }
            ),
            column_config={
                "Bultos base neteados": st.column_config.NumberColumn(
                    "Bultos base neteados",
                    help=HELP_BULTOS_BASE,
                ),
                "Reserva ACO VK": st.column_config.NumberColumn(
                    "Reserva ACO VK",
                    help=HELP_RESERVA_ACO,
                ),
                "Stock neto disponible": st.column_config.NumberColumn(
                    "Stock neto disponible",
                    help=HELP_STOCK_NETO,
                ),
            },
            use_container_width=True,
            hide_index=True,
        )

with tab_actual:
    st.subheader("Detalle actual en Connexa con disponibilidad y estados downstream")

    if df_current.empty:
        st.info("No hay líneas actuales en `PRECARGA_CONNEXA` para este proveedor.")
    else:
        df_show = df_current.sort_values(
            ["requested_at", "connexa_purchase_code", "item_code_num"],
            ascending=[False, False, True],
        ).copy()

        columnas = [
            "estado_operativo",
            "status_code",
            "connexa_purchase_code",
            "requested_at",
            "origin_cd_num",
            "dest_store_num",
            "item_code_num",
            "item_description",
            "qty_requested_num",
            "stock_unidades",
            "transfer_pendiente_unidades",
            "pedido_pendiente_unidades",
            "factor_venta",
            "q_bultos_disponible_base",
            "bultos_aco_valkimia",
            "q_bultos_disponible",
            "saldo_antes",
            "saldo_despues",
            "q_bultos_asignado",
            "publicable_ahora",
            "motivo_no_publicado",
            "dmz_estado",
            "dmz_mensaje_error",
            "dmz_estado_vk",
            "dmz_mensaje_error_vk",
            "dmz_u_id_sincro",
            "vk_INIEst",
            "vk_INIFecEst",
            "connexa_header_uuid",
            "connexa_detail_uuid",
        ]
        columnas = [c for c in columnas if c in df_show.columns]

        st.dataframe(
            df_show[columnas].rename(
                columns={
                    "estado_operativo": "Estado operativo",
                    "status_code": "Estado Connexa",
                    "connexa_purchase_code": "Código transferencia",
                    "requested_at": "Fecha pedido",
                    "origin_cd_num": "CD origen",
                    "dest_store_num": "Sucursal destino",
                    "item_code_num": "Artículo",
                    "item_description": "Descripción",
                    "qty_requested_num": "Bultos solicitados",
                    "stock_unidades": "Stock físico",
                    "transfer_pendiente_unidades": "Transfer pendiente",
                    "pedido_pendiente_unidades": "Pedido pendiente",
                    "factor_venta": "Factor venta",
                    "q_bultos_disponible_base": "Bultos base neteados",
                    "bultos_aco_valkimia": "Reserva ACO VK",
                    "q_bultos_disponible": "Stock neto disponible",
                    "saldo_antes": "Saldo antes",
                    "saldo_despues": "Saldo después",
                    "q_bultos_asignado": "Bultos asignados",
                    "publicable_ahora": "Publicable ahora",
                    "motivo_no_publicado": "Motivo no publicado",
                    "dmz_estado": "Estado SGM/DMZ",
                    "dmz_mensaje_error": "Error SGM/DMZ",
                    "dmz_estado_vk": "Estado VK en DMZ",
                    "dmz_mensaje_error_vk": "Error VK en DMZ",
                    "dmz_u_id_sincro": "Id sincro",
                    "vk_INIEst": "Último estado VK",
                    "vk_INIFecEst": "Fecha estado VK",
                    "connexa_header_uuid": "Header UUID",
                    "connexa_detail_uuid": "Detail UUID",
                }
            ),
            column_config={
                "Bultos base neteados": st.column_config.NumberColumn(
                    "Bultos base neteados",
                    help=HELP_BULTOS_BASE,
                ),
                "Reserva ACO VK": st.column_config.NumberColumn(
                    "Reserva ACO VK",
                    help=HELP_RESERVA_ACO,
                ),
                "Stock neto disponible": st.column_config.NumberColumn(
                    "Stock neto disponible",
                    help=HELP_STOCK_NETO,
                ),
            },
            use_container_width=True,
            hide_index=True,
        )

with tab_hist:
    st.subheader("Seguimiento end-to-end en el rango seleccionado")

    if df_history.empty:
        st.info("No hay líneas históricas para este proveedor en el rango seleccionado.")
    else:
        df_hist_show = df_history.sort_values(
            ["requested_at", "connexa_purchase_code", "item_code_num"],
            ascending=[False, False, True],
        ).copy()

        columnas = [
            "estado_operativo",
            "status_code",
            "connexa_purchase_code",
            "requested_at",
            "origin_cd_num",
            "dest_store_num",
            "item_code_num",
            "item_description",
            "qty_requested_num",
            "qty_shipped_num",
            "qty_received_num",
            "dmz_estado",
            "dmz_mensaje_error",
            "dmz_estado_vk",
            "dmz_mensaje_error_vk",
            "dmz_u_id_sincro",
            "vk_INIEst",
            "vk_INIFecReg",
            "vk_INIFecEst",
            "vk_INIMotPed",
            "connexa_header_uuid",
            "connexa_detail_uuid",
        ]
        columnas = [c for c in columnas if c in df_hist_show.columns]

        st.dataframe(
            df_hist_show[columnas].rename(
                columns={
                    "estado_operativo": "Estado operativo",
                    "status_code": "Estado Connexa",
                    "connexa_purchase_code": "Código transferencia",
                    "requested_at": "Fecha pedido",
                    "origin_cd_num": "CD origen",
                    "dest_store_num": "Sucursal destino",
                    "item_code_num": "Artículo",
                    "item_description": "Descripción",
                    "qty_requested_num": "Bultos solicitados",
                    "qty_shipped_num": "Bultos enviados",
                    "qty_received_num": "Bultos recibidos",
                    "dmz_estado": "Estado SGM/DMZ",
                    "dmz_mensaje_error": "Error SGM/DMZ",
                    "dmz_estado_vk": "Estado VK en DMZ",
                    "dmz_mensaje_error_vk": "Error VK en DMZ",
                    "dmz_u_id_sincro": "Id sincro",
                    "vk_INIEst": "Último estado VK",
                    "vk_INIFecReg": "Fecha registro VK",
                    "vk_INIFecEst": "Fecha estado VK",
                    "vk_INIMotPed": "Motivo VK",
                    "connexa_header_uuid": "Header UUID",
                    "connexa_detail_uuid": "Detail UUID",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_connexa_engine, get_diarco_engine, get_sqlserver_engine
from modules.ui import render_header
from modules.queries.transfer_monitor import (
    build_current_pending_control_snapshot,
    build_current_pending_snapshot,
    build_history_control_snapshot,
    build_history_snapshot,
    load_supplier_dim,
    load_vk_user_transfer_range,
    load_vk_header_status,
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


def _format_supplier_name(nombre: object, codigo: object) -> str:
    nombre_txt = str(nombre or "").strip()
    if pd.notna(codigo):
        try:
            codigo_txt = str(int(codigo))
        except Exception:
            codigo_txt = str(codigo).strip()
    else:
        codigo_txt = ""

    if nombre_txt and codigo_txt:
        return f"{nombre_txt} ({codigo_txt})"
    if nombre_txt:
        return nombre_txt
    if codigo_txt:
        return codigo_txt
    return "-"


def _count_mask(df: pd.DataFrame, mask: pd.Series) -> int:
    if df.empty:
        return 0
    return int(mask.fillna(False).sum())


def _sum_col(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _distinct_count(df: pd.DataFrame, col: str, mask: pd.Series | None = None) -> int:
    if df.empty or col not in df.columns:
        return 0

    work = df
    if mask is not None:
        work = df[mask.fillna(False)].copy()

    if work.empty:
        return 0

    return int(work[col].dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique())


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


def _build_pipeline_stage_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["etapa", "cabeceras", "lineas"])

    dmz_mask = _upper_text_series(df, "dmz_estado").ne("")
    sgm_ok_mask = _upper_text_series(df, "dmz_estado").isin(["PROCESADO", "DUPLICADO"])
    vk_sent_mask = _upper_text_series(df, "dmz_estado_vk").ne("") | _upper_text_series(df, "vk_INIEst").ne("")
    vk_trace_mask = _upper_text_series(df, "vk_INIEst").ne("")

    etapas = [
        {
            "etapa": "Generadas en Connexa",
            "cabeceras": _distinct_count(df, "connexa_header_uuid"),
            "lineas": len(df),
        },
        {
            "etapa": "Cargadas en DMZ / Pend. SGM",
            "cabeceras": _distinct_count(df, "connexa_header_uuid", dmz_mask),
            "lineas": _count_mask(df, dmz_mask),
        },
        {
            "etapa": "Procesadas por SGM",
            "cabeceras": _distinct_count(df, "connexa_header_uuid", sgm_ok_mask),
            "lineas": _count_mask(df, sgm_ok_mask),
        },
        {
            "etapa": "Enviadas a Valkimia",
            "cabeceras": _distinct_count(df, "connexa_header_uuid", vk_sent_mask),
            "lineas": _count_mask(df, vk_sent_mask),
        },
        {
            "etapa": "Con retorno de Valkimia",
            "cabeceras": _distinct_count(df, "connexa_header_uuid", vk_trace_mask),
            "lineas": _count_mask(df, vk_trace_mask),
        },
    ]
    return pd.DataFrame(etapas)


def _build_vk_result_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "resultado" not in df.columns:
        return pd.DataFrame(columns=["resultado", "cabeceras"])

    out = (
        df.assign(resultado=df["resultado"].fillna("").astype(str).str.strip().replace("", "SIN_RESULTADO"))
        ["resultado"]
        .value_counts(dropna=False)
        .rename_axis("resultado")
        .reset_index(name="cabeceras")
    )
    return out


def _first_non_empty(values: pd.Series) -> str:
    for value in values.tolist():
        if pd.isna(value):
            continue
        txt = str(value).strip()
        if txt:
            return txt
    return ""


def _build_vk_header_display(df_vk_headers: pd.DataFrame, df_history: pd.DataFrame) -> pd.DataFrame:
    if df_vk_headers.empty:
        return df_vk_headers.copy()

    out = df_vk_headers.copy()
    if df_history.empty or "connexa_header_uuid" not in df_history.columns:
        out["proveedor_label"] = "-"
        out["sucursal_label"] = "-"
        return out

    cols = [c for c in ["connexa_header_uuid", "n_proveedor", "c_proveedor", "dest_store_num"] if c in df_history.columns]
    if "connexa_header_uuid" not in cols:
        out["proveedor_label"] = "-"
        out["sucursal_label"] = "-"
        return out

    base = df_history[cols].copy()
    grouped = (
        base.groupby("connexa_header_uuid", dropna=False, as_index=False)
        .agg(
            n_proveedor=("n_proveedor", _first_non_empty) if "n_proveedor" in base.columns else ("connexa_header_uuid", lambda s: ""),
            c_proveedor=("c_proveedor", "min") if "c_proveedor" in base.columns else ("connexa_header_uuid", lambda s: pd.NA),
            dest_store_num=("dest_store_num", "min") if "dest_store_num" in base.columns else ("connexa_header_uuid", lambda s: pd.NA),
        )
    )

    grouped["proveedor_label"] = grouped.apply(
        lambda row: _format_supplier_name(row.get("n_proveedor"), row.get("c_proveedor")),
        axis=1,
    )
    grouped["sucursal_label"] = (
        pd.to_numeric(grouped.get("dest_store_num"), errors="coerce")
        .astype("Int64")
        .astype(str)
        .replace("<NA>", "-")
    )

    out = out.merge(
        grouped[["connexa_header_uuid", "proveedor_label", "sucursal_label"]],
        how="left",
        on="connexa_header_uuid",
    )
    out["proveedor_label"] = out["proveedor_label"].fillna("-").astype(str).str.strip()
    out["sucursal_label"] = out["sucursal_label"].fillna("-").astype(str).str.strip()
    return out


def _build_weekly_user_transfer_summary(df: pd.DataFrame, desde: date, hasta: date) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Usuario", "Origen", "Total"])

    work = df.copy()
    work["usuario_label"] = work["c_usuario"].fillna("SIN_USUARIO").astype(str).str.strip().str.upper()
    work.loc[work["usuario_label"].eq(""), "usuario_label"] = "SIN_USUARIO"
    work["origen_label"] = work["usuario_label"].eq("CONNEXA").map({True: "Connexa", False: "Usuario"})

    week_start = pd.to_datetime(work["f_alta"], errors="coerce")
    week_start = week_start.dt.normalize() - pd.to_timedelta(week_start.dt.weekday.fillna(0), unit="D")
    work["week_start"] = week_start

    week_from = pd.Timestamp(desde) - pd.to_timedelta(pd.Timestamp(desde).weekday(), unit="D")
    week_to = pd.Timestamp(hasta) - pd.to_timedelta(pd.Timestamp(hasta).weekday(), unit="D")
    week_range = pd.date_range(start=week_from, end=week_to, freq="7D")

    pivot = (
        work.pivot_table(
            index=["usuario_label", "origen_label"],
            columns="week_start",
            values="transfer_key",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
    )

    for week in week_range:
        if week not in pivot.columns:
            pivot[week] = 0

    week_columns = list(week_range)
    pivot["Total"] = pivot[week_columns].sum(axis=1)
    pivot = pivot.rename(columns={"usuario_label": "Usuario", "origen_label": "Origen"})

    rename_map = {week: f"Sem {week.strftime('%d/%m')}" for week in week_columns}
    pivot = pivot.rename(columns=rename_map)

    ordered_cols = ["Usuario", "Origen", "Total"] + [rename_map[week] for week in week_columns]
    pivot = pivot[ordered_cols].sort_values(["Origen", "Total", "Usuario"], ascending=[True, False, True]).reset_index(drop=True)
    return pivot


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
def _load_current_control_snapshot() -> pd.DataFrame:
    connexa_engine = get_connexa_engine()
    pg_engine = get_diarco_engine()
    sql_engine = get_sqlserver_engine()

    if connexa_engine is None or pg_engine is None or sql_engine is None:
        return pd.DataFrame()

    return build_current_pending_control_snapshot(connexa_engine, pg_engine, sql_engine)


@st.cache_data(ttl=TTL, show_spinner=True)
def _load_history_snapshot(proveedor: int, desde: date, hasta: date) -> pd.DataFrame:
    connexa_engine = get_connexa_engine()
    pg_engine = get_diarco_engine()
    sql_engine = get_sqlserver_engine()

    if connexa_engine is None or pg_engine is None or sql_engine is None:
        return pd.DataFrame()

    return build_history_snapshot(connexa_engine, pg_engine, sql_engine, desde, hasta, proveedor)


@st.cache_data(ttl=TTL, show_spinner=True)
def _load_history_control_snapshot(desde: date, hasta: date) -> pd.DataFrame:
    connexa_engine = get_connexa_engine()
    pg_engine = get_diarco_engine()
    sql_engine = get_sqlserver_engine()

    if connexa_engine is None or pg_engine is None or sql_engine is None:
        return pd.DataFrame()

    return build_history_control_snapshot(connexa_engine, pg_engine, sql_engine, desde, hasta)


@st.cache_data(ttl=TTL, show_spinner=False)
def _load_vk_header_summary(header_uuids: tuple[str, ...]) -> pd.DataFrame:
    sql_engine = get_sqlserver_engine()
    if sql_engine is None or not header_uuids:
        return pd.DataFrame()

    return load_vk_header_status(sql_engine, list(header_uuids))


@st.cache_data(ttl=TTL, show_spinner=False)
def _load_vk_user_transfer_summary(desde: date, hasta: date) -> pd.DataFrame:
    sql_engine = get_sqlserver_engine()
    if sql_engine is None:
        return pd.DataFrame()

    return load_vk_user_transfer_range(sql_engine, desde, hasta)


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
f1, f2 = st.columns(2)
with f1:
    desde = st.date_input("Desde", value=default_from)
with f2:
    hasta = st.date_input("Hasta", value=date.today())

global_current = _load_current_control_snapshot()
global_history = _load_history_control_snapshot(desde, hasta)

header_ids = tuple(
    sorted(
        {
            str(x).strip().lower()
            for x in global_history.get("connexa_header_uuid", pd.Series(dtype="object")).dropna().tolist()
            if str(x).strip()
        }
    )
)
vk_header_summary = _load_vk_header_summary(header_ids)
vk_header_display = _build_vk_header_display(vk_header_summary, global_history)
vk_user_transfers = _load_vk_user_transfer_summary(desde, hasta)
weekly_user_summary = _build_weekly_user_transfer_summary(vk_user_transfers, desde, hasta)

pipeline_counts = _build_pipeline_stage_counts(global_history)
vk_result_counts = _build_vk_result_counts(vk_header_summary)

st.subheader("Resumen general del pipeline")
st.caption(
    f"Rango analizado: {desde.strftime('%d/%m/%Y')} al {hasta.strftime('%d/%m/%Y')}."
)
st.info(
    "Este bloque resume transferencias originadas en Connexa y su traza aguas abajo "
    "en DMZ / SGM / Valkimia. No incluye transferencias cargadas manualmente por usuarios en SGM."
)

g1, g2, g3, g4, g5, g6 = st.columns(6)
g1.metric("Cabeceras Connexa", f"{_distinct_count(global_history, 'connexa_header_uuid'):,}")
g2.metric("Líneas Connexa", f"{len(global_history):,}")
g3.metric("Cabeceras en DMZ", f"{_distinct_count(global_history, 'connexa_header_uuid', _upper_text_series(global_history, 'dmz_estado').ne('')):,}")
g4.metric("Procesadas SGM", f"{_distinct_count(global_history, 'connexa_header_uuid', _upper_text_series(global_history, 'dmz_estado').isin(['PROCESADO', 'DUPLICADO'])):,}")
g5.metric("Con traza VK", f"{_distinct_count(global_history, 'connexa_header_uuid', _upper_text_series(global_history, 'vk_INIEst').ne('')):,}")
g6.metric("Cabeceras cerrables VK", f"{_count_mask(vk_header_summary, _bool_series(vk_header_summary, 'cerrable')):,}")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Pendientes actuales en PRECARGA", f"{_distinct_count(global_current, 'connexa_header_uuid'):,}")
c2.metric("Líneas actuales en PRECARGA", f"{len(global_current):,}")
c3.metric("Líneas actuales ya en DMZ", f"{_count_mask(global_current, _bool_series(global_current, 'ya_publicado')):,}")
c4.metric("Líneas actuales con traza VK", f"{_count_mask(global_current, _upper_text_series(global_current, 'vk_INIEst').ne('')):,}")

st.markdown("**Transferencias registradas en Valkimia por usuario**")
u1, u2, u3, u4 = st.columns(4)
u1.metric("Transferencias en período", f"{len(vk_user_transfers):,}")
u2.metric(
    "Generadas por Connexa",
    f"{_count_mask(vk_user_transfers, _upper_text_series(vk_user_transfers, 'c_usuario').eq('CONNEXA')):,}",
)
u3.metric(
    "Generadas por usuarios",
    f"{_count_mask(vk_user_transfers, _upper_text_series(vk_user_transfers, 'c_usuario').ne('CONNEXA')):,}",
)
u4.metric(
    "Usuarios manuales activos",
    f"{_distinct_count(vk_user_transfers[~_upper_text_series(vk_user_transfers, 'c_usuario').eq('CONNEXA')], 'c_usuario'):,}",
)

if weekly_user_summary.empty:
    st.info("No se encontraron transferencias registradas en Valkimia por usuario en el período seleccionado.")
else:
    st.dataframe(
        weekly_user_summary,
        use_container_width=True,
        hide_index=True,
    )

sum1, sum2 = st.columns(2)
with sum1:
    st.markdown("**Pipeline por etapa**")
    if pipeline_counts.empty:
        st.info("No hay transferencias Connexa en el rango seleccionado.")
    else:
        fig = px.bar(
            pipeline_counts,
            x="cabeceras",
            y="etapa",
            orientation="h",
            text="cabeceras",
            title="Cabeceras por etapa",
        )
        fig.update_layout(height=360, yaxis_title="", xaxis_title="Cabeceras")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            pipeline_counts.rename(columns={"etapa": "Etapa", "cabeceras": "Cabeceras", "lineas": "Líneas"}),
            use_container_width=True,
            hide_index=True,
        )

with sum2:
    st.markdown("**Resultado por cabecera en Valkimia**")
    if vk_result_counts.empty:
        st.info("No se encontraron retornos por cabecera en Valkimia para el rango seleccionado.")
    else:
        fig = px.bar(
            vk_result_counts,
            x="cabeceras",
            y="resultado",
            orientation="h",
            text="cabeceras",
            title="Resultado VK por cabecera",
        )
        fig.update_layout(height=360, yaxis_title="", xaxis_title="Cabeceras")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(
            vk_header_display.rename(
                columns={
                    "proveedor_label": "Proveedor",
                    "sucursal_label": "Sucursal destino",
                    "total_lineas": "Total líneas",
                    "cant_aco": "ACO",
                    "cant_pre": "PRE",
                    "cant_rem": "REM",
                    "cant_etr": "ETR",
                    "cant_otro": "Otros",
                    "resultado": "Resultado",
                    "cerrable": "Cerrable",
                }
            )[
                [
                    "Proveedor",
                    "Sucursal destino",
                    "Total líneas",
                    "ACO",
                    "PRE",
                    "REM",
                    "ETR",
                    "Otros",
                    "Resultado",
                    "Cerrable",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

st.divider()
st.subheader("Control por proveedor")

proveedor_label = st.selectbox("Proveedor", sorted(label_to_supplier.keys()))
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

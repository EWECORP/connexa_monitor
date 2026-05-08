from __future__ import annotations

import os
import uuid
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from modules.db import get_connexa_engine, get_diarco_engine, get_sqlserver_engine
from modules.ui import render_header
from modules.queries.transfer_monitor import build_current_pending_control_snapshot, upsert_transfer_blocklist


st.set_page_config(
    page_title="Control Transferencias Abril / VKM",
    page_icon="🧭",
    layout="wide",
)

render_header("Control de Transferencias Pendientes y Multiples para VKM")

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))


def _default_from() -> date:
    today = date.today()
    first_this_month = today.replace(day=1)
    return (first_this_month - timedelta(days=1)).replace(day=1)


def _count_mask(df: pd.DataFrame, mask: pd.Series) -> int:
    if df.empty:
        return 0
    return int(mask.fillna(False).sum())


def _parse_uuid_list(raw: str) -> list[str]:
    values: list[str] = []
    for part in raw.replace(",", "\n").splitlines():
        candidate = part.strip().lower()
        if not candidate:
            continue
        try:
            values.append(str(uuid.UUID(candidate)))
        except Exception:
            continue
    return sorted(set(values))


def _normalize_control(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["requested_at"] = pd.to_datetime(out.get("requested_at"), errors="coerce")
    out["created_at"] = pd.to_datetime(out.get("created_at"), errors="coerce")
    out["control_ts"] = out["requested_at"].fillna(out["created_at"])
    out["qty_requested_num"] = pd.to_numeric(out.get("qty_requested_num"), errors="coerce").fillna(0.0)
    out["c_proveedor"] = pd.to_numeric(out.get("c_proveedor"), errors="coerce").astype("Int64")
    out["origin_cd_num"] = pd.to_numeric(out.get("origin_cd_num"), errors="coerce").astype("Int64")
    out["dest_store_num"] = pd.to_numeric(out.get("dest_store_num"), errors="coerce").astype("Int64")
    out["item_code_num"] = pd.to_numeric(out.get("item_code_num"), errors="coerce").astype("Int64")
    out["n_proveedor"] = out.get("n_proveedor", "").fillna("SIN_PROVEEDOR_MAPEADO").astype(str).str.strip()
    out.loc[out["n_proveedor"].eq(""), "n_proveedor"] = "SIN_PROVEEDOR_MAPEADO"
    out["ya_publicado"] = out.get("ya_publicado", False).fillna(False).astype(bool)
    out["bloqueada_manual"] = out.get("bloqueada_manual", False).fillna(False).astype(bool)
    out["bloqueo_motivo"] = out.get("bloqueo_motivo", "").fillna("").astype(str).str.strip()
    out["bloqueo_usuario"] = out.get("bloqueo_usuario", "").fillna("").astype(str).str.strip()
    out["bloqueo_observacion"] = out.get("bloqueo_observacion", "").fillna("").astype(str).str.strip()
    out["bloqueo_created_at"] = pd.to_datetime(out.get("bloqueo_created_at"), errors="coerce")
    return out


def _apply_control_logic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    grp_cols = ["c_proveedor", "origin_cd_num", "dest_store_num", "item_code_num"]
    work = df.copy()
    work["linea_sin_enviar"] = ~work["ya_publicado"]

    work = work.sort_values(
        by=grp_cols + ["control_ts", "connexa_detail_uuid"],
        ascending=[True, True, True, True, False, False],
        kind="mergesort",
    ).copy()

    work["rank_control"] = work.groupby(grp_cols, dropna=False).cumcount() + 1
    work["grupo_duplicado"] = work.groupby(grp_cols, dropna=False)["connexa_detail_uuid"].transform("count") > 1
    work["es_candidata_vigente"] = work["rank_control"].eq(1)

    def _accion(row: pd.Series) -> str:
        if bool(row.get("bloqueada_manual", False)):
            return "BLOQUEADA_MANUALMENTE"
        if bool(row.get("ya_publicado", False)):
            return "YA_ENVIADA_O_INSERTADA"
        if bool(row.get("grupo_duplicado", False)) and bool(row.get("es_candidata_vigente", False)):
            return "REVISAR_CANDIDATA_VIGENTE"
        if bool(row.get("grupo_duplicado", False)):
            return "NO_ENVIAR_SIN_VALIDAR"
        return "PENDIENTE_UNICA"

    work["accion_sugerida"] = work.apply(_accion, axis=1)
    return work


def _build_provider_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    base_keys = ["c_proveedor", "n_proveedor"]
    grupos_dup = (
        df[df["grupo_duplicado"]]
        .drop_duplicates(subset=base_keys + ["origin_cd_num", "dest_store_num", "item_code_num"])
        .groupby(base_keys, dropna=False)
        .size()
        .rename("grupos_duplicados")
        .reset_index()
    )

    resumen = (
        df.groupby(base_keys, dropna=False, as_index=False)
        .agg(
            lineas_pendientes=("connexa_detail_uuid", "count"),
            lineas_sin_enviar=("linea_sin_enviar", "sum"),
            lineas_ya_publicadas=("ya_publicado", "sum"),
            lineas_bloqueadas=("bloqueada_manual", "sum"),
            candidatas_vigentes=("es_candidata_vigente", "sum"),
            bultos_pendientes=("qty_requested_num", "sum"),
            primera_fecha=("control_ts", "min"),
            ultima_fecha=("control_ts", "max"),
        )
    )

    resumen = resumen.merge(grupos_dup, how="left", on=base_keys)
    resumen["grupos_duplicados"] = resumen["grupos_duplicados"].fillna(0).astype(int)
    resumen = resumen.sort_values(
        ["lineas_sin_enviar", "grupos_duplicados", "bultos_pendientes"],
        ascending=[False, False, False],
    )
    return resumen


def _build_duplicate_groups(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    dup = df[df["grupo_duplicado"]].copy()
    if dup.empty:
        return pd.DataFrame()

    grp_cols = ["c_proveedor", "n_proveedor", "origin_cd_num", "dest_store_num", "item_code_num"]
    out = (
        dup.groupby(grp_cols, dropna=False, as_index=False)
        .agg(
            descripcion=("item_description", "first"),
            lineas=("connexa_detail_uuid", "count"),
            headers_distintos=("connexa_header_uuid", "nunique"),
            bultos_totales=("qty_requested_num", "sum"),
            bultos_maximos=("qty_requested_num", "max"),
            primera_fecha=("control_ts", "min"),
            ultima_fecha=("control_ts", "max"),
            ya_publicadas=("ya_publicado", "sum"),
            bloqueadas=("bloqueada_manual", "sum"),
        )
    )

    candidatas = (
        dup[dup["es_candidata_vigente"]]
        .loc[:, grp_cols + ["connexa_detail_uuid", "connexa_header_uuid", "qty_requested_num", "accion_sugerida"]]
        .rename(
            columns={
                "connexa_detail_uuid": "detail_uuid_candidata",
                "connexa_header_uuid": "header_uuid_candidata",
                "qty_requested_num": "bultos_candidata",
                "accion_sugerida": "accion_sugerida",
            }
        )
    )

    out = out.merge(candidatas, how="left", on=grp_cols)
    out = out.sort_values(["lineas", "ultima_fecha", "bultos_totales"], ascending=[False, False, False])
    return out


def _build_block_rows(df: pd.DataFrame, selected_uuids: list[str], motivo: str, usuario: str, observacion: str) -> list[dict]:
    if df.empty or not selected_uuids:
        return []

    subset = df[df["connexa_detail_uuid"].isin(selected_uuids)].copy()
    if subset.empty:
        return []

    rows: list[dict] = []
    for _, row in subset.iterrows():
        rows.append(
            {
                "connexa_detail_uuid": row.get("connexa_detail_uuid"),
                "connexa_header_uuid": row.get("connexa_header_uuid"),
                "motivo": motivo,
                "usuario": usuario,
                "observacion": observacion,
                "active": True,
            }
        )
    return rows


@st.cache_data(ttl=TTL, show_spinner=True)
def _load_control_snapshot() -> pd.DataFrame:
    connexa_engine = get_connexa_engine()
    pg_engine = get_diarco_engine()
    sql_engine = get_sqlserver_engine()

    if connexa_engine is None or pg_engine is None or sql_engine is None:
        return pd.DataFrame()

    return build_current_pending_control_snapshot(connexa_engine, pg_engine, sql_engine)


st.markdown(
    """
Control pensado para saneamiento post-parada de sincronizacion.
Ayuda a identificar:

- proveedores con lineas pendientes sin enviar,
- grupos con multiples transferencias para la misma combinacion,
- linea candidata vigente para revisar antes de publicar en VKM.
"""
)

raw = _load_control_snapshot()
if raw.empty:
    st.warning("No se pudieron cargar transferencias pendientes desde Connexa / DMZ / VKM.")
    st.stop()

df = _normalize_control(raw)

col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
with col1:
    desde = st.date_input("Desde", value=_default_from())
with col2:
    solo_sin_enviar = st.checkbox("Solo sin enviar", value=True)
with col3:
    solo_duplicadas = st.checkbox("Solo grupos duplicados", value=False)

df = df[df["control_ts"].dt.date >= desde].copy()

proveedores_df = (
    df[["c_proveedor", "n_proveedor"]]
    .dropna(subset=["c_proveedor"])
    .drop_duplicates()
    .sort_values(["n_proveedor", "c_proveedor"])
)

provider_options = {"Todos": None}
for _, row in proveedores_df.iterrows():
    provider_options[f"{row['n_proveedor']} ({int(row['c_proveedor'])})"] = int(row["c_proveedor"])

with col4:
    proveedor_sel = st.selectbox("Proveedor", list(provider_options.keys()))

prov_id = provider_options[proveedor_sel]
if prov_id is not None:
    df = df[df["c_proveedor"] == int(prov_id)].copy()

df = _apply_control_logic(df)

if solo_sin_enviar:
    df = df[df["linea_sin_enviar"]].copy()
if solo_duplicadas:
    df = df[df["grupo_duplicado"]].copy()

provider_summary = _build_provider_summary(df)
duplicate_groups = _build_duplicate_groups(df)

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Proveedores afectados", f"{provider_summary['c_proveedor'].nunique() if not provider_summary.empty else 0:,}")
c2.metric("Líneas en control", f"{len(df):,}")
c3.metric("Líneas sin enviar", f"{_count_mask(df, df.get('linea_sin_enviar', pd.Series(index=df.index, dtype='bool'))):,}")
c4.metric("Grupos duplicados", f"{len(duplicate_groups):,}")
c5.metric("Candidatas vigentes", f"{_count_mask(df, df.get('es_candidata_vigente', pd.Series(index=df.index, dtype='bool'))):,}")
c6.metric("Bloqueadas manualmente", f"{_count_mask(df, df.get('bloqueada_manual', pd.Series(index=df.index, dtype='bool'))):,}")

suggested_block_ids = (
    df[
        df["grupo_duplicado"]
        & (~df["es_candidata_vigente"])
        & (~df["ya_publicado"])
        & (~df["bloqueada_manual"])
    ]["connexa_detail_uuid"]
    .dropna()
    .astype(str)
    .drop_duplicates()
    .tolist()
)

with st.expander("Bloqueo manual por detail_uuid"):
    st.caption(
        "La candidata vigente se define por la fecha mas reciente dentro del mismo proveedor + origen + destino + articulo."
    )

    if suggested_block_ids:
        st.caption("UUID sugeridos para bloquear con los filtros actuales")
        st.code("\n".join(suggested_block_ids), language="text")

    with st.form("transfer_blocklist_form"):
        usuario_default = os.getenv("USERNAME") or os.getenv("USER") or "streamlit"
        usuario = st.text_input("Usuario", value=usuario_default)
        motivo = st.text_input("Motivo", value="DUPLICADA_ABRIL_NO_ENVIAR")
        observacion = st.text_input("Observación", value="")
        raw_uuids = st.text_area(
            "Detail UUID a bloquear",
            value="\n".join(suggested_block_ids),
            height=180,
            help="Podés pegar uno por línea o separados por coma.",
        )
        submit_block = st.form_submit_button("Bloquear detail_uuid")

    if submit_block:
        selected_uuids = _parse_uuid_list(raw_uuids)
        if not selected_uuids:
            st.error("No encontré UUID válidos para bloquear.")
        else:
            pg_engine = get_diarco_engine()
            block_rows = _build_block_rows(raw, selected_uuids, motivo, usuario, observacion)
            if pg_engine is None:
                st.error("No se pudo abrir la conexión a diarco_data.")
            elif not block_rows:
                st.error("Los UUID indicados no están en el snapshot actual de pendientes.")
            else:
                upsert_transfer_blocklist(pg_engine, block_rows)
                st.cache_data.clear()
                st.success(f"Se bloquearon {len(block_rows):,} detail_uuid.")
                st.rerun()

tab1, tab2, tab3 = st.tabs(["Resumen por proveedor", "Grupos duplicados", "Detalle de líneas"])

with tab1:
    st.subheader("Proveedores con transferencias pendientes / multiples")
    st.dataframe(
        provider_summary.rename(
            columns={
                "c_proveedor": "Proveedor",
                "n_proveedor": "Nombre proveedor",
                "lineas_pendientes": "Líneas pendientes",
                "lineas_sin_enviar": "Líneas sin enviar",
                "lineas_ya_publicadas": "Líneas ya publicadas",
                "lineas_bloqueadas": "Líneas bloqueadas",
                "candidatas_vigentes": "Candidatas vigentes",
                "bultos_pendientes": "Bultos pendientes",
                "primera_fecha": "Primera fecha",
                "ultima_fecha": "Última fecha",
                "grupos_duplicados": "Grupos duplicados",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

with tab2:
    st.subheader("Grupos donde parece haberse generado mas de una transferencia")
    if duplicate_groups.empty:
        st.success("No se detectaron grupos duplicados con los filtros actuales.")
    else:
        st.dataframe(
            duplicate_groups.rename(
                columns={
                    "c_proveedor": "Proveedor",
                    "n_proveedor": "Nombre proveedor",
                    "origin_cd_num": "CD origen",
                    "dest_store_num": "Sucursal destino",
                    "item_code_num": "Artículo",
                    "descripcion": "Descripción",
                    "lineas": "Líneas",
                    "headers_distintos": "Headers distintos",
                    "bultos_totales": "Bultos totales",
                    "bultos_maximos": "Bultos máximos",
                    "primera_fecha": "Primera fecha",
                    "ultima_fecha": "Última fecha",
                    "ya_publicadas": "Ya publicadas",
                    "bloqueadas": "Bloqueadas",
                    "detail_uuid_candidata": "Detail UUID candidata",
                    "header_uuid_candidata": "Header UUID candidata",
                    "bultos_candidata": "Bultos candidata",
                    "accion_sugerida": "Acción sugerida",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

with tab3:
    st.subheader("Detalle de líneas para saneamiento")
    df_show = df.sort_values(
        ["grupo_duplicado", "control_ts", "qty_requested_num"],
        ascending=[False, False, False],
    ).copy()

    columnas = [
        "accion_sugerida",
        "grupo_duplicado",
        "es_candidata_vigente",
        "ya_publicado",
        "bloqueada_manual",
        "bloqueo_motivo",
        "bloqueo_usuario",
        "bloqueo_created_at",
        "dmz_estado",
        "dmz_estado_vk",
        "status_code",
        "n_proveedor",
        "c_proveedor",
        "control_ts",
        "connexa_purchase_code",
        "origin_cd_num",
        "dest_store_num",
        "item_code_num",
        "item_description",
        "qty_requested_num",
        "connexa_header_uuid",
        "connexa_detail_uuid",
    ]
    columnas = [c for c in columnas if c in df_show.columns]

    st.dataframe(
        df_show[columnas].rename(
            columns={
                "accion_sugerida": "Acción sugerida",
                "grupo_duplicado": "Grupo duplicado",
                "es_candidata_vigente": "Es candidata vigente",
                "ya_publicado": "Ya publicada",
                "bloqueada_manual": "Bloqueada manualmente",
                "bloqueo_motivo": "Motivo bloqueo",
                "bloqueo_usuario": "Usuario bloqueo",
                "bloqueo_created_at": "Fecha bloqueo",
                "dmz_estado": "Estado SGM/DMZ",
                "dmz_estado_vk": "Estado VK DMZ",
                "status_code": "Estado Connexa",
                "n_proveedor": "Nombre proveedor",
                "c_proveedor": "Proveedor",
                "control_ts": "Fecha control",
                "connexa_purchase_code": "Código transferencia",
                "origin_cd_num": "CD origen",
                "dest_store_num": "Sucursal destino",
                "item_code_num": "Artículo",
                "item_description": "Descripción",
                "qty_requested_num": "Bultos solicitados",
                "connexa_header_uuid": "Header UUID",
                "connexa_detail_uuid": "Detail UUID",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

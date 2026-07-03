from __future__ import annotations

import json
import os

import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_connexa_engine, get_pg_engine
from modules.ui import make_date_filters, render_header
from modules.queries.forecast_algoritmos import get_supplier_dim
from modules.queries.forecast_logs import (
    get_forecast_log_events,
    get_forecast_log_summary,
)


st.set_page_config(
    page_title="Monitoreo Logs Forecast",
    page_icon="🧭",
    layout="wide",
)

render_header("Monitoreo operativo de logs Forecast")

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))


def _normalize_key(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _supplier_label_map(df_suppliers: pd.DataFrame) -> dict[str, str]:
    if df_suppliers.empty:
        return {}

    tmp = df_suppliers.copy()
    tmp["supplier_code"] = tmp["supplier_code"].map(_normalize_key)
    tmp["supplier_name"] = tmp["supplier_name"].fillna("").astype(str).str.strip()
    return {
        row["supplier_code"]: (
            f"{row['supplier_name']} ({row['supplier_code']})"
            if row["supplier_name"]
            else row["supplier_code"]
        )
        for _, row in tmp.iterrows()
        if row["supplier_code"]
    }


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return float(_to_numeric(df[col]).fillna(0).sum())


def _safe_mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df.columns:
        return None
    series = _to_numeric(df[col]).dropna()
    if series.empty:
        return None
    return float(series.mean())


def _safe_int(df: pd.DataFrame, col: str) -> int:
    if df.empty or col not in df.columns:
        return 0
    return int(_to_numeric(df[col]).fillna(0).sum())


def _fmt_minutes(ms: float | None) -> str:
    if ms is None:
        return "—"
    return f"{ms / 60000:,.1f}"


def _pretty_json_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2)

    raw = str(value).strip()
    if raw == "" or raw.lower() == "none":
        return ""

    try:
        parsed = json.loads(raw)
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    except Exception:
        return raw


@st.cache_data(ttl=TTL, show_spinner=False)
def _fetch_suppliers() -> pd.DataFrame:
    try:
        eng = get_pg_engine()
        return get_supplier_dim(eng)
    except Exception:
        return pd.DataFrame(columns=["supplier_code", "supplier_name"])


@st.cache_data(ttl=TTL, show_spinner=True)
def _fetch_summary(desde, hasta, proveedor) -> pd.DataFrame:
    eng = get_connexa_engine()
    return get_forecast_log_summary(eng, desde=desde, hasta=hasta, proveedor=proveedor or None)


@st.cache_data(ttl=TTL, show_spinner=True)
def _fetch_events(execution_execute_id: str) -> pd.DataFrame:
    eng = get_connexa_engine()
    return get_forecast_log_events(eng, execution_execute_id)


with st.sidebar:
    st.subheader("Conexiones")
    try:
        get_connexa_engine()
        st.success("Connexa Platform: OK")
    except Exception as exc:
        st.error(f"Connexa Platform no disponible: {exc}")
        st.stop()

    try:
        get_pg_engine()
        st.success("Diarco Data: OK")
    except Exception as exc:
        st.warning(f"Diarco Data no disponible: {exc}")

    st.divider()
    st.subheader("Ayuda")
    st.caption(
        "Esta pantalla usa la vista `supply_planning.vw_forecast_execution_log_resumen` "
        "para resumir el pipeline Forecast de punta a punta."
    )


df_suppliers = _fetch_suppliers()
supplier_map = _supplier_label_map(df_suppliers)

supplier_options = [("", "(Todos)")]
if not df_suppliers.empty:
    tmp_sup = df_suppliers.copy()
    tmp_sup["supplier_code"] = tmp_sup["supplier_code"].map(_normalize_key)
    tmp_sup["supplier_label"] = tmp_sup["supplier_code"].map(lambda c: supplier_map.get(c, c))
    supplier_options.extend(
        [
            (row["supplier_code"], row["supplier_label"])
            for _, row in tmp_sup.sort_values(["supplier_name", "supplier_code"]).iterrows()
            if row["supplier_code"]
        ]
    )


st.caption(
    "Resumen operativo por ejecución de forecast, con filtros, KPIs, "
    "estado final y drill-down al timeline de eventos."
)

desde, hasta = make_date_filters()

colf1, colf2, colf3, colf4 = st.columns([2.3, 1.2, 1.2, 1.8])
with colf1:
    supplier_selected = st.selectbox(
        "Proveedor",
        options=supplier_options,
        index=0,
        format_func=lambda x: x[1],
    )[0]
with colf2:
    only_issues = st.checkbox("Solo con issues", value=False)
with colf3:
    only_last = st.checkbox("Solo last_execution", value=True)
with colf4:
    search_text = st.text_input("Buscar ejecución / UUID", value="").strip().lower()


df = _fetch_summary(desde, hasta, supplier_selected).copy()

if df.empty:
    st.info("No se encontraron ejecuciones de forecast para los filtros seleccionados.")
    st.stop()


for col in [
    "execution_created_at",
    "last_log_at",
    "last_error_at",
    "s10_started_at",
    "params_at",
    "s10_finished_at",
    "s20_finished_at",
    "s30_finished_at",
    "s40_kpis_at",
    "s40_detail_at",
    "s40_finished_at",
]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

for col in [
    "s10_duration_ms",
    "s20_duration_ms",
    "s30_duration_ms",
    "s40_duration_ms",
    "s40_detail_duration_ms",
    "forecast_total_products",
    "forecast_total_sites",
    "forecast_total_units",
    "forecast_rows",
    "graphics_error_rows",
    "publication_total_units",
    "publication_prepared_rows",
    "publication_inserted_rows",
    "publication_error_rows",
    "error_events",
    "warning_events",
]:
    if col in df.columns:
        df[col] = _to_numeric(df[col])

df["supplier_code"] = df["ext_supplier_code"].map(_normalize_key)
df["supplier_label"] = df["supplier_code"].map(lambda c: supplier_map.get(c, c or "-"))
df["method"] = df["method"].fillna("SIN_METODO").astype(str).str.strip()
df["execution_name"] = df["execution_name"].fillna("").astype(str)
df["publication_result_status"] = df["publication_result_status"].fillna("sin_publicacion").astype(str)
df["current_status_id"] = df["current_status_id"].astype(str)

if only_last and "last_execution" in df.columns:
    df = df[df["last_execution"].fillna(False).astype(bool)].copy()

if only_issues:
    issue_mask = (
        df["has_errors"].fillna(False).astype(bool)
        | df["has_warnings"].fillna(False).astype(bool)
        | df["publication_result_status"].isin(["partial", "invalid_input", "aborted_preparation", "unknown"])
        | df["current_status_id"].eq("99")
    )
    df = df[issue_mask].copy()

if search_text:
    search_cols = [
        "forecast_execution_execute_id",
        "forecast_execution_id",
        "execution_name",
        "last_message",
        "last_error_message",
        "method",
        "supplier_code",
    ]
    mask = pd.Series(False, index=df.index)
    for col in search_cols:
        if col in df.columns:
            mask = mask | df[col].fillna("").astype(str).str.lower().str.contains(search_text, regex=False)
    df = df[mask].copy()

if df.empty:
    st.info("Con los filtros aplicados no quedaron ejecuciones para mostrar.")
    st.stop()


st.markdown("## 1. Resumen ejecutivo")

total_runs = len(df)
error_runs = int(df["has_errors"].fillna(False).astype(bool).sum())
warning_runs = int(df["has_warnings"].fillna(False).astype(bool).sum())
success_runs = int(df["publication_result_status"].eq("success").sum())
partial_runs = int(df["publication_result_status"].eq("partial").sum())

avg_s10 = _safe_mean(df, "s10_duration_ms")
avg_s20 = _safe_mean(df, "s20_duration_ms")
avg_s30 = _safe_mean(df, "s30_duration_ms")
avg_s40 = _safe_mean(df, "s40_duration_ms")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Ejecuciones", f"{total_runs:,.0f}")
with col2:
    st.metric("Con errores", f"{error_runs:,.0f}")
with col3:
    st.metric("Con warnings", f"{warning_runs:,.0f}")
with col4:
    st.metric("Publicación success", f"{success_runs:,.0f}")

col5, col6, col7, col8 = st.columns(4)
with col5:
    st.metric("Publicación partial", f"{partial_runs:,.0f}")
with col6:
    st.metric("S10 prom. (min)", _fmt_minutes(avg_s10))
with col7:
    st.metric("S30 prom. (min)", _fmt_minutes(avg_s30))
with col8:
    st.metric("S40 prom. (min)", _fmt_minutes(avg_s40))


st.markdown("---")
st.markdown("## 2. Algoritmos más usados")

algo_usage = (
    df.groupby("method", dropna=False, as_index=False)
    .agg(
        ejecuciones=("forecast_execution_execute_id", "count"),
        proveedores=("supplier_code", pd.Series.nunique),
        con_error=("has_errors", "sum"),
        publicaciones_ok=("publication_result_status", lambda s: int(pd.Series(s).eq("success").sum())),
        s10_duration_ms=("s10_duration_ms", "mean"),
        s40_duration_ms=("s40_duration_ms", "mean"),
    )
    .sort_values(["proveedores", "ejecuciones"], ascending=[False, False])
)
algo_usage["con_error"] = _to_numeric(algo_usage["con_error"]).fillna(0).astype(int)
algo_usage["publicaciones_ok"] = _to_numeric(algo_usage["publicaciones_ok"]).fillna(0).astype(int)

col_algo_1, col_algo_2 = st.columns([1.2, 1.8])
with col_algo_1:
    if algo_usage.empty:
        st.info("No hay datos de algoritmos para los filtros actuales.")
    else:
        fig_algo = px.bar(
            algo_usage.head(12),
            x="method",
            y="proveedores",
            color="ejecuciones",
            text_auto=True,
            title="Proveedores por algoritmo",
        )
        fig_algo.update_layout(
            xaxis_title="Algoritmo",
            yaxis_title="Cantidad de proveedores",
            coloraxis_colorbar_title="Ejecuciones",
        )
        st.plotly_chart(fig_algo, use_container_width=True)

with col_algo_2:
    st.dataframe(
        algo_usage[
            [
                "method",
                "proveedores",
                "ejecuciones",
                "con_error",
                "publicaciones_ok",
                "s10_duration_ms",
                "s40_duration_ms",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

if supplier_selected:
    st.markdown("### 2.1 Algoritmos utilizados por el proveedor filtrado")
    provider_algo = (
        df.groupby(["supplier_label", "method"], dropna=False, as_index=False)
        .agg(
            ejecuciones=("forecast_execution_execute_id", "count"),
            errores=("has_errors", "sum"),
            warnings=("has_warnings", "sum"),
            forecast_unidades=("forecast_total_units", "sum"),
            publication_success=("publication_result_status", lambda s: int(pd.Series(s).eq("success").sum())),
            s10_duration_ms=("s10_duration_ms", "mean"),
            s40_duration_ms=("s40_duration_ms", "mean"),
            ultima_ejecucion=("execution_created_at", "max"),
        )
        .sort_values(["ejecuciones", "ultima_ejecucion"], ascending=[False, False])
    )
    provider_algo["errores"] = _to_numeric(provider_algo["errores"]).fillna(0).astype(int)
    provider_algo["warnings"] = _to_numeric(provider_algo["warnings"]).fillna(0).astype(int)
    provider_algo["publication_success"] = _to_numeric(provider_algo["publication_success"]).fillna(0).astype(int)

    cpa1, cpa2 = st.columns([1.1, 1.9])
    with cpa1:
        fig_provider_algo = px.bar(
            provider_algo,
            x="method",
            y="ejecuciones",
            color="publication_success",
            text_auto=True,
            title="Ejecuciones por algoritmo del proveedor",
        )
        fig_provider_algo.update_layout(
            xaxis_title="Algoritmo",
            yaxis_title="Ejecuciones",
            coloraxis_colorbar_title="Publ. OK",
        )
        st.plotly_chart(fig_provider_algo, use_container_width=True)
    with cpa2:
        st.dataframe(
            provider_algo[
                [
                    "method",
                    "ejecuciones",
                    "publication_success",
                    "errores",
                    "warnings",
                    "forecast_unidades",
                    "s10_duration_ms",
                    "s40_duration_ms",
                    "ultima_ejecucion",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )


st.markdown("---")
st.markdown("## 3. Estado operativo")

col_a, col_b = st.columns(2)

with col_a:
    status_counts = (
        df["current_status_id"]
        .fillna("SIN_ESTADO")
        .value_counts(dropna=False)
        .rename_axis("current_status_id")
        .reset_index(name="ejecuciones")
    )
    fig_status = px.bar(
        status_counts,
        x="current_status_id",
        y="ejecuciones",
        text_auto=True,
        title="Ejecuciones por estado actual",
    )
    fig_status.update_layout(xaxis_title="Estado", yaxis_title="Cantidad")
    st.plotly_chart(fig_status, use_container_width=True)

with col_b:
    pub_counts = (
        df["publication_result_status"]
        .fillna("sin_publicacion")
        .value_counts(dropna=False)
        .rename_axis("publication_result_status")
        .reset_index(name="ejecuciones")
    )
    fig_pub = px.bar(
        pub_counts,
        x="publication_result_status",
        y="ejecuciones",
        color="publication_result_status",
        text_auto=True,
        title="Resultado de publicación",
    )
    fig_pub.update_layout(xaxis_title="Resultado", yaxis_title="Cantidad", showlegend=False)
    st.plotly_chart(fig_pub, use_container_width=True)


st.markdown("---")
st.markdown("## 4. Tiempos por etapa")

duration_df = pd.DataFrame(
    {
        "etapa": ["S10", "S20", "S30", "S40"],
        "duracion_promedio_ms": [avg_s10, avg_s20, avg_s30, avg_s40],
    }
)
duration_df = duration_df.dropna(subset=["duracion_promedio_ms"])

col_c, col_d = st.columns([1.2, 1.8])
with col_c:
    if duration_df.empty:
        st.info("No hay duraciones disponibles con los filtros actuales.")
    else:
        fig_dur = px.bar(
            duration_df,
            x="etapa",
            y="duracion_promedio_ms",
            text_auto=".0f",
            title="Duración promedio por etapa (ms)",
        )
        fig_dur.update_layout(xaxis_title="Etapa", yaxis_title="Duración promedio (ms)")
        st.plotly_chart(fig_dur, use_container_width=True)

with col_d:
    slowest_cols = [
        "execution_created_at",
        "supplier_label",
        "execution_name",
        "method",
        "current_status_id",
        "s10_duration_ms",
        "s20_duration_ms",
        "s30_duration_ms",
        "s40_duration_ms",
        "publication_result_status",
    ]
    df_slowest = df[slowest_cols].copy()
    for col in ["s10_duration_ms", "s20_duration_ms", "s30_duration_ms", "s40_duration_ms"]:
        if col in df_slowest.columns:
            df_slowest[col] = _to_numeric(df_slowest[col])
    df_slowest["pipeline_total_ms"] = (
        df_slowest[["s10_duration_ms", "s20_duration_ms", "s30_duration_ms", "s40_duration_ms"]]
        .fillna(0)
        .sum(axis=1)
    )
    df_slowest = df_slowest.sort_values("pipeline_total_ms", ascending=False).head(15)
    st.dataframe(df_slowest, use_container_width=True, hide_index=True)


st.markdown("---")
st.markdown("## 5. Alertas y calidad del proceso")

issue_df = df[
    (
        df["has_errors"].fillna(False).astype(bool)
        | df["has_warnings"].fillna(False).astype(bool)
        | df["publication_result_status"].ne("success")
        | df["current_status_id"].eq("99")
    )
].copy()

if issue_df.empty:
    st.success("No se detectaron ejecuciones con errores, warnings o publicaciones parciales para los filtros actuales.")
else:
    issue_cols = [
        "execution_created_at",
        "supplier_label",
        "execution_name",
        "method",
        "current_status_id",
        "publication_result_status",
        "error_events",
        "warning_events",
        "last_error_event_type",
        "last_error_message",
        "graphics_error_rows",
        "publication_error_rows",
        "publication_prepared_rows",
        "publication_inserted_rows",
    ]
    st.dataframe(issue_df[issue_cols], use_container_width=True, hide_index=True)


st.markdown("---")
st.markdown("## 6. Detalle de ejecuciones")

detail_cols = [
    "execution_created_at",
    "forecast_execution_execute_id",
    "supplier_label",
    "execution_name",
    "method",
    "current_status_id",
    "last_event_type",
    "last_log_status",
    "error_events",
    "warning_events",
    "forecast_total_products",
    "forecast_total_sites",
    "forecast_total_units",
    "graphics_error_rows",
    "publication_result_status",
    "publication_prepared_rows",
    "publication_inserted_rows",
    "publication_error_rows",
    "publication_files_moved",
    "last_message",
]
st.dataframe(df[detail_cols], use_container_width=True, hide_index=True)
st.download_button(
    "Descargar detalle CSV",
    data=df.to_csv(index=False).encode("utf-8"),
    file_name="forecast_log_resumen.csv",
    mime="text/csv",
)


st.markdown("---")
st.markdown("## 7. Drill-down por ejecución")

execution_options = (
    df[["forecast_execution_execute_id", "execution_name", "supplier_label", "execution_created_at"]]
    .drop_duplicates()
    .sort_values("execution_created_at", ascending=False)
)
execution_options["option_label"] = execution_options.apply(
    lambda r: (
        f"{r['execution_created_at']:%Y-%m-%d %H:%M} | "
        f"{r['supplier_label']} | {r['execution_name']} | {r['forecast_execution_execute_id']}"
    ),
    axis=1,
)

selected_exec_label = st.selectbox(
    "Seleccionar ejecución",
    options=execution_options["option_label"].tolist(),
    index=0,
)
selected_exec_id = execution_options.loc[
    execution_options["option_label"].eq(selected_exec_label),
    "forecast_execution_execute_id",
].iloc[0]

summary_row = df[df["forecast_execution_execute_id"].eq(selected_exec_id)].head(1).copy()
events_df = _fetch_events(str(selected_exec_id))

if not summary_row.empty:
    st.markdown("### 7.1 Resumen puntual")
    row = summary_row.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Estado actual", row.get("current_status_id", "—"))
    with c2:
        st.metric("Resultado publicación", row.get("publication_result_status", "—"))
    with c3:
        st.metric("Errores", f"{int(row.get('error_events', 0) or 0):,}")
    with c4:
        st.metric("Warnings", f"{int(row.get('warning_events', 0) or 0):,}")

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.metric("Productos", f"{int(float(row.get('forecast_total_products', 0) or 0)):,.0f}")
    with c6:
        st.metric("Sitios", f"{int(float(row.get('forecast_total_sites', 0) or 0)):,.0f}")
    with c7:
        units_val = pd.to_numeric(pd.Series([row.get("forecast_total_units")]), errors="coerce").iloc[0]
        st.metric("Unidades forecast", f"{units_val:,.0f}" if pd.notna(units_val) else "—")
    with c8:
        st.metric("OTIF", f"{float(row.get('publication_otif')):,.2f}" if pd.notna(pd.to_numeric(pd.Series([row.get('publication_otif')]), errors='coerce').iloc[0]) else "—")


st.markdown("### 7.2 Timeline de eventos")

if events_df.empty:
    st.info("No se encontraron eventos de log para la ejecución seleccionada.")
else:
    for col in ["event_ts", "started_at", "ended_at"]:
        if col in events_df.columns:
            events_df[col] = pd.to_datetime(events_df[col], errors="coerce")

    timeline_cols = [
        "event_ts",
        "step_name",
        "event_type",
        "severity",
        "status",
        "duration_ms",
        "message",
        "error_code",
        "error_message",
        "total_products",
        "total_sites",
        "total_units",
        "total_amount",
    ]
    st.dataframe(events_df[timeline_cols], use_container_width=True, hide_index=True)

    with st.expander("Ver payloads JSON del timeline"):
        event_labels = events_df.apply(
            lambda r: (
                f"{pd.to_datetime(r['event_ts'], errors='coerce').strftime('%Y-%m-%d %H:%M:%S') if pd.notna(pd.to_datetime(r['event_ts'], errors='coerce')) else 'SIN_FECHA'}"
                f" | {r['step_name']} | {r['event_type']}"
            ),
            axis=1,
        ).tolist()
        selected_event_label = st.selectbox("Evento", options=event_labels, index=len(event_labels) - 1)
        selected_event = events_df.iloc[event_labels.index(selected_event_label)]

        jc1, jc2, jc3 = st.columns(3)
        with jc1:
            st.caption("context")
            st.code(_pretty_json_text(selected_event.get("context_json")), language="json")
        with jc2:
            st.caption("result")
            st.code(_pretty_json_text(selected_event.get("result_json")), language="json")
        with jc3:
            st.caption("diagnostics")
            st.code(_pretty_json_text(selected_event.get("diagnostics_json")), language="json")

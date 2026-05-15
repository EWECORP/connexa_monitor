from __future__ import annotations

import os
from datetime import timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

from modules.db import get_connexa_engine, get_pg_engine
from modules.ui import make_date_filters, render_header
from modules.queries.forecast_algoritmos import (
    get_forecast_result_detail,
    get_sales_daily_agg,
    get_supplier_dim,
)


st.set_page_config(
    page_title="Indicador 18 — Monitoreo de Algoritmos Forecast",
    page_icon="📉",
    layout="wide",
)

render_header("Indicador 18 — Monitoreo de Algoritmos Forecast y Ventas Reales")

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))


def _normalize_key(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _series_or_default(df: pd.DataFrame, col: str, default: object) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series(default, index=df.index)


def _safe_div(num: float, den: float) -> float | None:
    if den in (0, 0.0) or pd.isna(den):
        return None
    return float(num) / float(den)


def _pct_or_none(series: pd.Series) -> float | None:
    if series.empty:
        return None
    return float(series.mean()) * 100.0


def _wape(actual: pd.Series, predicted: pd.Series) -> float | None:
    den = float(pd.to_numeric(actual, errors="coerce").fillna(0).sum())
    if den == 0:
        return None
    num = float((pd.to_numeric(predicted, errors="coerce").fillna(0) - pd.to_numeric(actual, errors="coerce").fillna(0)).abs().sum())
    return num / den


def _bias(actual: pd.Series, predicted: pd.Series) -> float | None:
    den = float(pd.to_numeric(actual, errors="coerce").fillna(0).sum())
    if den == 0:
        return None
    num = float((pd.to_numeric(predicted, errors="coerce").fillna(0) - pd.to_numeric(actual, errors="coerce").fillna(0)).sum())
    return num / den


def _prepare_forecast(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["forecast_date"] = pd.to_datetime(_series_or_default(out, "forecast_date", pd.NaT), errors="coerce").dt.normalize()
    out["forecast_ts"] = pd.to_datetime(_series_or_default(out, "forecast_ts", pd.NaT), errors="coerce")
    out["supplier_code"] = _series_or_default(out, "supplier_code", "").map(_normalize_key)
    out["product_code"] = _series_or_default(out, "product_code", "").map(_normalize_key)
    out["site_code"] = _series_or_default(out, "site_code", "").map(_normalize_key)
    out["algorithm"] = _series_or_default(out, "algorithm", "SIN_ALGORITMO").fillna("SIN_ALGORITMO").astype(str).str.strip()
    out.loc[out["algorithm"].eq(""), "algorithm"] = "SIN_ALGORITMO"

    numeric_cols = [
        "result_id",
        "average",
        "forecast",
        "quantity_confirmed",
        "quantity_stock",
        "pending_purchases",
        "pending_transfer",
        "pending_in_transit",
        "window_sales_days",
        "serviceable_days",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(_series_or_default(out, col, np.nan), errors="coerce")

    out["approved"] = _series_or_default(out, "approved", False).fillna(False).astype(bool)
    out["blocked_for_purchase"] = _series_or_default(out, "blocked_for_purchase", False).fillna(False).astype(bool)
    out["reason"] = _series_or_default(out, "reason", "").fillna("").astype(str).str.strip()
    out["combo_key"] = out["product_code"] + " | " + out["site_code"]
    return out


def _prepare_sales(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out["sale_date"] = pd.to_datetime(_series_or_default(out, "sale_date", pd.NaT), errors="coerce").dt.normalize()
    out["product_code"] = _series_or_default(out, "product_code", "").map(_normalize_key)
    out["site_code"] = _series_or_default(out, "site_code", "").map(_normalize_key)
    out["supplier_code"] = _series_or_default(out, "supplier_code", "").map(_normalize_key)

    numeric_cols = ["units", "sales_amount", "price_rows", "distinct_prices"]
    for col in numeric_cols:
        out[col] = pd.to_numeric(_series_or_default(out, col, 0), errors="coerce").fillna(0.0)

    text_cols = ["product_name", "familia", "rubro", "subrubro"]
    for col in text_cols:
        out[col] = _series_or_default(out, col, "").fillna("").astype(str).str.strip()

    return out


def _pick_selected_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    grp_cols = ["forecast_date", "supplier_code", "product_code", "site_code"]
    work = df.copy()
    work["approved_sort"] = work["approved"].fillna(False).astype(int)
    work = work.sort_values(
        ["approved_sort", "forecast_ts", "result_id"],
        ascending=[False, False, False],
        kind="mergesort",
    )
    out = work.drop_duplicates(subset=grp_cols, keep="first").copy()
    out["selection_basis"] = np.where(out["approved"], "Aprobada", "Ultima simulacion del dia")
    return out


def _compute_forward_actuals(
    forecast_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    horizon_days: int,
) -> pd.DataFrame:
    if forecast_df.empty:
        return forecast_df.copy()

    out_parts: list[pd.DataFrame] = []
    sales_keys = ["product_code", "site_code"]
    sales_map = {
        key: grp.sort_values("sale_date").copy()
        for key, grp in sales_df.groupby(sales_keys, dropna=False)
    }

    for key, grp in forecast_df.groupby(sales_keys, dropna=False):
        part = grp.copy()
        sales_grp = sales_map.get(key)
        if sales_grp is None or sales_grp.empty:
            part["actual_units_h"] = 0.0
            part["actual_sales_amount_h"] = 0.0
            part["sales_days_h"] = 0
            part["multi_price_days_h"] = 0
            out_parts.append(part)
            continue

        dates = sales_grp["sale_date"].to_numpy(dtype="datetime64[ns]")
        units = sales_grp["units"].to_numpy(dtype=float)
        amounts = sales_grp["sales_amount"].to_numpy(dtype=float)
        sales_days = (sales_grp["units"].to_numpy(dtype=float) > 0).astype(int)
        multi_price_days = (sales_grp["distinct_prices"].to_numpy(dtype=float) > 1).astype(int)

        cum_units = np.concatenate(([0.0], units.cumsum()))
        cum_amounts = np.concatenate(([0.0], amounts.cumsum()))
        cum_sales_days = np.concatenate(([0], sales_days.cumsum()))
        cum_multi_price = np.concatenate(([0], multi_price_days.cumsum()))

        start_dates = pd.to_datetime(part["forecast_date"], errors="coerce").to_numpy(dtype="datetime64[ns]")
        end_dates = start_dates + np.timedelta64(max(horizon_days - 1, 0), "D")

        start_ix = np.searchsorted(dates, start_dates, side="left")
        end_ix = np.searchsorted(dates, end_dates, side="right")

        part["actual_units_h"] = cum_units[end_ix] - cum_units[start_ix]
        part["actual_sales_amount_h"] = cum_amounts[end_ix] - cum_amounts[start_ix]
        part["sales_days_h"] = cum_sales_days[end_ix] - cum_sales_days[start_ix]
        part["multi_price_days_h"] = cum_multi_price[end_ix] - cum_multi_price[start_ix]
        out_parts.append(part)

    return pd.concat(out_parts, ignore_index=True) if out_parts else forecast_df.copy()


def _build_supplier_label_map(df_suppliers: pd.DataFrame) -> dict[str, str]:
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


@st.cache_data(ttl=TTL, show_spinner=False)
def _fetch_suppliers() -> pd.DataFrame:
    eng = get_pg_engine()
    return get_supplier_dim(eng)


@st.cache_data(ttl=TTL, show_spinner=True)
def _fetch_forecast(desde, hasta, proveedor, articulo, sucursal) -> pd.DataFrame:
    eng = get_connexa_engine()
    return get_forecast_result_detail(
        eng,
        desde=desde,
        hasta=hasta,
        proveedor=proveedor or None,
        articulo=articulo or None,
        sucursal=sucursal or None,
    )


@st.cache_data(ttl=TTL, show_spinner=True)
def _fetch_sales(desde, hasta, proveedor, articulo, sucursal) -> pd.DataFrame:
    eng = get_pg_engine()
    return get_sales_daily_agg(
        eng,
        desde=desde,
        hasta=hasta,
        proveedor=proveedor or None,
        articulo=articulo or None,
        sucursal=sucursal or None,
    )


st.caption(
    "Las ventas se consolidan por fecha + articulo + sucursal. "
    "Si hubo multiples precios en el mismo dia, se suman todas las unidades "
    "para no duplicar ni fragmentar la demanda real."
)

desde, hasta = make_date_filters()

df_suppliers = _fetch_suppliers()
supplier_map = _build_supplier_label_map(df_suppliers)

supplier_options = [("", "(Todos)")]
if not df_suppliers.empty:
    tmp = df_suppliers.copy()
    tmp["supplier_code"] = tmp["supplier_code"].map(_normalize_key)
    tmp["supplier_label"] = tmp["supplier_code"].map(lambda c: supplier_map.get(c, c))
    supplier_options.extend(
        [
            (row["supplier_code"], row["supplier_label"])
            for _, row in tmp.sort_values(["supplier_name", "supplier_code"]).iterrows()
            if row["supplier_code"]
        ]
    )

col_f1, col_f2, col_f3, col_f4 = st.columns([2.4, 1.0, 1.0, 1.0])
with col_f1:
    supplier_selected = st.selectbox(
        "Proveedor",
        options=supplier_options,
        index=0,
        format_func=lambda x: x[1],
    )[0]
with col_f2:
    horizon_days = st.selectbox("Horizonte real", options=[7, 15, 30], index=1)
with col_f3:
    article_filter = st.text_input("Articulo", value="").strip()
with col_f4:
    site_filter = st.text_input("Sucursal", value="").strip()

sales_until = hasta + timedelta(days=int(horizon_days) - 1)

df_forecast_raw = _fetch_forecast(desde, hasta, supplier_selected, article_filter, site_filter)
df_sales_raw = _fetch_sales(desde, sales_until, supplier_selected, article_filter, site_filter)

df_forecast = _prepare_forecast(df_forecast_raw)
df_sales = _prepare_sales(df_sales_raw)

if df_forecast.empty:
    st.info("No se encontraron simulaciones de forecast para los filtros seleccionados.")
    st.stop()

df_forecast["supplier_label"] = df_forecast["supplier_code"].map(lambda c: supplier_map.get(c, c or "-"))

df_selected = _pick_selected_rows(df_forecast)
df_eval = _compute_forward_actuals(df_selected, df_sales, int(horizon_days))

df_eval["forecast_error"] = df_eval["forecast"].fillna(0) - df_eval["actual_units_h"].fillna(0)
df_eval["average_error"] = df_eval["average"].fillna(0) - df_eval["actual_units_h"].fillna(0)
df_eval["forecast_abs_error"] = df_eval["forecast_error"].abs()
df_eval["average_abs_error"] = df_eval["average_error"].abs()
df_eval["forecast_ape"] = np.where(
    df_eval["actual_units_h"].fillna(0) > 0,
    df_eval["forecast_abs_error"] / df_eval["actual_units_h"],
    np.nan,
)
df_eval["average_ape"] = np.where(
    df_eval["actual_units_h"].fillna(0) > 0,
    df_eval["average_abs_error"] / df_eval["actual_units_h"],
    np.nan,
)
df_eval["confirmed_vs_forecast"] = np.where(
    df_eval["forecast"].fillna(0) > 0,
    df_eval["quantity_confirmed"].fillna(0) / df_eval["forecast"],
    np.nan,
)


st.markdown("## 1. Resumen ejecutivo")

selected_runs = len(df_selected)
raw_runs = len(df_forecast)
actual_total = float(df_eval["actual_units_h"].fillna(0).sum())
forecast_total = float(df_eval["forecast"].fillna(0).sum())
average_total = float(df_eval["average"].fillna(0).sum())

wape_forecast = _wape(df_eval["actual_units_h"], df_eval["forecast"])
wape_average = _wape(df_eval["actual_units_h"], df_eval["average"])
bias_forecast = _bias(df_eval["actual_units_h"], df_eval["forecast"])

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Simulaciones crudas", value=f"{raw_runs:,.0f}")
with col2:
    st.metric("Combinaciones evaluadas", value=f"{selected_runs:,.0f}")
with col3:
    st.metric(f"Venta real prox. {horizon_days}d", value=f"{actual_total:,.0f}")
with col4:
    st.metric("Forecast total", value=f"{forecast_total:,.0f}")

col5, col6, col7, col8 = st.columns(4)
with col5:
    st.metric(
        "WAPE Forecast",
        value=f"{(wape_forecast * 100):,.1f} %" if wape_forecast is not None else "—",
    )
with col6:
    st.metric(
        "WAPE Average",
        value=f"{(wape_average * 100):,.1f} %" if wape_average is not None else "—",
        delta=(
            f"{((wape_average - wape_forecast) * 100):,.1f} pp"
            if wape_forecast is not None and wape_average is not None
            else None
        ),
    )
with col7:
    st.metric(
        "Bias Forecast",
        value=f"{(bias_forecast * 100):,.1f} %" if bias_forecast is not None else "—",
    )
with col8:
    st.metric(
        "Aprobadas",
        value=f"{_pct_or_none(df_eval['approved']):,.1f} %" if _pct_or_none(df_eval["approved"]) is not None else "—",
    )

col9, col10, col11, col12 = st.columns(4)
with col9:
    st.metric("Average total", value=f"{average_total:,.0f}")
with col10:
    st.metric(
        "Bloqueadas p/ compra",
        value=f"{_pct_or_none(df_eval['blocked_for_purchase']):,.1f} %" if _pct_or_none(df_eval["blocked_for_purchase"]) is not None else "—",
    )
with col11:
    confirmed_ratio = _safe_div(
        float(df_eval["quantity_confirmed"].fillna(0).sum()),
        float(df_eval["forecast"].fillna(0).sum()),
    )
    st.metric(
        "Confirmado / Forecast",
        value=f"{(confirmed_ratio * 100):,.1f} %" if confirmed_ratio is not None else "—",
    )
with col12:
    st.metric(
        "Dias con multi-precio",
        value=f"{int(df_eval['multi_price_days_h'].fillna(0).sum()):,d}",
    )


st.markdown("---")
st.markdown("## 2. Algoritmos por proveedor")

mix = (
    df_forecast.groupby(["supplier_label", "algorithm"], dropna=False, as_index=False)
    .agg(
        simulaciones=("result_id", "count"),
        combos=("combo_key", "nunique"),
        aprobadas=("approved", "mean"),
        bloqueadas=("blocked_for_purchase", "mean"),
    )
)
mix["aprobadas"] = mix["aprobadas"].fillna(0.0) * 100.0
mix["bloqueadas"] = mix["bloqueadas"].fillna(0.0) * 100.0

provider_totals = (
    mix.groupby("supplier_label", as_index=False)
    .agg(simulaciones=("simulaciones", "sum"))
    .sort_values("simulaciones", ascending=False)
)
top_suppliers = provider_totals.head(12)["supplier_label"].tolist()

mix_plot = mix[mix["supplier_label"].isin(top_suppliers)].copy()
if not mix_plot.empty:
    fig_mix = px.bar(
        mix_plot,
        x="supplier_label",
        y="simulaciones",
        color="algorithm",
        barmode="stack",
        title="Simulaciones por proveedor y algoritmo",
    )
    fig_mix.update_layout(
        xaxis_title="Proveedor",
        yaxis_title="Cantidad de simulaciones",
        legend_title="Algoritmo",
    )
    st.plotly_chart(fig_mix, use_container_width=True)

mix_pivot = (
    mix.pivot_table(
        index="supplier_label",
        columns="algorithm",
        values="simulaciones",
        aggfunc="sum",
        fill_value=0,
    )
    .reset_index()
)

st.dataframe(
    mix_pivot.sort_values("supplier_label"),
    use_container_width=True,
    hide_index=True,
)


st.markdown("---")
st.markdown("## 3. Cantidades estimadas vs ventas reales")

provider_daily = (
    df_eval.groupby(["forecast_date", "supplier_label"], dropna=False, as_index=False)
    .agg(
        forecast=("forecast", "sum"),
        average=("average", "sum"),
        actual_units_h=("actual_units_h", "sum"),
        quantity_confirmed=("quantity_confirmed", "sum"),
    )
)

if supplier_selected:
    provider_focus = supplier_map.get(supplier_selected, supplier_selected)
    provider_daily_plot = provider_daily[provider_daily["supplier_label"].eq(provider_focus)].copy()
    if not provider_daily_plot.empty:
        fig_provider = px.line(
            provider_daily_plot,
            x="forecast_date",
            y=["forecast", "average", "actual_units_h", "quantity_confirmed"],
            markers=True,
            title=f"{provider_focus}: forecast, average, venta real y confirmado",
        )
        fig_provider.update_layout(
            xaxis_title="Fecha de simulacion",
            yaxis_title="Unidades",
            legend_title="Serie",
        )
        st.plotly_chart(fig_provider, use_container_width=True)
else:
    provider_summary = (
        df_eval.groupby("supplier_label", dropna=False, as_index=False)
        .agg(
            forecast=("forecast", "sum"),
            average=("average", "sum"),
            actual_units_h=("actual_units_h", "sum"),
            simulaciones=("result_id", "count"),
        )
    )
    provider_summary["forecast_vs_real_gap"] = provider_summary["forecast"] - provider_summary["actual_units_h"]
    provider_summary = provider_summary.sort_values("forecast", ascending=False).head(15)
    fig_provider = px.bar(
        provider_summary,
        x="supplier_label",
        y=["forecast", "actual_units_h", "average"],
        barmode="group",
        title="Top proveedores por volumen estimado vs venta real",
    )
    fig_provider.update_layout(
        xaxis_title="Proveedor",
        yaxis_title="Unidades",
        legend_title="Serie",
    )
    st.plotly_chart(fig_provider, use_container_width=True)
    st.dataframe(provider_summary, use_container_width=True, hide_index=True)


st.markdown("---")
st.markdown("## 4. Evolucion por articulo y sucursal")

combo_summary = (
    df_eval.groupby(["product_code", "site_code"], dropna=False, as_index=False)
    .agg(
        simulaciones=("result_id", "count"),
        forecast_total=("forecast", "sum"),
        actual_total=("actual_units_h", "sum"),
        avg_abs_error=("forecast_abs_error", "mean"),
    )
    .sort_values(["simulaciones", "forecast_total"], ascending=[False, False])
)

combo_summary["label"] = combo_summary.apply(
    lambda r: f"Articulo {r['product_code']} | Sucursal {r['site_code']}",
    axis=1,
)

combo_options = combo_summary["label"].head(200).tolist()
if not combo_options:
    st.info("No hay combinaciones articulo / sucursal para graficar con los filtros actuales.")
else:
    combo_selected = st.selectbox(
        "Combo articulo / sucursal",
        options=combo_options,
        index=0,
    )
    combo_row = combo_summary[combo_summary["label"].eq(combo_selected)].head(1)
    product_code = combo_row["product_code"].iloc[0]
    site_code = combo_row["site_code"].iloc[0]

    combo_df = df_eval[
        df_eval["product_code"].eq(product_code)
        & df_eval["site_code"].eq(site_code)
    ].copy()
    combo_daily = (
        combo_df.groupby("forecast_date", as_index=False)
        .agg(
            forecast=("forecast", "sum"),
            average=("average", "sum"),
            actual_units_h=("actual_units_h", "sum"),
            quantity_confirmed=("quantity_confirmed", "sum"),
        )
        .sort_values("forecast_date")
    )

    fig_combo = px.line(
        combo_daily,
        x="forecast_date",
        y=["forecast", "average", "actual_units_h", "quantity_confirmed"],
        markers=True,
        title=f"Evolucion de estimaciones para articulo {product_code} en sucursal {site_code}",
    )
    fig_combo.update_layout(
        xaxis_title="Fecha de simulacion",
        yaxis_title="Unidades",
        legend_title="Serie",
    )
    st.plotly_chart(fig_combo, use_container_width=True)

    combo_detail = combo_df[
        [
            "forecast_date",
            "supplier_label",
            "algorithm",
            "forecast",
            "average",
            "actual_units_h",
            "quantity_confirmed",
            "approved",
            "blocked_for_purchase",
            "selection_basis",
            "reason",
        ]
    ].sort_values("forecast_date", ascending=False)
    st.dataframe(combo_detail, use_container_width=True, hide_index=True)


st.markdown("---")
st.markdown("## 5. Efectividad por algoritmo y outliers")

algo_perf = (
    df_eval.groupby("algorithm", dropna=False, as_index=False)
    .agg(
        simulaciones=("result_id", "count"),
        forecast_total=("forecast", "sum"),
        average_total=("average", "sum"),
        actual_total=("actual_units_h", "sum"),
        aprobadas=("approved", "mean"),
        bloqueadas=("blocked_for_purchase", "mean"),
        abs_error_forecast=("forecast_abs_error", "sum"),
        abs_error_average=("average_abs_error", "sum"),
    )
)

algo_perf["wape_forecast"] = np.where(
    algo_perf["actual_total"].fillna(0) > 0,
    algo_perf["abs_error_forecast"] / algo_perf["actual_total"],
    np.nan,
)
algo_perf["wape_average"] = np.where(
    algo_perf["actual_total"].fillna(0) > 0,
    algo_perf["abs_error_average"] / algo_perf["actual_total"],
    np.nan,
)
algo_perf["aprobadas"] = algo_perf["aprobadas"].fillna(0.0) * 100.0
algo_perf["bloqueadas"] = algo_perf["bloqueadas"].fillna(0.0) * 100.0
algo_perf = algo_perf.sort_values(["simulaciones", "forecast_total"], ascending=[False, False])

col_a, col_b = st.columns(2)
with col_a:
    fig_algo = px.bar(
        algo_perf,
        x="algorithm",
        y="wape_forecast",
        title="WAPE del forecast por algoritmo",
        text_auto=".1%",
    )
    fig_algo.update_layout(
        xaxis_title="Algoritmo",
        yaxis_title="WAPE",
    )
    st.plotly_chart(fig_algo, use_container_width=True)
with col_b:
    fig_algo2 = px.bar(
        algo_perf,
        x="algorithm",
        y=["aprobadas", "bloqueadas"],
        barmode="group",
        title="Aprobacion y bloqueo por algoritmo",
    )
    fig_algo2.update_layout(
        xaxis_title="Algoritmo",
        yaxis_title="Porcentaje",
        legend_title="Serie",
    )
    st.plotly_chart(fig_algo2, use_container_width=True)

st.dataframe(
    algo_perf[
        [
            "algorithm",
            "simulaciones",
            "forecast_total",
            "average_total",
            "actual_total",
            "wape_forecast",
            "wape_average",
            "aprobadas",
            "bloqueadas",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

outliers = df_eval[
    [
        "forecast_date",
        "supplier_label",
        "algorithm",
        "product_code",
        "site_code",
        "forecast",
        "average",
        "actual_units_h",
        "forecast_error",
        "forecast_abs_error",
        "approved",
        "blocked_for_purchase",
        "reason",
    ]
].sort_values(["forecast_abs_error", "forecast_date"], ascending=[False, False])

with st.expander("Top outliers de forecast"):
    st.dataframe(outliers.head(100), use_container_width=True, hide_index=True)

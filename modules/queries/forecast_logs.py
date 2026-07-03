from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SQL_FORECAST_LOG_SUMMARY = text(
    """
    SELECT
        forecast_execution_execute_id,
        forecast_execution_id,
        forecast_execution_schedule_id,
        execution_created_at,
        ext_supplier_code,
        supplier_id,
        current_status_id,
        last_execution,
        execution_name,
        method,
        last_log_at,
        last_event_type,
        last_severity,
        last_log_status,
        last_step_name,
        last_message,
        error_events,
        warning_events,
        has_errors,
        has_warnings,
        last_error_at,
        last_error_event_type,
        last_error_step_name,
        last_error_code,
        last_error_message,
        s10_started_at,
        params_at,
        s10_finished_at,
        s10_duration_ms,
        s20_finished_at,
        s20_duration_ms,
        s30_finished_at,
        s30_duration_ms,
        s40_kpis_at,
        s40_detail_at,
        s40_detail_duration_ms,
        s40_finished_at,
        s40_duration_ms,
        param_ventana,
        param_f1,
        param_f2,
        param_f3,
        param_fecha_base,
        param_sucursales,
        param_rubros,
        param_subrubro1,
        param_subrubro2,
        param_subrubro3,
        forecast_total_products,
        forecast_total_sites,
        forecast_total_units,
        forecast_rows,
        forecast_file_path,
        extended_total_products,
        extended_total_sites,
        extended_rows,
        extended_ventas_rows,
        extended_articulos_rows,
        extended_stock_rows,
        extended_forecast_rows,
        extended_duplicate_rows,
        extended_null_product_id_rows,
        extended_null_site_id_rows,
        extended_file_path,
        graphics_total_products,
        graphics_total_sites,
        graphics_rows,
        graphics_rows_with_payload,
        graphics_recovered_from_backup_rows,
        graphics_new_rows,
        graphics_error_rows,
        graphics_file_path,
        publication_total_products,
        publication_total_units,
        publication_total_amount,
        publication_otif,
        publication_stock_days,
        publication_stock_days_color,
        publication_maximum_backorder_days,
        publication_contains_breaks,
        publication_missing_articles,
        publication_monthly_sales_mm,
        publication_monthly_purchases_mm,
        publication_monthly_margin_mm,
        publication_result_status,
        publication_prepared_rows,
        publication_inserted_rows,
        publication_source_rows,
        publication_error_rows,
        publication_files_moved
    FROM supply_planning.vw_forecast_execution_log_resumen
    WHERE execution_created_at >= :desde
      AND execution_created_at < (:hasta + INTERVAL '1 day')
      AND (
        :proveedor IS NULL
        OR TRIM(COALESCE(ext_supplier_code::text, '')) = :proveedor
      )
    ORDER BY execution_created_at DESC, forecast_execution_execute_id DESC;
    """
)


SQL_FORECAST_LOG_EVENTS = text(
    """
    SELECT
        id,
        supply_forecast_execution_execute_id,
        "timestamp" AS event_ts,
        event_type,
        severity,
        status,
        step_name,
        message,
        error_code,
        error_message,
        started_at,
        ended_at,
        duration_ms,
        total_products,
        total_sites,
        total_units,
        total_amount,
        alert_required,
        alert_sent,
        alert_sent_at,
        alert_channel,
        alert_message,
        context::text AS context_json,
        result::text AS result_json,
        diagnostics::text AS diagnostics_json
    FROM supply_planning.spl_supply_forecast_execution_execute_log
    WHERE supply_forecast_execution_execute_id = :execution_execute_id
    ORDER BY "timestamp" ASC, id ASC;
    """
)


def get_forecast_log_summary(
    connexa_engine: Engine,
    desde: date,
    hasta: date,
    proveedor: str | None = None,
) -> pd.DataFrame:
    if connexa_engine is None:
        return pd.DataFrame()

    params = {
        "desde": desde,
        "hasta": hasta,
        "proveedor": proveedor,
    }
    with connexa_engine.connect() as con:
        return pd.read_sql(SQL_FORECAST_LOG_SUMMARY, con, params=params)


def get_forecast_log_events(
    connexa_engine: Engine,
    execution_execute_id: str,
) -> pd.DataFrame:
    if connexa_engine is None or not execution_execute_id:
        return pd.DataFrame()

    with connexa_engine.connect() as con:
        return pd.read_sql(
            SQL_FORECAST_LOG_EVENTS,
            con,
            params={"execution_execute_id": execution_execute_id},
        )

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SQL_SUPPLIER_DIM = text(
    """
    SELECT
        TRIM(c_proveedor::text) AS supplier_code,
        TRIM(n_proveedor) AS supplier_name
    FROM src.m_10_proveedores
    ORDER BY 2, 1;
    """
)


SQL_FORECAST_RESULT_DETAIL = text(
    """
    SELECT
        r.id AS result_id,
        date_trunc('day', r."timestamp")::date AS forecast_date,
        r."timestamp" AS forecast_ts,
        COALESCE(NULLIF(TRIM(r.ext_supplier_code), ''), r.supplier_id::text) AS supplier_code,
        r.supplier_id,
        --- COALESCE(NULLIF(TRIM(r.algorithm), ''), 'SIN_ALGORITMO') AS algorithm,
        COALESCE(
            NULLIF(
                TRIM(regexp_replace(r.algorithm, '^.*(ALGO_\d+)$', '\1')),
                ''
            ),
            'SIN_ALGORITMO'
        ) AS algorithm,
        COALESCE(NULLIF(TRIM(r.ext_product_code), ''), r.product_id::text) AS product_code,
        COALESCE(NULLIF(TRIM(r.ext_site_code), ''), r.site_id::text) AS site_code,
        r.average,
        COALESCE(r.forecast, r.forcast) AS forecast,
        r.quantity_confirmed,
        r.quantity_stock,
        r.pending_purchases,
        r.pending_transfer,
        r.pending_in_transit,
        r.window_sales_days,
        r.serviceable_days,
        r.approved,
        r.blocked_for_purchase,
        COALESCE(NULLIF(TRIM(r.reason), ''), '') AS reason
    FROM supply_planning.spl_supply_forecast_execution_execute_result r
    WHERE r."timestamp" >= :desde
      AND r."timestamp" < (:hasta + INTERVAL '1 day')
      AND (
        :proveedor IS NULL
        OR COALESCE(NULLIF(TRIM(r.ext_supplier_code), ''), r.supplier_id::text) = :proveedor
      )
      AND (
        :articulo IS NULL
        OR COALESCE(NULLIF(TRIM(r.ext_product_code), ''), r.product_id::text) = :articulo
      )
      AND (
        :sucursal IS NULL
        OR COALESCE(NULLIF(TRIM(r.ext_site_code), ''), r.site_id::text) = :sucursal
      )
    ORDER BY r."timestamp" DESC, r.id DESC;
    """
)


SQL_SALES_DAILY_AGG = text(
    """
    SELECT
        v.fecha::date AS sale_date,
        TRIM(v.codigo_articulo::text) AS product_code,
        TRIM(v.sucursal::text) AS site_code,
        SUM(COALESCE(v.unidades, 0)) AS units,
        SUM(COALESCE(v.importe_vendido::numeric, 0::numeric)) AS sales_amount,
        COUNT(*) AS price_rows,
        COUNT(DISTINCT v.precio) AS distinct_prices,
        MAX(v.c_proveedor_primario)::text AS supplier_code,
        MAX(v.nombre_articulo) AS product_name,
        MAX(v.familia) AS familia,
        MAX(v.rubro) AS rubro,
        MAX(v.subrubro) AS subrubro
    FROM src.base_ventas_extendida v
    WHERE v.fecha >= :desde
      AND v.fecha <= :hasta
      AND (
        :proveedor IS NULL
        OR COALESCE(TRIM(v.c_proveedor_primario::text), '') = :proveedor
      )
      AND (
        :articulo IS NULL
        OR TRIM(v.codigo_articulo::text) = :articulo
      )
      AND (
        :sucursal IS NULL
        OR TRIM(v.sucursal::text) = :sucursal
      )
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3;
    """
)


def get_supplier_dim(pg_engine: Engine) -> pd.DataFrame:
    if pg_engine is None:
        return pd.DataFrame()

    with pg_engine.connect() as con:
        return pd.read_sql(SQL_SUPPLIER_DIM, con)


def get_forecast_result_detail(
    pg_engine_connexa: Engine,
    desde: date,
    hasta: date,
    proveedor: str | None = None,
    articulo: str | None = None,
    sucursal: str | None = None,
) -> pd.DataFrame:
    if pg_engine_connexa is None:
        return pd.DataFrame()

    params = {
        "desde": desde,
        "hasta": hasta,
        "proveedor": proveedor,
        "articulo": articulo,
        "sucursal": sucursal,
    }
    with pg_engine_connexa.connect() as con:
        return pd.read_sql(SQL_FORECAST_RESULT_DETAIL, con, params=params)


def get_sales_daily_agg(
    pg_engine: Engine,
    desde: date,
    hasta: date,
    proveedor: str | None = None,
    articulo: str | None = None,
    sucursal: str | None = None,
) -> pd.DataFrame:
    if pg_engine is None:
        return pd.DataFrame()

    params = {
        "desde": desde,
        "hasta": hasta,
        "proveedor": proveedor,
        "articulo": articulo,
        "sucursal": sucursal,
    }
    with pg_engine.connect() as con:
        return pd.read_sql(SQL_SALES_DAILY_AGG, con, params=params)

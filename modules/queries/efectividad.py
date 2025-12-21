#efectividad.py
from datetime import date
from typing import Optional
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

"""
Agrupar:

SQL_FP_ESTADOS_PROP

SQL_FP_DETALLE

Tal vez reutilizar SQL_FP_CONVERSION_MENSUAL si se desea concentrar la cadena de efectividad aquí.

Funciones tipo:

get_estados_propuestas(pg_engine_connexa, desde, hasta)

get_detalle_forecast_propuesta(pg_engine_connexa, desde, hasta)
"""

# Estados de propuestas en el rango
SQL_FP_ESTADOS_PROP = text("""
WITH b AS (
  SELECT *
  FROM mon.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT pp_status, COUNT(*) AS propuestas
FROM b
GROUP BY pp_status
ORDER BY propuestas DESC NULLS LAST;
""")

# Detalle para exportar (rango)
SQL_FP_DETALLE = text("""
SELECT
  base_ts, fe_id, fe_start, fe_end, fe_status_name,
  pp_id, pp_number, pp_status, pp_open_at, pp_closed_at,
  buyer_id, user_name, pp_total_amount, pp_total_units, pp_total_products,
  exec_time_min, lead_open_min, adjust_time_min, ext_supplier_code, fe_supplier_id
FROM mon.v_forecast_propuesta_base
WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
ORDER BY base_ts DESC, pp_id NULLS LAST;
""")

# Serie mensual de conversión: ejecuciones de forecast (terminadas) vs propuestas generadas
SQL_FP_CONVERSION_MENSUAL = text("""
WITH b AS (
  SELECT *
  FROM mon.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
)
, fe_m AS (
  SELECT date_trunc('month', (COALESCE(fe_end, fe_start, fe_ts) AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
         COUNT(DISTINCT fe_id) AS ejecuciones
  FROM b
  WHERE fe_end IS NOT NULL  -- consideramos forecasts completados
  GROUP BY 1
)
, pp_m AS (
  SELECT date_trunc('month', (COALESCE(pp_open_at, base_ts) AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
         COUNT(DISTINCT pp_id) AS propuestas
  FROM b
  WHERE pp_id IS NOT NULL
  GROUP BY 1
)
SELECT
  COALESCE(fe_m.mes, pp_m.mes) AS mes,
  COALESCE(fe_m.ejecuciones, 0) AS ejecuciones,
  COALESCE(pp_m.propuestas, 0)  AS propuestas,
  CASE WHEN COALESCE(fe_m.ejecuciones,0) = 0 THEN NULL
       ELSE 1.0 * COALESCE(pp_m.propuestas,0) / fe_m.ejecuciones
  END AS conversion
FROM fe_m
FULL OUTER JOIN pp_m USING (mes)
ORDER BY mes;
""")

# Productividad por comprador (mensual): #propuestas, monto, tiempos P50/P90
SQL_FP_MENSUAL_COMPRADOR = text("""
WITH b AS (
  SELECT *
  FROM mon.v_forecast_propuesta_base
  WHERE base_ts >= :desde AND base_ts < (:hasta + INTERVAL '1 day')
    AND pp_id IS NOT NULL
)
SELECT
  date_trunc('month', (base_ts AT TIME ZONE 'America/Argentina/Buenos_Aires'))::date AS mes,
  COALESCE(NULLIF(trim(user_name), ''), buyer_id::text, '- sin comprador -')         AS comprador,
  COUNT(DISTINCT pp_id)                                                               AS propuestas,
  SUM(pp_total_amount)                                                                AS monto_total,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY adjust_time_min)                        AS p50_ajuste_min,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY adjust_time_min)                        AS p90_ajuste_min,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY lead_open_min)                          AS p50_lead_min,
  AVG(exec_time_min)                                                                  AS avg_exec_min
FROM b
GROUP BY 1, 2
ORDER BY mes, comprador;
""")


# ============================================================
# Funciones públicas — Efectividad
# ============================================================


def get_estados_propuestas(pg_engine_connexa, desde, hasta):
    """ Obtener conteo de propuestas por estado en el rango indicado. """
    with pg_engine_connexa.connect() as conn:
        result = conn.execute(SQL_FP_ESTADOS_PROP, {"desde": desde, "hasta": hasta})
        return result.fetchall()
    
def get_detalle_forecast_propuesta(pg_engine_connexa, desde, hasta):
    """ Obtener detalle de forecast y propuestas en el rango indicado. """
    with pg_engine_connexa.connect() as conn:
        result = conn.execute(SQL_FP_DETALLE, {"desde": desde, "hasta": hasta})
        return result.fetchall()
    
def get_conversion_mensual(pg_engine_connexa, desde, hasta):
    """ Obtener serie mensual de conversion forecast -> propuestas. """
    with pg_engine_connexa.connect() as conn:
        result = conn.execute(SQL_FP_CONVERSION_MENSUAL, {"desde": desde, "hasta": hasta})
        return result.fetchall()
    
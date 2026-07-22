# -*- coding: utf-8 -*-
"""Validacion y actualizacion masiva de parametros logisticos desde Excel."""

from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import Engine, text


KEY_COLUMNS = ["codigo_proveedor", "codigo_sucursal", "codigo_articulo"]
VALUE_COLUMNS = [
    "q_dias_stock",
    "q_dias_sobre_stock",
    "number_of_boxes_per_layer",
    "number_of_layers",
    "dias_preparacion",
]

COLUMN_ALIASES = {
    "cod_prov": "codigo_proveedor",
    "codigo_proveedor": "codigo_proveedor",
    "suc": "codigo_sucursal",
    "sucursal": "codigo_sucursal",
    "articulo": "codigo_articulo",
    "codigo_articulo": "codigo_articulo",
    "dias_stk": "q_dias_stock",
    "dias_sstk": "q_dias_sobre_stock",
    # PISO es la cantidad de cajas por capa; ALTURA, la cantidad de capas.
    "piso_pallet": "number_of_boxes_per_layer",
    "altura_pallet": "number_of_layers",
    "dias_de_preparacion": "dias_preparacion",
}

DISPLAY_COLUMNS = {
    "codigo_proveedor": "Cod Prov",
    "codigo_sucursal": "SUC",
    "codigo_articulo": "ARTICULO",
    "q_dias_stock": "DIAS STK",
    "q_dias_sobre_stock": "DIAS SSTK",
    "number_of_boxes_per_layer": "PISO PALLET",
    "number_of_layers": "ALTURA PALLET",
    "dias_preparacion": "DIAS DE PREPARACION",
}


@dataclass(frozen=True)
class ValidationResult:
    valid_rows: pd.DataFrame
    errors: pd.DataFrame
    duplicate_rows_removed: int = 0


def _normalize_header(value: object) -> str:
    value = unicodedata.normalize("NFKD", str(value).strip())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def read_and_validate_excel(file) -> ValidationResult:
    """Lee la primera hoja y devuelve filas validas y errores por fila."""
    source = pd.read_excel(
        file,
        sheet_name=0,
        dtype=object,
        usecols=lambda column: _normalize_header(column) in COLUMN_ALIASES,
    )
    normalized = [_normalize_header(column) for column in source.columns]
    if len(normalized) != len(set(normalized)):
        raise ValueError("La planilla contiene encabezados duplicados.")

    source.columns = normalized
    # Solo se exige el nombre canonico principal de cada columna; los aliases
    # alternativos se resuelven debajo.
    required_alias_groups = {
        "Cod Prov": ("cod_prov", "codigo_proveedor"),
        "SUC": ("suc", "sucursal"),
        "ARTICULO": ("articulo", "codigo_articulo"),
        "DIAS STK": ("dias_stk",),
        "DIAS SSTK": ("dias_sstk",),
        "PISO PALLET": ("piso_pallet",),
        "ALTURA PALLET": ("altura_pallet",),
        "DIAS DE PREPARACION": ("dias_de_preparacion",),
    }
    missing_labels = [
        label
        for label, aliases in required_alias_groups.items()
        if not any(alias in source.columns for alias in aliases)
    ]
    if missing_labels:
        raise ValueError("Faltan columnas obligatorias: " + ", ".join(missing_labels))

    rename = {
        column: COLUMN_ALIASES[column]
        for column in source.columns
        if column in COLUMN_ALIASES
    }
    work = source.rename(columns=rename)[KEY_COLUMNS + VALUE_COLUMNS].copy()
    if work.columns.duplicated().any():
        raise ValueError(
            "La planilla contiene mas de una columna equivalente para el mismo dato."
        )
    work.insert(0, "fila_excel", range(2, len(work) + 2))

    numeric_columns = KEY_COLUMNS + VALUE_COLUMNS
    error_messages: dict[int, list[str]] = {int(row): [] for row in work["fila_excel"]}
    for column in numeric_columns:
        original = work[column]
        converted = pd.to_numeric(original, errors="coerce")
        invalid = converted.isna() | (converted % 1 != 0)
        for row in work.loc[invalid, "fila_excel"]:
            error_messages[int(row)].append(
                f"{DISPLAY_COLUMNS[column]} debe ser un numero entero"
            )
        work[column] = converted.astype("Int64")

    for column in KEY_COLUMNS:
        invalid = work[column].notna() & (work[column] <= 0)
        for row in work.loc[invalid, "fila_excel"]:
            error_messages[int(row)].append(f"{DISPLAY_COLUMNS[column]} debe ser mayor a 0")

    for column in VALUE_COLUMNS:
        invalid = work[column].notna() & (work[column] < 0)
        for row in work.loc[invalid, "fila_excel"]:
            error_messages[int(row)].append(f"{DISPLAY_COLUMNS[column]} no puede ser negativo")

    initially_valid = work["fila_excel"].map(lambda row: not error_messages[int(row)])
    candidates = work.loc[initially_valid].copy()
    duplicate_rows_removed = 0
    if not candidates.empty:
        duplicate_mask = candidates.duplicated(KEY_COLUMNS, keep=False)
        duplicates = candidates.loc[duplicate_mask]
        for _, group in duplicates.groupby(KEY_COLUMNS, dropna=False):
            if group[VALUE_COLUMNS].drop_duplicates().shape[0] > 1:
                for row in group["fila_excel"]:
                    error_messages[int(row)].append(
                        "La clave proveedor/sucursal/articulo esta repetida con valores diferentes"
                    )
        conflict_rows = {
            row for row, messages in error_messages.items() if messages
        }
        candidates = candidates[~candidates["fila_excel"].isin(conflict_rows)]
        before = len(candidates)
        candidates = candidates.drop_duplicates(KEY_COLUMNS + VALUE_COLUMNS, keep="first")
        duplicate_rows_removed = before - len(candidates)

    errors = pd.DataFrame(
        [
            {"fila_excel": row, "errores": "; ".join(messages)}
            for row, messages in error_messages.items()
            if messages
        ]
    )
    valid_rows = candidates.reset_index(drop=True)
    return ValidationResult(valid_rows, errors, duplicate_rows_removed)


CREATE_STAGE_SQL = """
CREATE TEMP TABLE tmp_logistic_parameters (
    fila_excel integer NOT NULL,
    codigo_proveedor bigint NOT NULL,
    codigo_sucursal bigint NOT NULL,
    codigo_articulo bigint NOT NULL,
    q_dias_stock integer NOT NULL,
    q_dias_sobre_stock integer NOT NULL,
    number_of_boxes_per_layer integer NOT NULL,
    number_of_layers integer NOT NULL,
    dias_preparacion integer NOT NULL
) ON COMMIT DROP
"""

PREVIEW_SQL = """
WITH productos AS (
    SELECT p.c_proveedor_primario AS codigo_proveedor,
           p.c_sucu_empr AS codigo_sucursal,
           p.c_articulo AS codigo_articulo,
           count(*) AS coincidencias_producto,
           min(p.number_of_boxes_per_layer) AS piso_actual,
           min(p.number_of_layers) AS altura_actual
      FROM src.base_productos_vigentes p
      JOIN tmp_logistic_parameters t
        ON p.c_proveedor_primario = t.codigo_proveedor
       AND p.c_sucu_empr = t.codigo_sucursal
       AND p.c_articulo = t.codigo_articulo
     GROUP BY 1, 2, 3
), stock AS (
    SELECT s.codigo_proveedor, s.codigo_sucursal, s.codigo_articulo,
           count(*) AS coincidencias_stock,
           min(s.q_dias_stock) AS dias_stk_actual,
           min(s.q_dias_sobre_stock) AS dias_sstk_actual,
           min(s.dias_preparacion) AS preparacion_actual
      FROM src.base_stock_sucursal s
      JOIN tmp_logistic_parameters t
        ON s.codigo_proveedor = t.codigo_proveedor
       AND s.codigo_sucursal = t.codigo_sucursal
       AND s.codigo_articulo = t.codigo_articulo
     GROUP BY 1, 2, 3
)
SELECT t.fila_excel AS "Fila Excel",
       t.codigo_proveedor AS "Cod Prov", t.codigo_sucursal AS "SUC",
       t.codigo_articulo AS "ARTICULO",
       COALESCE(p.coincidencias_producto, 0) AS "Coincidencias productos",
       COALESCE(s.coincidencias_stock, 0) AS "Coincidencias stock",
       p.piso_actual AS "Piso actual", t.number_of_boxes_per_layer AS "Piso nuevo",
       p.altura_actual AS "Altura actual", t.number_of_layers AS "Altura nueva",
       s.dias_stk_actual AS "Dias STK actual", t.q_dias_stock AS "Dias STK nuevo",
       s.dias_sstk_actual AS "Dias SSTK actual", t.q_dias_sobre_stock AS "Dias SSTK nuevo",
       s.preparacion_actual AS "Preparacion actual", t.dias_preparacion AS "Preparacion nueva",
       CASE
         WHEN COALESCE(p.coincidencias_producto, 0) = 1
          AND COALESCE(s.coincidencias_stock, 0) = 1 THEN 'LISTO'
         WHEN COALESCE(p.coincidencias_producto, 0) = 0
          AND COALESCE(s.coincidencias_stock, 0) = 0 THEN 'NO EXISTE EN AMBAS TABLAS'
         WHEN COALESCE(p.coincidencias_producto, 0) = 0 THEN 'NO EXISTE EN PRODUCTOS'
         WHEN COALESCE(s.coincidencias_stock, 0) = 0 THEN 'NO EXISTE EN STOCK'
         ELSE 'CLAVE DUPLICADA EN BASE'
       END AS "Estado"
  FROM tmp_logistic_parameters t
  LEFT JOIN productos p USING (codigo_proveedor, codigo_sucursal, codigo_articulo)
  LEFT JOIN stock s USING (codigo_proveedor, codigo_sucursal, codigo_articulo)
 ORDER BY t.fila_excel
"""

CREATE_ELIGIBLE_SQL = """
CREATE TEMP TABLE tmp_logistic_eligible ON COMMIT DROP AS
WITH product_matches AS (
    SELECT t.fila_excel
      FROM tmp_logistic_parameters t
      JOIN src.base_productos_vigentes p
        ON p.c_proveedor_primario = t.codigo_proveedor
       AND p.c_sucu_empr = t.codigo_sucursal
       AND p.c_articulo = t.codigo_articulo
     GROUP BY t.fila_excel
    HAVING count(*) = 1
), stock_matches AS (
    SELECT t.fila_excel
      FROM tmp_logistic_parameters t
      JOIN src.base_stock_sucursal s
        ON s.codigo_proveedor = t.codigo_proveedor
       AND s.codigo_sucursal = t.codigo_sucursal
       AND s.codigo_articulo = t.codigo_articulo
     GROUP BY t.fila_excel
    HAVING count(*) = 1
)
SELECT t.*
  FROM tmp_logistic_parameters t
  JOIN product_matches p USING (fila_excel)
  JOIN stock_matches s USING (fila_excel)
"""


def _stage_rows(connection, rows: pd.DataFrame) -> None:
    connection.execute(text(CREATE_STAGE_SQL))
    columns = ["fila_excel"] + KEY_COLUMNS + VALUE_COLUMNS
    values = [
        tuple(int(value) for value in record)
        for record in rows[columns].itertuples(index=False, name=None)
    ]
    sql = """
        INSERT INTO tmp_logistic_parameters (
            fila_excel, codigo_proveedor, codigo_sucursal, codigo_articulo,
            q_dias_stock, q_dias_sobre_stock, number_of_boxes_per_layer,
            number_of_layers, dias_preparacion
        ) VALUES %s
    """
    cursor = connection.connection.cursor()
    try:
        execute_values(cursor, sql, values, page_size=10_000)
    finally:
        cursor.close()
    connection.execute(text("""
        CREATE UNIQUE INDEX tmp_logistic_parameters_key_idx
            ON tmp_logistic_parameters
               (codigo_proveedor, codigo_sucursal, codigo_articulo);
        CREATE UNIQUE INDEX tmp_logistic_parameters_row_idx
            ON tmp_logistic_parameters (fila_excel);
        ANALYZE tmp_logistic_parameters;
    """))


def preview_updates(engine: Engine, rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame()
    with engine.begin() as connection:
        _stage_rows(connection, rows)
        return pd.read_sql(text(PREVIEW_SQL), connection)


def apply_updates(engine: Engine, rows: pd.DataFrame) -> dict[str, int]:
    """Actualiza solo claves univocas presentes en ambas tablas, atomicamente."""
    if rows.empty:
        return {"filas_aplicadas": 0, "productos_actualizados": 0, "stock_actualizado": 0}

    with engine.begin() as connection:
        connection.execute(text("SET LOCAL statement_timeout = '5min'"))
        connection.execute(text("SET LOCAL lock_timeout = '30s'"))
        _stage_rows(connection, rows)
        connection.execute(text(CREATE_ELIGIBLE_SQL))
        connection.execute(text("CREATE UNIQUE INDEX ON tmp_logistic_eligible (fila_excel)"))
        connection.execute(text("ANALYZE tmp_logistic_eligible"))
        eligible = connection.execute(text("SELECT count(*) FROM tmp_logistic_eligible")).scalar_one()

        product_result = connection.execute(text("""
            UPDATE src.base_productos_vigentes p
               SET number_of_boxes_per_layer = t.number_of_boxes_per_layer,
                   number_of_layers = t.number_of_layers
              FROM tmp_logistic_eligible t
             WHERE p.c_proveedor_primario = t.codigo_proveedor
               AND p.c_sucu_empr = t.codigo_sucursal
               AND p.c_articulo = t.codigo_articulo
        """))
        stock_result = connection.execute(text("""
            UPDATE src.base_stock_sucursal s
               SET q_dias_stock = t.q_dias_stock,
                   q_dias_sobre_stock = t.q_dias_sobre_stock,
                   dias_preparacion = t.dias_preparacion
              FROM tmp_logistic_eligible t
             WHERE s.codigo_proveedor = t.codigo_proveedor
               AND s.codigo_sucursal = t.codigo_sucursal
               AND s.codigo_articulo = t.codigo_articulo
        """))
        if product_result.rowcount != eligible or stock_result.rowcount != eligible:
            raise RuntimeError("La cantidad actualizada no coincide entre las dos tablas; se revirtio la transaccion.")

    return {
        "filas_aplicadas": int(eligible),
        "productos_actualizados": int(product_result.rowcount),
        "stock_actualizado": int(stock_result.rowcount),
    }

from __future__ import annotations

from datetime import date
import uuid
from typing import Iterable, List, Set

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


SQL_SUPPLIER_DIM = text(
    """
WITH base AS (
    SELECT DISTINCT
        p.c_proveedor_primario::int AS c_proveedor,
        COALESCE(NULLIF(TRIM(pr.n_proveedor), ''), CAST(p.c_proveedor_primario AS text)) AS n_proveedor
    FROM src.base_productos_vigentes p
    LEFT JOIN src.m_10_proveedores pr
           ON pr.c_proveedor = p.c_proveedor_primario
    WHERE p.c_proveedor_primario IS NOT NULL
)
SELECT c_proveedor, n_proveedor
FROM base
ORDER BY n_proveedor, c_proveedor;
"""
)


SQL_PRODUCT_MAP = text(
    """
WITH ranked AS (
    SELECT
        p.c_sucu_empr::int AS dest_store_num,
        p.c_articulo::bigint AS item_code_num,
        p.c_proveedor_primario::int AS c_proveedor,
        COALESCE(NULLIF(TRIM(pr.n_proveedor), ''), CAST(p.c_proveedor_primario AS text)) AS n_proveedor,
        p.abastecimiento::text AS abastecimiento,
        p.cod_cd::text AS cod_cd,
        ROW_NUMBER() OVER (
            PARTITION BY p.c_sucu_empr, p.c_articulo
            ORDER BY p.c_proveedor_primario NULLS LAST
        ) AS rn
    FROM src.base_productos_vigentes p
    LEFT JOIN src.m_10_proveedores pr
           ON pr.c_proveedor = p.c_proveedor_primario
    WHERE p.c_sucu_empr IS NOT NULL
      AND p.c_articulo IS NOT NULL
)
SELECT
    dest_store_num,
    item_code_num,
    c_proveedor,
    n_proveedor,
    abastecimiento,
    cod_cd
FROM ranked
WHERE rn = 1;
"""
)


SQL_PRODUCT_MAP_BY_SUPPLIER = text(
    """
WITH ranked AS (
    SELECT
        p.c_sucu_empr::int AS dest_store_num,
        p.c_articulo::bigint AS item_code_num,
        p.c_proveedor_primario::int AS c_proveedor,
        COALESCE(NULLIF(TRIM(pr.n_proveedor), ''), CAST(p.c_proveedor_primario AS text)) AS n_proveedor,
        p.abastecimiento::text AS abastecimiento,
        p.cod_cd::text AS cod_cd,
        ROW_NUMBER() OVER (
            PARTITION BY p.c_sucu_empr, p.c_articulo
            ORDER BY p.c_proveedor_primario NULLS LAST
        ) AS rn
    FROM src.base_productos_vigentes p
    LEFT JOIN src.m_10_proveedores pr
           ON pr.c_proveedor = p.c_proveedor_primario
    WHERE p.c_sucu_empr IS NOT NULL
      AND p.c_articulo IS NOT NULL
      AND p.c_proveedor_primario = :proveedor
      AND p.c_sucu_empr = ANY(CAST(:lista_sucursales AS int[]))
      AND p.c_articulo = ANY(CAST(:lista_articulos AS bigint[]))
)
SELECT
    dest_store_num,
    item_code_num,
    c_proveedor,
    n_proveedor,
    abastecimiento,
    cod_cd
FROM ranked
WHERE rn = 1;
"""
)


SQL_PRODUCT_MAP_BY_CANDIDATES = text(
    """
WITH ranked AS (
    SELECT
        p.c_sucu_empr::int AS dest_store_num,
        p.c_articulo::bigint AS item_code_num,
        p.c_proveedor_primario::int AS c_proveedor,
        COALESCE(NULLIF(TRIM(pr.n_proveedor), ''), CAST(p.c_proveedor_primario AS text)) AS n_proveedor,
        p.abastecimiento::text AS abastecimiento,
        p.cod_cd::text AS cod_cd,
        ROW_NUMBER() OVER (
            PARTITION BY p.c_sucu_empr, p.c_articulo
            ORDER BY p.c_proveedor_primario NULLS LAST
        ) AS rn
    FROM src.base_productos_vigentes p
    LEFT JOIN src.m_10_proveedores pr
           ON pr.c_proveedor = p.c_proveedor_primario
    WHERE p.c_sucu_empr IS NOT NULL
      AND p.c_articulo IS NOT NULL
      AND p.c_sucu_empr = ANY(CAST(:lista_sucursales AS int[]))
      AND p.c_articulo = ANY(CAST(:lista_articulos AS bigint[]))
)
SELECT
    dest_store_num,
    item_code_num,
    c_proveedor,
    n_proveedor,
    abastecimiento,
    cod_cd
FROM ranked
WHERE rn = 1;
"""
)


SQL_CONNEXA_PENDING = text(
    """
SELECT
    d.id AS connexa_detail_uuid,
    h.id AS connexa_header_uuid,
    h.origin_cd,
    h.destination_store_code,
    h.connexa_purchase_code,
    h.requested_at,
    h.created_by,
    h.created_at,
    h.updated_at,
    h.status_id,
    s.code AS status_code,
    d.item_code,
    d.item_description,
    d.qty_requested,
    d.qty_planned,
    d.qty_shipped,
    d.qty_received,
    d.uom_id,
    d.units_per_package,
    d.packages_per_layer,
    d.layers_per_pallet
FROM supply_planning.spl_distribution_transfer_detail d
JOIN supply_planning.spl_distribution_transfer h
  ON d.distribution_transfer_id = h.id
JOIN supply_planning.spl_distribution_transfer_status s
  ON h.status_id = s.id
WHERE s.code = 'PRECARGA_CONNEXA';
"""
)


SQL_CONNEXA_RANGE = text(
    """
SELECT
    d.id AS connexa_detail_uuid,
    h.id AS connexa_header_uuid,
    h.origin_cd,
    h.destination_store_code,
    h.connexa_purchase_code,
    h.requested_at,
    h.created_by,
    h.created_at,
    h.updated_at,
    h.status_id,
    s.code AS status_code,
    d.item_code,
    d.item_description,
    d.qty_requested,
    d.qty_planned,
    d.qty_shipped,
    d.qty_received,
    d.uom_id,
    d.units_per_package,
    d.packages_per_layer,
    d.layers_per_pallet
FROM supply_planning.spl_distribution_transfer_detail d
JOIN supply_planning.spl_distribution_transfer h
  ON d.distribution_transfer_id = h.id
JOIN supply_planning.spl_distribution_transfer_status s
  ON h.status_id = s.id
WHERE COALESCE(h.requested_at, h.created_at, h.updated_at) >= :desde
  AND COALESCE(h.requested_at, h.created_at, h.updated_at) < (:hasta + INTERVAL '1 day');
"""
)


SQL_STOCK_BASE = text(
    """
SELECT
    codigo_articulo::bigint AS item_code_num,
    codigo_sucursal::bigint AS origin_cd_num,
    codigo_proveedor::bigint AS c_proveedor,
    COALESCE(stock, 0)::numeric AS stock_unidades,
    COALESCE(transfer_pendiente, 0)::numeric AS transfer_pendiente_unidades,
    COALESCE(pedido_pendiente, 0)::numeric AS pedido_pendiente_unidades,
    COALESCE(transito_pendiente, 0)::numeric AS transito_pendiente_unidades,
    COALESCE(factor_venta, 0)::numeric AS factor_venta,
    FLOOR(
        (COALESCE(stock, 0) + COALESCE(transfer_pendiente, 0))
        / NULLIF(COALESCE(factor_venta, 0), 0)
    )::numeric AS q_bultos_disponible_base
FROM src.base_stock_sucursal
WHERE codigo_sucursal = ANY(CAST(:lista_sucursales AS bigint[]))
  AND codigo_articulo = ANY(CAST(:lista_articulos AS bigint[]))
  AND codigo_proveedor = ANY(CAST(:lista_proveedores AS bigint[]))
  AND COALESCE(factor_venta, 0) > 0
GROUP BY codigo_articulo, codigo_sucursal, codigo_proveedor, stock, transfer_pendiente, pedido_pendiente, transito_pendiente, factor_venta;
"""
)


SQL_BLOCKLIST_EXISTS = text(
    """
SELECT to_regclass('audit.transfer_blocklist') IS NOT NULL AS exists;
"""
)


SQL_TRANSFER_BLOCKLIST_BY_IDS = text(
    """
SELECT
    LOWER(connexa_detail_uuid::text) AS connexa_detail_uuid,
    LOWER(connexa_header_uuid::text) AS connexa_header_uuid,
    motivo,
    usuario,
    observacion,
    active,
    created_at,
    updated_at
FROM audit.transfer_blocklist
WHERE active IS TRUE
  AND connexa_detail_uuid = ANY(CAST(:lista_ids AS uuid[]));
"""
)


SQL_TRANSFER_BLOCKLIST_UPSERT = text(
    """
INSERT INTO audit.transfer_blocklist (
    connexa_detail_uuid,
    connexa_header_uuid,
    motivo,
    usuario,
    observacion,
    active
)
VALUES (
    CAST(:connexa_detail_uuid AS uuid),
    CAST(:connexa_header_uuid AS uuid),
    :motivo,
    :usuario,
    :observacion,
    :active
)
ON CONFLICT (connexa_detail_uuid) DO UPDATE
   SET connexa_header_uuid = EXCLUDED.connexa_header_uuid,
       motivo             = EXCLUDED.motivo,
       usuario            = EXCLUDED.usuario,
       observacion        = EXCLUDED.observacion,
       active             = EXCLUDED.active,
       updated_at         = NOW();
"""
)


def _chunks(values: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _normalize_uuid_strings(values: Iterable[object]) -> List[str]:
    normalized: List[str] = []
    for value in values:
        if value is None:
            continue
        raw = str(value).strip().lower()
        if not raw:
            continue
        try:
            normalized.append(str(uuid.UUID(raw)))
        except Exception:
            continue
    return sorted(set(normalized))


def _empty_blocklist_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "connexa_detail_uuid",
            "connexa_header_uuid",
            "motivo",
            "usuario",
            "observacion",
            "active",
            "created_at",
            "updated_at",
        ]
    )


def transfer_blocklist_table_exists(pg_engine: Engine) -> bool:
    with pg_engine.connect() as conn:
        result = conn.execute(SQL_BLOCKLIST_EXISTS).scalar()
    return bool(result)


def ensure_transfer_blocklist_table(pg_engine: Engine) -> None:
    statements = [
        """
        CREATE SCHEMA IF NOT EXISTS audit;
        """,
        """
        CREATE TABLE IF NOT EXISTS audit.transfer_blocklist (
            connexa_detail_uuid uuid PRIMARY KEY,
            connexa_header_uuid uuid NULL,
            motivo text NOT NULL,
            usuario text NOT NULL,
            observacion text NULL,
            active boolean NOT NULL DEFAULT TRUE,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            updated_at timestamptz NOT NULL DEFAULT NOW()
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS ix_transfer_blocklist_active
            ON audit.transfer_blocklist (active, created_at DESC);
        """,
    ]

    with pg_engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def load_transfer_blocklist(pg_engine: Engine, detail_uuids: Iterable[object]) -> pd.DataFrame:
    ids = _normalize_uuid_strings(detail_uuids)
    if not ids or not transfer_blocklist_table_exists(pg_engine):
        return _empty_blocklist_df()

    with pg_engine.connect() as conn:
        df = pd.read_sql(
            SQL_TRANSFER_BLOCKLIST_BY_IDS,
            conn,
            params={"lista_ids": ids},
            parse_dates=["created_at", "updated_at"],
        )

    if df.empty:
        return _empty_blocklist_df()

    for col in ("connexa_detail_uuid", "connexa_header_uuid"):
        df[col] = df[col].fillna("").astype(str).str.strip().str.lower()
        df.loc[df[col].eq(""), col] = pd.NA

    for col in ("motivo", "usuario", "observacion"):
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["active"] = df["active"].fillna(False).astype(bool)
    return df


def upsert_transfer_blocklist(pg_engine: Engine, rows: Iterable[dict]) -> int:
    payload = []
    for row in rows:
        detail_uuid = _normalize_uuid_strings([row.get("connexa_detail_uuid")])
        if not detail_uuid:
            continue

        header_uuid = _normalize_uuid_strings([row.get("connexa_header_uuid")])
        payload.append(
            {
                "connexa_detail_uuid": detail_uuid[0],
                "connexa_header_uuid": header_uuid[0] if header_uuid else None,
                "motivo": str(row.get("motivo") or "").strip() or "BLOQUEO_MANUAL",
                "usuario": str(row.get("usuario") or "").strip() or "streamlit",
                "observacion": str(row.get("observacion") or "").strip() or None,
                "active": bool(row.get("active", True)),
            }
        )

    if not payload:
        return 0

    ensure_transfer_blocklist_table(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(SQL_TRANSFER_BLOCKLIST_UPSERT, payload)
    return len(payload)


def load_supplier_dim(pg_engine: Engine) -> pd.DataFrame:
    with pg_engine.connect() as conn:
        df = pd.read_sql(SQL_SUPPLIER_DIM, conn)

    if df.empty:
        return pd.DataFrame(columns=["c_proveedor", "n_proveedor"])

    df["c_proveedor"] = pd.to_numeric(df["c_proveedor"], errors="coerce").astype("Int64")
    df["n_proveedor"] = df["n_proveedor"].fillna("").astype(str).str.strip()
    return df


def load_product_map(pg_engine: Engine) -> pd.DataFrame:
    with pg_engine.connect() as conn:
        df = pd.read_sql(SQL_PRODUCT_MAP, conn)

    if df.empty:
        return pd.DataFrame(
            columns=["dest_store_num", "item_code_num", "c_proveedor", "n_proveedor", "abastecimiento", "cod_cd"]
        )

    for col in ("dest_store_num", "item_code_num", "c_proveedor"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ("n_proveedor", "abastecimiento", "cod_cd"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def _extract_positive_ints(series: pd.Series) -> List[int]:
    return sorted(
        {
            int(x)
            for x in pd.to_numeric(series, errors="coerce").dropna().tolist()
            if int(x) > 0
        }
    )


def load_product_map_for_supplier(
    pg_engine: Engine,
    df_norm: pd.DataFrame,
    proveedor: int,
) -> pd.DataFrame:
    if df_norm.empty:
        return pd.DataFrame(
            columns=["dest_store_num", "item_code_num", "c_proveedor", "n_proveedor", "abastecimiento", "cod_cd"]
        )

    sucursales = _extract_positive_ints(df_norm["dest_store_num"])
    articulos = _extract_positive_ints(df_norm["item_code_num"])

    if not sucursales or not articulos:
        return pd.DataFrame(
            columns=["dest_store_num", "item_code_num", "c_proveedor", "n_proveedor", "abastecimiento", "cod_cd"]
        )

    with pg_engine.connect() as conn:
        df = pd.read_sql(
            SQL_PRODUCT_MAP_BY_SUPPLIER,
            conn,
            params={
                "proveedor": int(proveedor),
                "lista_sucursales": sucursales,
                "lista_articulos": articulos,
            },
        )

    if df.empty:
        return pd.DataFrame(
            columns=["dest_store_num", "item_code_num", "c_proveedor", "n_proveedor", "abastecimiento", "cod_cd"]
        )

    for col in ("dest_store_num", "item_code_num", "c_proveedor"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ("n_proveedor", "abastecimiento", "cod_cd"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def load_product_map_for_candidates(
    pg_engine: Engine,
    df_norm: pd.DataFrame,
) -> pd.DataFrame:
    if df_norm.empty:
        return pd.DataFrame(
            columns=["dest_store_num", "item_code_num", "c_proveedor", "n_proveedor", "abastecimiento", "cod_cd"]
        )

    sucursales = _extract_positive_ints(df_norm["dest_store_num"])
    articulos = _extract_positive_ints(df_norm["item_code_num"])

    if not sucursales or not articulos:
        return pd.DataFrame(
            columns=["dest_store_num", "item_code_num", "c_proveedor", "n_proveedor", "abastecimiento", "cod_cd"]
        )

    with pg_engine.connect() as conn:
        df = pd.read_sql(
            SQL_PRODUCT_MAP_BY_CANDIDATES,
            conn,
            params={
                "lista_sucursales": sucursales,
                "lista_articulos": articulos,
            },
        )

    if df.empty:
        return pd.DataFrame(
            columns=["dest_store_num", "item_code_num", "c_proveedor", "n_proveedor", "abastecimiento", "cod_cd"]
        )

    for col in ("dest_store_num", "item_code_num", "c_proveedor"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ("n_proveedor", "abastecimiento", "cod_cd"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def load_connexa_pending_raw(connexa_engine: Engine) -> pd.DataFrame:
    return pd.read_sql(
        SQL_CONNEXA_PENDING,
        connexa_engine,
        parse_dates=["requested_at", "created_at", "updated_at"],
    )


def load_connexa_range_raw(connexa_engine: Engine, desde: date, hasta: date) -> pd.DataFrame:
    return pd.read_sql(
        SQL_CONNEXA_RANGE,
        connexa_engine,
        params={"desde": desde, "hasta": hasta},
        parse_dates=["requested_at", "created_at", "updated_at"],
    )


def normalize_transfer_df(df_src: pd.DataFrame) -> pd.DataFrame:
    if df_src.empty:
        return df_src.copy()

    df = df_src.copy()

    df["connexa_header_uuid"] = df["connexa_header_uuid"].astype(str).str.strip().str.lower()
    df["connexa_detail_uuid"] = df["connexa_detail_uuid"].astype(str).str.strip().str.lower()

    df["item_code_num"] = pd.to_numeric(df["item_code"], errors="coerce").astype("Int64")
    df["dest_store_num"] = pd.to_numeric(df["destination_store_code"], errors="coerce").astype("Int64")
    df["qty_requested_num"] = pd.to_numeric(df["qty_requested"], errors="coerce").fillna(0.0).round(3)
    df["qty_planned_num"] = pd.to_numeric(df["qty_planned"], errors="coerce").fillna(0.0).round(3)
    df["qty_shipped_num"] = pd.to_numeric(df["qty_shipped"], errors="coerce").fillna(0.0).round(3)
    df["qty_received_num"] = pd.to_numeric(df["qty_received"], errors="coerce").fillna(0.0).round(3)
    df["units_per_package"] = pd.to_numeric(df["units_per_package"], errors="coerce").fillna(1.0)
    df.loc[df["units_per_package"] <= 0, "units_per_package"] = 1.0

    origin_str = df["origin_cd"].astype(str)
    df["origin_cd_num"] = (
        origin_str.str.extract(r"^(\d+)", expand=False)
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )

    return df


def enrich_with_supplier(df_transfers: pd.DataFrame, df_product_map: pd.DataFrame) -> pd.DataFrame:
    if df_transfers.empty:
        return df_transfers.copy()

    out = df_transfers.merge(
        df_product_map,
        how="left",
        on=["item_code_num", "dest_store_num"],
    )

    return out


def filter_supplier_rows(df_enriched: pd.DataFrame, proveedor: int) -> pd.DataFrame:
    if df_enriched.empty or "c_proveedor" not in df_enriched.columns:
        return pd.DataFrame(columns=df_enriched.columns)

    mask = pd.to_numeric(df_enriched["c_proveedor"], errors="coerce") == int(proveedor)
    return df_enriched[mask].copy()


def load_stock_base(pg_engine: Engine, df_norm: pd.DataFrame) -> pd.DataFrame:
    if df_norm.empty:
        return pd.DataFrame(
            columns=[
                "item_code_num",
                "origin_cd_num",
                "c_proveedor",
                "stock_unidades",
                "transfer_pendiente_unidades",
                "pedido_pendiente_unidades",
                "transito_pendiente_unidades",
                "factor_venta",
                "q_bultos_disponible_base",
            ]
        )

    articulos = _extract_positive_ints(df_norm["item_code_num"])
    sucursales = _extract_positive_ints(df_norm["origin_cd_num"])
    proveedores = _extract_positive_ints(df_norm["c_proveedor"])

    if not articulos or not sucursales or not proveedores:
        return pd.DataFrame(
            columns=[
                "item_code_num",
                "origin_cd_num",
                "c_proveedor",
                "stock_unidades",
                "transfer_pendiente_unidades",
                "pedido_pendiente_unidades",
                "transito_pendiente_unidades",
                "factor_venta",
                "q_bultos_disponible_base",
            ]
        )

    with pg_engine.connect() as conn:
        df = pd.read_sql(
            SQL_STOCK_BASE,
            conn,
            params={
                "lista_sucursales": sucursales,
                "lista_articulos": articulos,
                "lista_proveedores": proveedores,
            },
        )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "item_code_num",
                "origin_cd_num",
                "c_proveedor",
                "stock_unidades",
                "transfer_pendiente_unidades",
                "pedido_pendiente_unidades",
                "transito_pendiente_unidades",
                "factor_venta",
                "q_bultos_disponible_base",
            ]
        )

    for col in ("item_code_num", "origin_cd_num", "c_proveedor"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in (
        "stock_unidades",
        "transfer_pendiente_unidades",
        "pedido_pendiente_unidades",
        "transito_pendiente_unidades",
        "factor_venta",
        "q_bultos_disponible_base",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["q_bultos_disponible_base"] = df["q_bultos_disponible_base"].clip(lower=0.0)
    return df


def load_aco_valkimia(sql_engine: Engine, df_norm: pd.DataFrame, chunk_size: int = 300) -> pd.DataFrame:
    if df_norm.empty or "item_code_num" not in df_norm.columns:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "bultos_aco_valkimia"])

    skus = _extract_positive_ints(df_norm["item_code_num"])

    if not skus:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "bultos_aco_valkimia"])

    frames: List[pd.DataFrame] = []

    for bloque in _chunks([str(x) for x in skus], chunk_size):
        placeholders = ", ".join([f":p{j}" for j in range(len(bloque))])
        sql = text(
            f"""
SELECT
    CAST([INIArtId] AS bigint) AS item_code_num,
    SUM(CAST([INICnt1] AS decimal(18,3))) AS bultos_aco_valkimia
FROM [DIARCO-VKMSQL\\SQL2008R2].[VALKIMIA].[dbo].[IntNecIN]
WHERE INIEst = 'ACO'
  AND [INIEntId] < 300
  AND [INICnt1] > 0
  AND [INIArtId] IN ({placeholders})
GROUP BY [INIArtId];
"""
        )

        params = {f"p{j}": int(bloque[j]) for j in range(len(bloque))}
        with sql_engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["item_code_num", "origin_cd_num", "bultos_aco_valkimia"])

    out = pd.concat(frames, ignore_index=True)
    out["item_code_num"] = pd.to_numeric(out["item_code_num"], errors="coerce").astype("Int64")
    out["bultos_aco_valkimia"] = pd.to_numeric(out["bultos_aco_valkimia"], errors="coerce").fillna(0.0)
    out["origin_cd_num"] = 41
    out = out.groupby(["item_code_num", "origin_cd_num"], as_index=False)["bultos_aco_valkimia"].sum()
    return out


def load_staging_status(sql_engine: Engine, detail_uuids: List[str], chunk_size: int = 300) -> pd.DataFrame:
    ids = sorted({str(x).strip().lower() for x in detail_uuids if str(x).strip()})
    if not ids:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []

    for bloque in _chunks(ids, chunk_size):
        placeholders = ", ".join([f":p{j}" for j in range(len(bloque))])
        sql = text(
            f"""
WITH ranked AS (
    SELECT
        id AS dmz_id,
        LOWER(LTRIM(RTRIM(connexa_header_uuid))) AS connexa_header_uuid,
        LOWER(LTRIM(RTRIM(connexa_detail_uuid))) AS connexa_detail_uuid,
        c_articulo,
        c_sucu_dest,
        c_sucu_orig,
        q_bultos,
        q_factor,
        u_id_sincro,
        estado,
        mensaje_error,
        estado_vk,
        mensaje_error_vk,
        f_alta,
        f_procesado,
        f_procesado_vk,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(LTRIM(RTRIM(connexa_detail_uuid)))
            ORDER BY id DESC
        ) AS rn
    FROM repl.TRANSF_CONNEXA_IN
    WHERE connexa_detail_uuid IN ({placeholders})
)
SELECT
    dmz_id,
    connexa_header_uuid,
    connexa_detail_uuid,
    c_articulo,
    c_sucu_dest,
    c_sucu_orig,
    q_bultos,
    q_factor,
    u_id_sincro,
    estado,
    mensaje_error,
    estado_vk,
    mensaje_error_vk,
    f_alta,
    f_procesado,
    f_procesado_vk
FROM ranked
WHERE rn = 1;
"""
        )

        params = {f"p{j}": bloque[j] for j in range(len(bloque))}
        with sql_engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params, parse_dates=["f_alta", "f_procesado", "f_procesado_vk"])
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["connexa_detail_uuid"] = out["connexa_detail_uuid"].astype(str).str.strip().str.lower()
    return out


def load_vk_latest_status(sql_engine: Engine, detail_uuids: List[str], chunk_size: int = 300) -> pd.DataFrame:
    ids = sorted({str(x).strip().lower() for x in detail_uuids if str(x).strip()})
    if not ids:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []

    for bloque in _chunks(ids, chunk_size):
        placeholders = ", ".join([f":p{j}" for j in range(len(bloque))])
        sql = text(
            f"""
SELECT
    LOWER(LTRIM(RTRIM(connexa_header_uuid))) AS connexa_header_uuid,
    LOWER(LTRIM(RTRIM(connexa_detail_uuid))) AS connexa_detail_uuid,
    u_id_sincro,
    INIId,
    INIIdSincro,
    INIEst,
    INIFecEnt,
    INIFecReg,
    INIFecEst,
    INIDepId,
    INIEntId,
    INIArtId,
    INIArtC,
    INIUxB,
    INICnt1,
    INICnt2,
    INIMRecibido,
    INIMotPed,
    INICnt2Rem,
    INICnt1Rem,
    INICnt2Pre,
    INICnt1Pre,
    INILinPrio,
    EmpId
FROM [data-sync].[repl].[V_CONNEXA_VK_ULTIMO_ESTADO_LINEA]
WHERE connexa_detail_uuid IN ({placeholders});
"""
        )

        params = {f"p{j}": bloque[j] for j in range(len(bloque))}
        with sql_engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params, parse_dates=["INIFecEnt", "INIFecReg", "INIFecEst"])
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["connexa_detail_uuid"] = out["connexa_detail_uuid"].astype(str).str.strip().str.lower()
    return out


def enrich_with_stock_and_snd(
    df_norm: pd.DataFrame,
    df_stock_base: pd.DataFrame,
    df_aco_valkimia: pd.DataFrame,
) -> pd.DataFrame:
    if df_norm.empty:
        return df_norm.copy()

    df = df_norm.merge(df_stock_base, how="left", on=["item_code_num", "origin_cd_num", "c_proveedor"])
    df = df.merge(df_aco_valkimia, how="left", on=["item_code_num", "origin_cd_num"])

    for col in (
        "stock_unidades",
        "transfer_pendiente_unidades",
        "pedido_pendiente_unidades",
        "transito_pendiente_unidades",
        "factor_venta",
    ):
        df[col] = pd.to_numeric(df.get(col), errors="coerce").fillna(0.0)

    df["q_bultos_disponible_base"] = pd.to_numeric(df.get("q_bultos_disponible_base"), errors="coerce").fillna(0.0)
    df["bultos_aco_valkimia"] = pd.to_numeric(df.get("bultos_aco_valkimia"), errors="coerce").fillna(0.0)
    df["q_bultos_disponible"] = (df["q_bultos_disponible_base"] - df["bultos_aco_valkimia"]).clip(lower=0.0)
    return df


def merge_transfer_blocklist(df_base: pd.DataFrame, df_blocklist: pd.DataFrame) -> pd.DataFrame:
    out = df_base.copy()
    if df_base.empty:
        return out

    if df_blocklist.empty:
        out["bloqueada_manual"] = False
        out["bloqueo_motivo"] = ""
        out["bloqueo_usuario"] = ""
        out["bloqueo_observacion"] = ""
        out["bloqueo_created_at"] = pd.NaT
        return out

    block = df_blocklist.rename(
        columns={
            "motivo": "bloqueo_motivo",
            "usuario": "bloqueo_usuario",
            "observacion": "bloqueo_observacion",
            "created_at": "bloqueo_created_at",
        }
    )
    block["bloqueada_manual"] = True

    out = out.merge(
        block[
            [
                "connexa_detail_uuid",
                "bloqueada_manual",
                "bloqueo_motivo",
                "bloqueo_usuario",
                "bloqueo_observacion",
                "bloqueo_created_at",
            ]
        ],
        how="left",
        on="connexa_detail_uuid",
    )

    out["bloqueada_manual"] = out["bloqueada_manual"].fillna(False).astype(bool)
    for col in ("bloqueo_motivo", "bloqueo_usuario", "bloqueo_observacion"):
        out[col] = out[col].fillna("").astype(str).str.strip()
    return out


def mark_already_published(df: pd.DataFrame, df_staging: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    publicados: Set[str] = set()

    if not df_staging.empty and "connexa_detail_uuid" in df_staging.columns:
        publicados = set(df_staging["connexa_detail_uuid"].astype(str).str.strip().str.lower().tolist())

    out["ya_publicado"] = out["connexa_detail_uuid"].isin(publicados)
    return out


def assign_stock_todo_o_nada(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    work = df.copy()
    work["requested_at_ord"] = pd.to_datetime(work["requested_at"], errors="coerce")
    work["created_at_ord"] = pd.to_datetime(work["created_at"], errors="coerce")
    work["qty_requested_num"] = pd.to_numeric(work["qty_requested_num"], errors="coerce").fillna(0.0).round(3)
    work["q_bultos_disponible"] = pd.to_numeric(work["q_bultos_disponible"], errors="coerce").fillna(0.0)

    work = work.sort_values(
        by=["origin_cd_num", "item_code_num", "requested_at_ord", "created_at_ord", "connexa_detail_uuid"],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    ).copy()

    resultados: List[pd.Series] = []

    for _, grp in work.groupby(["origin_cd_num", "item_code_num"], dropna=False, sort=False):
        grp = grp.copy()
        saldo = float(grp["q_bultos_disponible"].iloc[0]) if len(grp) else 0.0
        saldo_inicial = saldo

        for _, row in grp.iterrows():
            row = row.copy()
            qty = float(row["qty_requested_num"]) if pd.notna(row["qty_requested_num"]) else 0.0

            row["saldo_inicial_grupo"] = round(saldo_inicial, 3)
            row["saldo_antes"] = round(saldo, 3)

            if bool(row.get("ya_publicado", False)):
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = True
                row["motivo_no_publicado"] = ""
                row["saldo_despues"] = round(saldo, 3)
                resultados.append(row)
                continue

            if bool(row.get("bloqueada_manual", False)):
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = False
                row["motivo_no_publicado"] = "BLOQUEADA_MANUALMENTE"
                row["saldo_despues"] = round(saldo, 3)
                resultados.append(row)
                continue

            if qty <= 0:
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = False
                row["motivo_no_publicado"] = "QTY_REQUESTED_INVALIDA"
                row["saldo_despues"] = round(saldo, 3)
                resultados.append(row)
                continue

            if saldo >= qty:
                saldo -= qty
                row["q_bultos_asignado"] = round(qty, 3)
                row["publicable"] = True
                row["motivo_no_publicado"] = ""
                row["saldo_despues"] = round(saldo, 3)
            else:
                row["q_bultos_asignado"] = 0.0
                row["publicable"] = False
                row["motivo_no_publicado"] = "SIN_STOCK_SUFICIENTE"
                row["saldo_despues"] = round(saldo, 3)

            resultados.append(row)

    out = pd.DataFrame(resultados)
    out["publicable_ahora"] = out["publicable"] & (~out["ya_publicado"])
    return out


def merge_downstream_status(
    df_base: pd.DataFrame,
    df_staging: pd.DataFrame,
    df_vk: pd.DataFrame,
) -> pd.DataFrame:
    out = df_base.copy()

    if not df_staging.empty:
        stg = df_staging.rename(
            columns={
                "dmz_id": "dmz_id",
                "u_id_sincro": "dmz_u_id_sincro",
                "estado": "dmz_estado",
                "mensaje_error": "dmz_mensaje_error",
                "estado_vk": "dmz_estado_vk",
                "mensaje_error_vk": "dmz_mensaje_error_vk",
                "f_alta": "dmz_f_alta",
                "f_procesado": "dmz_f_procesado",
                "f_procesado_vk": "dmz_f_procesado_vk",
                "q_bultos": "dmz_q_bultos",
                "q_factor": "dmz_q_factor",
            }
        )
        out = out.merge(
            stg[
                [
                    "connexa_detail_uuid",
                    "dmz_id",
                    "dmz_u_id_sincro",
                    "dmz_estado",
                    "dmz_mensaje_error",
                    "dmz_estado_vk",
                    "dmz_mensaje_error_vk",
                    "dmz_f_alta",
                    "dmz_f_procesado",
                    "dmz_f_procesado_vk",
                    "dmz_q_bultos",
                    "dmz_q_factor",
                ]
            ],
            how="left",
            on="connexa_detail_uuid",
        )

    if not df_vk.empty:
        vk = df_vk.rename(
            columns={
                "u_id_sincro": "vk_u_id_sincro",
                "INIId": "vk_INIId",
                "INIIdSincro": "vk_INIIdSincro",
                "INIEst": "vk_INIEst",
                "INIFecEnt": "vk_INIFecEnt",
                "INIFecReg": "vk_INIFecReg",
                "INIFecEst": "vk_INIFecEst",
                "INIDepId": "vk_INIDepId",
                "INIEntId": "vk_INIEntId",
                "INIArtId": "vk_INIArtId",
                "INIArtC": "vk_INIArtC",
                "INICnt1": "vk_INICnt1",
                "INICnt2": "vk_INICnt2",
                "INIMRecibido": "vk_INIMRecibido",
                "INIMotPed": "vk_INIMotPed",
                "INICnt2Rem": "vk_INICnt2Rem",
                "INICnt1Rem": "vk_INICnt1Rem",
                "INICnt2Pre": "vk_INICnt2Pre",
                "INICnt1Pre": "vk_INICnt1Pre",
            }
        )
        out = out.merge(
            vk[
                [
                    "connexa_detail_uuid",
                    "vk_u_id_sincro",
                    "vk_INIId",
                    "vk_INIIdSincro",
                    "vk_INIEst",
                    "vk_INIFecEnt",
                    "vk_INIFecReg",
                    "vk_INIFecEst",
                    "vk_INIDepId",
                    "vk_INIEntId",
                    "vk_INIArtId",
                    "vk_INIArtC",
                    "vk_INICnt1",
                    "vk_INICnt2",
                    "vk_INIMRecibido",
                    "vk_INIMotPed",
                    "vk_INICnt2Rem",
                    "vk_INICnt1Rem",
                    "vk_INICnt2Pre",
                    "vk_INICnt1Pre",
                ]
            ],
            how="left",
            on="connexa_detail_uuid",
        )

    out["estado_operativo"] = out.apply(_derive_operational_state, axis=1)
    return out


def _derive_operational_state(row: pd.Series) -> str:
    vk_estado = str(row.get("vk_INIEst") or "").strip().upper()
    dmz_estado_vk = str(row.get("dmz_estado_vk") or "").strip().upper()
    dmz_estado = str(row.get("dmz_estado") or "").strip().upper()
    connexa_estado = str(row.get("status_code") or "").strip().upper()

    if vk_estado:
        return f"VALKIMIA::{vk_estado}"
    if dmz_estado_vk == "PROCESADO":
        return "VALKIMIA::PROCESADO"
    if dmz_estado_vk == "ERROR":
        return "VALKIMIA::ERROR"
    if dmz_estado_vk == "EN_PROCESO":
        return "VALKIMIA::EN_PROCESO"
    if dmz_estado_vk == "PENDIENTE":
        return "VALKIMIA::PENDIENTE"
    if dmz_estado == "PROCESADO":
        return "SGM::PROCESADO"
    if dmz_estado == "DUPLICADO":
        return "SGM::DUPLICADO"
    if dmz_estado == "ERROR":
        return "SGM::ERROR"
    if dmz_estado == "EN_PROCESO":
        return "SGM::EN_PROCESO"
    if dmz_estado == "PENDIENTE":
        return "SGM::PENDIENTE"

    if connexa_estado == "PRECARGA_CONNEXA":
        if bool(row.get("bloqueada_manual", False)):
            return "CONNEXA::BLOQUEADA_MANUAL"
        if bool(row.get("ya_publicado", False)):
            return "CONNEXA::YA_INSERTADA_DMZ"
        if bool(row.get("publicable_ahora", False)):
            return "CONNEXA::LISTA_PARA_PUBLICAR"
        if bool(row.get("publicable", False)):
            return "CONNEXA::YA_PUBLICADA"
        return "CONNEXA::BLOQUEADA_SIN_STOCK"

    if connexa_estado:
        return f"CONNEXA::{connexa_estado}"

    return "SIN_ESTADO"


def build_current_pending_snapshot(
    connexa_engine: Engine,
    pg_engine: Engine,
    sql_engine: Engine,
    proveedor: int,
) -> pd.DataFrame:
    df_raw = load_connexa_pending_raw(connexa_engine)
    if df_raw.empty:
        return pd.DataFrame()

    df_norm = normalize_transfer_df(df_raw)
    df_product_map = load_product_map_for_supplier(pg_engine, df_norm, proveedor)
    df_norm = enrich_with_supplier(df_norm, df_product_map)
    df_norm = filter_supplier_rows(df_norm, proveedor)
    if df_norm.empty:
        return pd.DataFrame()

    df_staging = load_staging_status(sql_engine, df_norm["connexa_detail_uuid"].tolist())
    df_aco = load_aco_valkimia(sql_engine, df_norm)
    df_stock = load_stock_base(pg_engine, df_norm)
    df_blocklist = load_transfer_blocklist(pg_engine, df_norm["connexa_detail_uuid"].tolist())

    df_work = enrich_with_stock_and_snd(df_norm, df_stock, df_aco)
    df_work = merge_transfer_blocklist(df_work, df_blocklist)
    df_work = mark_already_published(df_work, df_staging)
    df_work = assign_stock_todo_o_nada(df_work)

    df_vk = load_vk_latest_status(sql_engine, df_work["connexa_detail_uuid"].tolist())
    df_work = merge_downstream_status(df_work, df_staging, df_vk)
    return df_work


def build_history_snapshot(
    connexa_engine: Engine,
    pg_engine: Engine,
    sql_engine: Engine,
    desde: date,
    hasta: date,
    proveedor: int,
) -> pd.DataFrame:
    df_raw = load_connexa_range_raw(connexa_engine, desde, hasta)
    if df_raw.empty:
        return pd.DataFrame()

    df_hist = normalize_transfer_df(df_raw)
    df_product_map = load_product_map_for_supplier(pg_engine, df_hist, proveedor)
    df_hist = enrich_with_supplier(df_hist, df_product_map)
    df_hist = filter_supplier_rows(df_hist, proveedor)
    if df_hist.empty:
        return pd.DataFrame()

    df_staging = load_staging_status(sql_engine, df_hist["connexa_detail_uuid"].tolist())
    df_blocklist = load_transfer_blocklist(pg_engine, df_hist["connexa_detail_uuid"].tolist())
    df_hist = merge_transfer_blocklist(df_hist, df_blocklist)
    df_hist = mark_already_published(df_hist, df_staging)
    df_vk = load_vk_latest_status(sql_engine, df_hist["connexa_detail_uuid"].tolist())
    df_hist = merge_downstream_status(df_hist, df_staging, df_vk)
    return df_hist


def build_current_pending_control_snapshot(
    connexa_engine: Engine,
    pg_engine: Engine,
    sql_engine: Engine,
) -> pd.DataFrame:
    df_raw = load_connexa_pending_raw(connexa_engine)
    if df_raw.empty:
        return pd.DataFrame()

    df_pending = normalize_transfer_df(df_raw)
    df_product_map = load_product_map_for_candidates(pg_engine, df_pending)
    df_pending = enrich_with_supplier(df_pending, df_product_map)

    df_staging = load_staging_status(sql_engine, df_pending["connexa_detail_uuid"].tolist())
    df_blocklist = load_transfer_blocklist(pg_engine, df_pending["connexa_detail_uuid"].tolist())
    df_pending = merge_transfer_blocklist(df_pending, df_blocklist)
    df_pending = mark_already_published(df_pending, df_staging)
    df_vk = load_vk_latest_status(sql_engine, df_pending["connexa_detail_uuid"].tolist())
    df_pending = merge_downstream_status(df_pending, df_staging, df_vk)
    return df_pending

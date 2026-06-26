# -*- coding: utf-8 -*-
"""
Utilidad operativa para precargas de OC Connexa en SGM.

Acciones disponibles:
- Recuperar registros sin OC desde T874 hacia T080.
- Limpiar bloqueos en T080.
"""

import os
from datetime import date
from typing import Iterable

import pandas as pd
import streamlit as st
from dotenv import dotenv_values
from sqlalchemy import text
from sqlalchemy.engine import Engine

from modules.db import get_sqlserver_prod_engine


st.set_page_config(
    page_title="Utilidad Precargas SGM",
    layout="wide",
)

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))

DB_NAME = os.getenv("SQLP_DATABASE", "DiarcoP")
T080 = f"[{DB_NAME}].[dbo].[T080_OC_PRECARGA_KIKKER]"
T874 = f"[{DB_NAME}].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]"
T020 = f"[{DB_NAME}].[dbo].[T020_PROVEEDOR]"
PROVEEDORES_UNICOS = f"""
(
    SELECT [C_PROVEEDOR], MAX([N_PROVEEDOR]) AS [N_PROVEEDOR]
    FROM {T020}
    GROUP BY [C_PROVEEDOR]
)
"""

KEY_COLUMNS = ["C_PROVEEDOR", "C_ARTICULO", "C_SUCU_EMPR", "C_COMPRA_KIKKER"]
NO_OC_CONDITION = """
(
    COALESCE(NULLIF(LTRIM(RTRIM(CAST({alias}.[U_PREFIJO_OC] AS varchar(50)))), ''), '0') = '0'
    AND COALESCE(NULLIF(LTRIM(RTRIM(CAST({alias}.[U_SUFIJO_OC] AS varchar(50)))), ''), '0') = '0'
)
"""


def _current_user() -> str:
    return (
        os.getenv("CONNEXA_OPERATOR")
        or os.getenv("USERNAME")
        or os.getenv("USER")
        or "streamlit"
    ).strip()


def _authorized_users() -> set[str]:
    env_file_values = dotenv_values(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
    raw = (
        env_file_values.get("CONNEXA_OC_UTIL_AUTHORIZED_USERS")
        or os.getenv("CONNEXA_OC_UTIL_AUTHORIZED_USERS")
        or env_file_values.get("CONNEXA_MAINTENANCE_USERS")
        or os.getenv("CONNEXA_MAINTENANCE_USERS")
        or ""
    )
    return {item.strip().lower() for item in raw.replace(";", ",").split(",") if item.strip()}


def _is_authorized(user: str) -> bool:
    allowed = _authorized_users()
    return bool(allowed) and user.strip().lower() in allowed


def _action_disabled_reason(user: str, can_write: bool, confirmar: bool) -> str:
    if not can_write:
        allowed = _authorized_users()
        if allowed:
            return f"Usuario `{user}` no autorizado para ejecutar cambios."
        return f"Configure `CONNEXA_OC_UTIL_AUTHORIZED_USERS={user}` en `.env` para habilitar acciones."
    if not confirmar:
        return "Marque `Confirmo operar sobre SQL Server produccion` en la barra lateral."
    return ""


def _key_params(row: pd.Series) -> tuple[str, str, str, str]:
    return tuple(str(row.get(col.lower(), row.get(col, "")) or "").strip() for col in KEY_COLUMNS)  # type: ignore


def _sql_key_condition(alias: str) -> str:
    return " AND ".join(
        f"CAST({alias}.[{col}] AS varchar(100)) = CAST(? AS varchar(100))"
        for col in KEY_COLUMNS
    )


def _read_raw_query(engine: Engine, sql: str, params: Iterable = ()) -> pd.DataFrame:
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(sql, tuple(params))
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame.from_records(rows, columns=cols)
    finally:
        raw.close()


@st.cache_data(ttl=TTL, show_spinner=False)
def load_historico_sin_oc(
    _engine: Engine,
    proveedor: str,
    proveedor_nombre: str,
    comprador: str,
    fecha_alta: date | None,
    limite: int,
) -> pd.DataFrame:
    where = [NO_OC_CONDITION.format(alias="H")]
    params: list[str | int] = []

    if proveedor:
        where.append("CAST(H.[C_PROVEEDOR] AS varchar(100)) = CAST(? AS varchar(100))")
        params.append(proveedor)
    if proveedor_nombre:
        where.append("UPPER(COALESCE(P.[N_PROVEEDOR], '')) LIKE ?")
        params.append(f"%{proveedor_nombre.upper()}%")
    if comprador:
        where.append("CAST(H.[C_COMPRADOR] AS varchar(100)) = CAST(? AS varchar(100))")
        params.append(comprador)
    if fecha_alta:
        where.append("TRY_CONVERT(date, H.[F_ALTA_SIST]) = ?")
        params.append(fecha_alta)

    params.append(int(limite))
    sql = f"""
    SELECT TOP (?)
        H.[C_PROVEEDOR] AS c_proveedor,
        COALESCE(P.[N_PROVEEDOR], '') AS n_proveedor,
        H.[C_COMPRADOR] AS c_comprador,
        H.[C_COMPRA_KIKKER] AS c_compra_kikker,
        H.[C_ARTICULO] AS c_articulo,
        H.[C_SUCU_EMPR] AS c_sucu_empr,
        H.[Q_BULTOS_KILOS_DIARCO] AS q_bultos_kilos_diarco,
        H.[F_ALTA_SIST] AS f_alta_sist,
        H.[C_USUARIO_BLOQUEO] AS c_usuario_bloqueo,
        H.[M_PROCESADO] AS m_procesado,
        H.[F_PROCESADO] AS f_procesado,
        H.[U_PREFIJO_OC] AS u_prefijo_oc,
        H.[U_SUFIJO_OC] AS u_sufijo_oc,
        H.[C_USUARIO_MODIF] AS c_usuario_modif
    FROM {T874} H
    LEFT JOIN {PROVEEDORES_UNICOS} P
        ON H.[C_PROVEEDOR] = P.[C_PROVEEDOR]
    WHERE {" AND ".join(where)}
    ORDER BY H.[F_ALTA_SIST] DESC, H.[C_PROVEEDOR], H.[C_COMPRA_KIKKER], H.[C_ARTICULO], H.[C_SUCU_EMPR]
    """
    return _read_raw_query(_engine, sql, [params[-1], *params[:-1]])


@st.cache_data(ttl=TTL, show_spinner=False)
def count_bloqueados_t080(_engine: Engine, proveedor: str, comprador: str) -> int:
    where = ["NULLIF(LTRIM(RTRIM(COALESCE(CAST(T.[C_USUARIO_BLOQUEO] AS varchar(100)), ''))), '') IS NOT NULL"]
    params: list[str] = []

    if proveedor:
        where.append("CAST(T.[C_PROVEEDOR] AS varchar(100)) = CAST(? AS varchar(100))")
        params.append(proveedor)
    if comprador:
        where.append("CAST(T.[C_COMPRADOR] AS varchar(100)) = CAST(? AS varchar(100))")
        params.append(comprador)

    sql = f"""
    SELECT COUNT(*)
    FROM {T080} T
    WHERE {" AND ".join(where)}
    """
    raw = _engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute(sql, tuple(params))
        total = int(cur.fetchone()[0])
        cur.close()
        return total
    finally:
        raw.close()


@st.cache_data(ttl=TTL, show_spinner=False)
def load_bloqueados(_engine: Engine, proveedor: str, comprador: str, limite: int) -> pd.DataFrame:
    where = ["NULLIF(LTRIM(RTRIM(COALESCE(CAST(T.[C_USUARIO_BLOQUEO] AS varchar(100)), ''))), '') IS NOT NULL"]
    params: list[str | int] = []

    if proveedor:
        where.append("CAST(T.[C_PROVEEDOR] AS varchar(100)) = CAST(? AS varchar(100))")
        params.append(proveedor)
    if comprador:
        where.append("CAST(T.[C_COMPRADOR] AS varchar(100)) = CAST(? AS varchar(100))")
        params.append(comprador)

    params.append(int(limite))
    sql = f"""
    SELECT TOP (?)
        T.[C_PROVEEDOR] AS c_proveedor,
        COALESCE(P.[N_PROVEEDOR], '') AS n_proveedor,
        T.[C_COMPRADOR] AS c_comprador,
        T.[C_COMPRA_KIKKER] AS c_compra_kikker,
        T.[C_ARTICULO] AS c_articulo,
        T.[C_SUCU_EMPR] AS c_sucu_empr,
        T.[Q_BULTOS_KILOS_DIARCO] AS q_bultos_kilos_diarco,
        T.[F_ALTA_SIST] AS f_alta_sist,
        T.[C_USUARIO_BLOQUEO] AS c_usuario_bloqueo,
        T.[M_PROCESADO] AS m_procesado,
        T.[F_PROCESADO] AS f_procesado,
        T.[U_PREFIJO_OC] AS u_prefijo_oc,
        T.[U_SUFIJO_OC] AS u_sufijo_oc
    FROM {T080} T
    LEFT JOIN {PROVEEDORES_UNICOS} P
        ON T.[C_PROVEEDOR] = P.[C_PROVEEDOR]
    WHERE {" AND ".join(where)}
    ORDER BY T.[F_ALTA_SIST] DESC, T.[C_PROVEEDOR], T.[C_COMPRA_KIKKER], T.[C_ARTICULO], T.[C_SUCU_EMPR]
    """
    return _read_raw_query(_engine, sql, [params[-1], *params[:-1]])


def _insertable_columns(cur) -> list[str]:
    sql = f"""
    SELECT c.name
    FROM sys.columns c
    JOIN sys.types ty
        ON c.user_type_id = ty.user_type_id
    WHERE c.object_id = OBJECT_ID(N'{DB_NAME}.dbo.T080_OC_PRECARGA_KIKKER')
      AND c.is_identity = 0
      AND c.is_computed = 0
      AND ty.name NOT IN ('timestamp', 'rowversion')
      AND EXISTS (
          SELECT 1
          FROM sys.columns h
          WHERE h.object_id = OBJECT_ID(N'{DB_NAME}.dbo.T874_OC_PRECARGA_KIKKER_HIST')
            AND h.name = c.name
      )
    ORDER BY c.column_id
    """
    cur.execute(sql)
    return [str(row[0]) for row in cur.fetchall()]


def recuperar_desde_historico(engine: Engine, row: pd.Series) -> str:
    key_params = _key_params(row)
    key_where_h = _sql_key_condition("H")
    key_where_t = _sql_key_condition("T")
    no_oc_h = NO_OC_CONDITION.format(alias="H")
    raw = engine.raw_connection()
    try:
        raw.autocommit = False
        cur = raw.cursor()
        columns = _insertable_columns(cur)
        if not columns:
            raise RuntimeError("No se pudieron determinar columnas comunes entre T874 y T080.")

        col_list = ", ".join(f"[{col}]" for col in columns)

        cur.execute(
            f"SELECT COUNT(*) FROM {T874} H WITH (UPDLOCK, HOLDLOCK) WHERE {key_where_h} AND {no_oc_h}",
            key_params,
        )
        hist_count = int(cur.fetchone()[0])
        if hist_count != 1:
            raw.rollback()
            return f"No se recuperó: la clave encontró {hist_count} registros en T874."

        cur.execute(
            f"SELECT COUNT(*) FROM {T080} T WITH (UPDLOCK, HOLDLOCK) WHERE {key_where_t}",
            key_params,
        )
        target_count = int(cur.fetchone()[0])
        if target_count > 0:
            raw.rollback()
            return "No se recuperó: ya existe un registro con esa clave en T080."

        cur.execute(
            f"""
            INSERT INTO {T080} ({col_list})
            SELECT {col_list}
            FROM {T874} H
            WHERE {key_where_h}
              AND {no_oc_h}
              AND NOT EXISTS (
                  SELECT 1
                  FROM {T080} T
                  WHERE {key_where_t}
              )
            """,
            (*key_params, *key_params),
        )
        cur.execute("SELECT @@ROWCOUNT")
        inserted = int(cur.fetchone()[0])
        if inserted != 1:
            raw.rollback()
            return f"No se recuperó: se esperaba insertar 1 registro y se insertaron {inserted}."

        cur.execute(
            f"DELETE H FROM {T874} H WHERE {_sql_key_condition('H')} AND {NO_OC_CONDITION.format(alias='H')}",
            key_params,
        )
        cur.execute("SELECT @@ROWCOUNT")
        deleted = int(cur.fetchone()[0])
        if deleted != 1:
            raw.rollback()
            return f"No se recuperó: se esperaba eliminar 1 registro de T874 y se eliminaron {deleted}."

        raw.commit()
        return "Registro recuperado en T080 y eliminado de T874."
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def recuperar_visibles_desde_historico(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    raw = engine.raw_connection()
    summary = {
        "recuperados": 0,
        "omitidos_t874": 0,
        "omitidos_t080": 0,
        "omitidos_insert": 0,
        "omitidos_delete": 0,
    }

    try:
        raw.autocommit = False
        cur = raw.cursor()
        columns = _insertable_columns(cur)
        if not columns:
            raise RuntimeError("No se pudieron determinar columnas comunes entre T874 y T080.")

        col_list = ", ".join(f"[{col}]" for col in columns)
        key_where_h = _sql_key_condition("H")
        key_where_t = _sql_key_condition("T")
        no_oc_h = NO_OC_CONDITION.format(alias="H")

        for _, row in df.iterrows():
            key_params = _key_params(row)

            cur.execute(
                f"SELECT COUNT(*) FROM {T874} H WITH (UPDLOCK, HOLDLOCK) WHERE {key_where_h} AND {no_oc_h}",
                key_params,
            )
            hist_count = int(cur.fetchone()[0])
            if hist_count != 1:
                summary["omitidos_t874"] += 1
                continue

            cur.execute(
                f"SELECT COUNT(*) FROM {T080} T WITH (UPDLOCK, HOLDLOCK) WHERE {key_where_t}",
                key_params,
            )
            target_count = int(cur.fetchone()[0])
            if target_count > 0:
                summary["omitidos_t080"] += 1
                continue

            cur.execute(
                f"""
                INSERT INTO {T080} ({col_list})
                SELECT {col_list}
                FROM {T874} H
                WHERE {key_where_h}
                  AND {no_oc_h}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {T080} T
                      WHERE {key_where_t}
                  )
                """,
                (*key_params, *key_params),
            )
            cur.execute("SELECT @@ROWCOUNT")
            inserted = int(cur.fetchone()[0])
            if inserted != 1:
                summary["omitidos_insert"] += 1
                continue

            cur.execute(
                f"DELETE H FROM {T874} H WHERE {_sql_key_condition('H')} AND {NO_OC_CONDITION.format(alias='H')}",
                key_params,
            )
            cur.execute("SELECT @@ROWCOUNT")
            deleted = int(cur.fetchone()[0])
            if deleted != 1:
                raise RuntimeError(
                    "La recuperacion masiva inserto un registro, pero no pudo eliminar el origen en T874."
                )

            summary["recuperados"] += 1

        raw.commit()
        return summary
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def desbloquear_t080(engine: Engine, row: pd.Series) -> str:
    key_params = _key_params(row)
    raw = engine.raw_connection()
    try:
        raw.autocommit = False
        cur = raw.cursor()
        cur.execute(
            f"""
            UPDATE T
               SET [C_USUARIO_BLOQUEO] = ''
            FROM {T080} T
            WHERE {_sql_key_condition("T")}
              AND NULLIF(LTRIM(RTRIM(COALESCE(CAST(T.[C_USUARIO_BLOQUEO] AS varchar(100)), ''))), '') IS NOT NULL
            """,
            key_params,
        )
        cur.execute("SELECT @@ROWCOUNT")
        updated = int(cur.fetchone()[0])
        if updated != 1:
            raw.rollback()
            return f"No se desbloqueó: se esperaba actualizar 1 registro y se actualizaron {updated}."

        raw.commit()
        return "Registro desbloqueado."
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def desbloquear_visibles_t080(engine: Engine, df: pd.DataFrame) -> dict[str, int]:
    raw = engine.raw_connection()
    summary = {
        "desbloqueados": 0,
        "omitidos": 0,
    }

    try:
        raw.autocommit = False
        cur = raw.cursor()
        for _, row in df.iterrows():
            key_params = _key_params(row)
            cur.execute(
                f"""
                UPDATE T
                   SET [C_USUARIO_BLOQUEO] = ''
                FROM {T080} T
                WHERE {_sql_key_condition("T")}
                  AND NULLIF(LTRIM(RTRIM(COALESCE(CAST(T.[C_USUARIO_BLOQUEO] AS varchar(100)), ''))), '') IS NOT NULL
                """,
                key_params,
            )
            cur.execute("SELECT @@ROWCOUNT")
            updated = int(cur.fetchone()[0])
            if updated == 1:
                summary["desbloqueados"] += 1
            else:
                summary["omitidos"] += 1

        raw.commit()
        return summary
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def _show_action_rows(df: pd.DataFrame, action_label: str, disabled: bool, action_fn, engine: Engine, key_prefix: str) -> None:
    if df.empty:
        return

    st.caption("Acciones por fila sobre la clave Proveedor + Compra + Articulo + Sucursal.")
    header = st.columns([0.8, 1.4, 1.0, 1.0, 1.0, 1.0, 1.2])
    for col, title in zip(header, ["Proveedor", "Nombre", "Compra", "Articulo", "Sucursal", "Bloqueo", "Accion"]):
        col.markdown(f"**{title}**")

    for idx, row in df.head(100).iterrows():
        cols = st.columns([0.8, 1.4, 1.0, 1.0, 1.0, 1.0, 1.2])
        cols[0].write(str(row.get("c_proveedor", "")))
        cols[1].write(str(row.get("n_proveedor", ""))[:36])
        cols[2].write(str(row.get("c_compra_kikker", "")))
        cols[3].write(str(row.get("c_articulo", "")))
        cols[4].write(str(row.get("c_sucu_empr", "")))
        cols[5].write(str(row.get("c_usuario_bloqueo", "")))
        if cols[6].button(action_label, key=f"{key_prefix}_{idx}", disabled=disabled):
            try:
                message = action_fn(engine, row)
                st.cache_data.clear()
                if message.startswith("Registro"):
                    st.success(message)
                    st.rerun()
                else:
                    st.warning(message)
            except Exception as exc:
                st.error(f"No se pudo ejecutar la accion: {exc}")

    if len(df) > 100:
        st.info("Se muestran botones para las primeras 100 filas. Ajuste los filtros para operar sobre otro registro.")


def main() -> None:
    st.title("Utilidad de precargas SGM")
    st.caption("Recuperacion T874 -> T080 y desbloqueo de registros en T080.")

    user = _current_user()
    can_write = _is_authorized(user)

    with st.sidebar:
        st.subheader("Conexion")
        try:
            engine = get_sqlserver_prod_engine()
            if engine is None:
                raise RuntimeError("Faltan variables SQLP_* para SQL Server Produccion.")
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            st.success("SQL Server SGM: OK")
        except Exception as exc:
            st.error(f"SQL Server SGM no disponible: {exc}")
            st.stop()

        st.divider()
        st.subheader("Permisos")
        st.write(f"Usuario detectado: `{user}`")
        if can_write:
            st.success("Acciones habilitadas.")
        elif _authorized_users():
            st.warning("Usuario sin autorizacion para ejecutar cambios.")
        else:
            st.warning("Configure CONNEXA_OC_UTIL_AUTHORIZED_USERS para habilitar acciones.")

        confirmar = st.checkbox("Confirmo operar sobre SQL Server produccion", value=False)
        limite = st.number_input("Limite de filas", min_value=10, max_value=1000, value=200, step=10)
        if st.button("Refrescar datos"):
            st.cache_data.clear()
            st.rerun()

    action_disabled = not (can_write and confirmar)
    action_disabled_reason = _action_disabled_reason(user, can_write, confirmar)
    if action_disabled_reason:
        st.warning(f"Acciones deshabilitadas: {action_disabled_reason}")

    tab_hist, tab_lock = st.tabs([
        "1. Recuperacion desde historico",
        "2. Desbloqueo T080",
    ])

    with tab_hist:
        st.subheader("Precargas en T874 sin OC generada")
        f1, f2, f3, f4 = st.columns([1, 2, 1, 1])
        proveedor = f1.text_input("C_PROVEEDOR", value="", placeholder="Ej.: 925").strip()
        proveedor_nombre = f2.text_input("Nombre proveedor", value="", placeholder="Texto contenido en N_PROVEEDOR").strip()
        comprador = f3.text_input("C_COMPRADOR", value="", placeholder="Ej.: 12").strip()
        usar_fecha_alta = f4.checkbox("Filtrar F_ALTA_SIST", value=False)
        fecha_alta = None
        if usar_fecha_alta:
            fecha_alta = f4.date_input("Fecha alta", value=date.today())

        df_hist = load_historico_sin_oc(engine, proveedor, proveedor_nombre, comprador, fecha_alta, int(limite))
        if df_hist.empty:
            st.info("No se encontraron registros historicos sin OC para los filtros indicados.")
        else:
            st.metric("Registros encontrados", f"{len(df_hist):,}")
            st.dataframe(df_hist, width="stretch", hide_index=True)
            st.download_button(
                "Descargar historico filtrado",
                data=df_hist.to_csv(index=False).encode("utf-8"),
                file_name="t874_sin_oc_filtrado.csv",
                mime="text/csv",
            )

            if len(df_hist) >= int(limite):
                st.warning("El listado alcanzo el limite configurado. El recupero masivo operara solo sobre estas filas visibles.")

            c_bulk, c_hint = st.columns([1, 3])
            with c_bulk:
                if st.button(
                    f"Recuperar visibles ({len(df_hist):,})",
                    disabled=action_disabled,
                    type="primary",
                    key="recuperar_visibles",
                ):
                    try:
                        summary = recuperar_visibles_desde_historico(engine, df_hist)
                        st.cache_data.clear()
                        st.success(
                            "Recuperacion masiva finalizada. "
                            f"Recuperados: {summary['recuperados']:,}. "
                            f"Omitidos por no existir/unicidad T874: {summary['omitidos_t874']:,}. "
                            f"Omitidos por duplicado en T080: {summary['omitidos_t080']:,}."
                        )
                        if summary["omitidos_insert"] or summary["omitidos_delete"]:
                            st.warning(
                                "Hubo filas omitidas por validaciones de insercion/eliminacion. "
                                f"Insert: {summary['omitidos_insert']:,}; Delete: {summary['omitidos_delete']:,}."
                            )
                        st.rerun()
                    except Exception as exc:
                        st.error(f"No se pudo ejecutar la recuperacion masiva: {exc}")
            with c_hint:
                st.caption("La accion masiva respeta los filtros actuales y el limite de filas visible.")

            _show_action_rows(df_hist, "Recuperar", action_disabled, recuperar_desde_historico, engine, "recuperar")

    with tab_lock:
        st.subheader("Precargas bloqueadas en T080")
        st.caption(f"Fuente de consulta y desbloqueo: {T080}. Esta seccion no consulta T874.")
        l1, l2 = st.columns(2)
        lock_proveedor = l1.text_input("C_PROVEEDOR bloqueado", value="", placeholder="Ej.: 3835").strip()
        lock_comprador = l2.text_input("C_COMPRADOR bloqueado", value="", placeholder="Ej.: 12").strip()

        total_bloqueados_t080 = count_bloqueados_t080(engine, lock_proveedor, lock_comprador)
        df_lock = load_bloqueados(engine, lock_proveedor, lock_comprador, int(limite))
        if df_lock.empty:
            st.success("No hay registros bloqueados en T080 para los filtros indicados.")
        else:
            c_total, c_visible = st.columns(2)
            c_total.metric("Total bloqueados en T080 filtrados", f"{total_bloqueados_t080:,}")
            c_visible.metric("Registros visibles", f"{len(df_lock):,}")
            if len(df_lock) < total_bloqueados_t080:
                st.warning("El listado esta limitado por el valor configurado en la barra lateral.")
            st.dataframe(df_lock, width="stretch", hide_index=True)
            st.download_button(
                "Descargar bloqueados",
                data=df_lock.to_csv(index=False).encode("utf-8"),
                file_name="t080_bloqueados.csv",
                mime="text/csv",
            )
            c_unlock, c_unlock_hint = st.columns([1, 3])
            with c_unlock:
                if st.button(
                    f"Desbloquear visibles ({len(df_lock):,})",
                    disabled=action_disabled,
                    type="primary",
                    key="desbloquear_visibles",
                ):
                    try:
                        summary = desbloquear_visibles_t080(engine, df_lock)
                        st.cache_data.clear()
                        st.success(
                            "Desbloqueo masivo finalizado. "
                            f"Desbloqueados: {summary['desbloqueados']:,}. "
                            f"Omitidos: {summary['omitidos']:,}."
                        )
                        st.rerun()
                    except Exception as exc:
                        st.error(f"No se pudo ejecutar el desbloqueo masivo: {exc}")
            with c_unlock_hint:
                st.caption("La accion masiva respeta los filtros actuales y el limite de filas visible.")

            _show_action_rows(df_lock, "Desbloquear", action_disabled, desbloquear_t080, engine, "desbloquear")


if __name__ == "__main__":
    main()

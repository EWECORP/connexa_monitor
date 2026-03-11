# -*- coding: utf-8 -*-
# 13_Auditoria_Compra_Agil_MS.py
"""
Aplicación Streamlit: Auditoría de Compra Ágil / Compra Directa
(Connexa → Pre-carga Connexa → SGM)

Objetivo
--------
Auditar de punta a punta el flujo de pedidos originados en Compra Ágil
desde Connexa, su consolidación en pre-carga (diarco_data) y su
tránsito/confirmación en SGM (SQL Server).

Origen Connexa
--------------
Cabecera:
  supply_planning.spl_agile_buyer_purchase

Detalle:
  supply_planning.spl_agile_buyer_purchase_supplier_product_site

Downstream
----------
Postgres diarco_data:
  public.t080_oc_precarga_connexa

SQL Server SGM:
  [data-sync].[dbo].[V_T080_OC_PRECARGA_KIKKER]
  [data-sync].[dbo].[V_T874_OC_PRECARGA_KIKKER_HIST]

Claves funcionales
------------------
Proveedor:
  - Connexa: supplier_code
  - diarco_data: c_proveedor
  - SQL Server: C_PROVEEDOR

Compra:
  - Connexa: purchase_code
  - diarco_data: c_compra_connexa
  - SQL Server: C_COMPRA_KIKKER
"""

import os
import logging
from typing import Tuple

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Engine

from modules.db import (
    get_connexa_engine,
    get_diarco_engine,
    get_sqlserver_engine,
)

# =========================
# Configuración base / logging
# =========================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("auditoria_compra_agil")

st.set_page_config(
    page_title="Auditoría Compra Ágil SGM",
    layout="wide"
)

ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))  # 5 minutos

# =========================
# Consultas Connexa
# =========================

SQL_DISTINCT_SUPPLIERS_AGILE = """
SELECT DISTINCT
    TRIM(CAST(supplier_code AS text)) AS ext_code_supplier,
    MAX(TRIM(CAST(supplier_name AS text))) AS supplier_name
FROM supply_planning.spl_agile_buyer_purchase_supplier_product_site
WHERE supplier_code IS NOT NULL
GROUP BY TRIM(CAST(supplier_code AS text))
ORDER BY 1;
"""

SQL_DISTINCT_PURCHASES_BY_SUPPLIER = """
SELECT DISTINCT
    p.id,
    p.created_at,
    p.purchase_code,
    p.status,
    p.name,
    TRIM(CAST(d.supplier_code AS text)) AS ext_code_supplier,
    d.supplier_name,
    p.user_id
FROM supply_planning.spl_agile_buyer_purchase p
JOIN supply_planning.spl_agile_buyer_purchase_supplier_product_site d
    ON d.agile_purchase_id = p.id
WHERE TRIM(CAST(d.supplier_code AS text)) = TRIM(CAST(:ext_code_supplier AS text))
ORDER BY p.created_at DESC, p.purchase_code DESC;
"""

SQL_AGILE_PURCHASE_HEADER = """
SELECT
    p.id,
    p.created_at,
    p.name,
    p.supplier_id,
    p.user_id,
    p.status,
    p.purchase_code
FROM supply_planning.spl_agile_buyer_purchase p
WHERE p.purchase_code = :purchase_code
"""

SQL_AGILE_PURCHASE_DETAIL = """
SELECT
    d.id,
    d.agile_purchase_id,
    d.approved,
    d.purchase_price,
    d.last_purchase_date,
    d.last_purchase_quantity,
    d.occupation_pallet,
    d.product_code,
    d.product_description,
    d.product_id,
    d.purchased_units,
    d.quantity_confirmed,
    d.pending_quantity,
    d.quantity_sold_first_half_month,
    d.quantity_sold_second_half_month,
    d.requested_units,
    d.site_code,
    d.site_id,
    d.site_name,
    d.stock,
    TRIM(CAST(d.supplier_code AS text)) AS supplier_code,
    d.supplier_id,
    d.supplier_name,
    d.total_price,
    d.bulk_units,
    d.litter,
    d.pallet,
    d.pallet_height,
    d.stock_days,
    d.pending_transfer,
    d.purchase_code
FROM supply_planning.spl_agile_buyer_purchase_supplier_product_site d
WHERE d.purchase_code = :purchase_code
  AND TRIM(CAST(d.supplier_code AS text)) = TRIM(CAST(:ext_code_supplier AS text))
ORDER BY d.site_code, d.product_code
"""

# =========================
# Consultas Pre-carga / SGM
# =========================

SQL_PRECARGA_PG = """
SELECT
    c_proveedor,
    c_articulo,
    c_sucu_empr,
    q_bultos_kilos_diarco,
    f_alta_sist,
    c_usuario_genero_oc,
    c_terminal_genero_oc,
    f_genero_oc,
    c_usuario_bloqueo,
    m_procesado,
    f_procesado,
    u_prefijo_oc,
    u_sufijo_oc,
    c_compra_connexa,
    c_usuario_modif,
    c_comprador,
    m_publicado
FROM public.t080_oc_precarga_connexa
WHERE CAST(c_compra_connexa AS text) = CAST(:purchase_code AS text)
  AND CAST(c_proveedor AS text) = CAST(:ext_code_supplier AS text)
"""

SQL_PRECARGA_SQL = """
SELECT
    [C_PROVEEDOR]             AS c_proveedor,
    [C_ARTICULO]              AS c_articulo,
    [C_SUCU_EMPR]             AS c_sucu_empr,
    [Q_BULTOS_KILOS_DIARCO]   AS q_bultos_kilos_diarco,
    [F_ALTA_SIST]             AS f_alta_sist,
    [C_USUARIO_GENERO_OC]     AS c_usuario_genero_oc,
    [C_TERMINAL_GENERO_OC]    AS c_terminal_genero_oc,
    [F_GENERO_OC]             AS f_genero_oc,
    [C_USUARIO_BLOQUEO]       AS c_usuario_bloqueo,
    [M_PROCESADO]             AS m_procesado,
    [F_PROCESADO]             AS f_procesado,
    [U_PREFIJO_OC]            AS u_prefijo_oc,
    [U_SUFIJO_OC]             AS u_sufijo_oc,
    [C_COMPRA_KIKKER]         AS c_compra_kikker,
    [C_USUARIO_MODIF]         AS c_usuario_modif,
    [C_COMPRADOR]             AS c_comprador
FROM [data-sync].[dbo].[V_T080_OC_PRECARGA_KIKKER]
WHERE CAST([C_COMPRA_KIKKER] AS varchar(100)) = CAST(? AS varchar(100))
  AND CAST([C_PROVEEDOR] AS varchar(100)) = CAST(? AS varchar(100))
"""

SQL_PRECARGA_HIST_SQL = """
SELECT
    [C_PROVEEDOR]             AS c_proveedor,
    [C_ARTICULO]              AS c_articulo,
    [C_SUCU_EMPR]             AS c_sucu_empr,
    [Q_BULTOS_KILOS_DIARCO]   AS q_bultos_kilos_diarco,
    [F_ALTA_SIST]             AS f_alta_sist,
    [C_USUARIO_GENERO_OC]     AS c_usuario_genero_oc,
    [C_TERMINAL_GENERO_OC]    AS c_terminal_genero_oc,
    [F_GENERO_OC]             AS f_genero_oc,
    [C_USUARIO_BLOQUEO]       AS c_usuario_bloqueo,
    [M_PROCESADO]             AS m_procesado,
    [F_PROCESADO]             AS f_procesado,
    [U_PREFIJO_OC]            AS u_prefijo_oc,
    [U_SUFIJO_OC]             AS u_sufijo_oc,
    [C_COMPRA_KIKKER]         AS c_compra_kikker,
    [C_USUARIO_MODIF]         AS c_usuario_modif,
    [C_COMPRADOR]             AS c_comprador
FROM [data-sync].[dbo].[V_T874_OC_PRECARGA_KIKKER_HIST]
WHERE CAST([C_COMPRA_KIKKER] AS varchar(100)) = CAST(? AS varchar(100))
  AND CAST([C_PROVEEDOR] AS varchar(100)) = CAST(? AS varchar(100))
"""

# =========================
# Capa de datos
# =========================

@st.cache_data(ttl=ttl, show_spinner=False)
def listar_suppliers_agile(_engine: Engine) -> pd.DataFrame:
    with _engine.connect() as conn:
        df = pd.read_sql(text(SQL_DISTINCT_SUPPLIERS_AGILE), conn)
    if not df.empty:
        df["ext_code_supplier"] = df["ext_code_supplier"].astype(str).str.strip()
        df["supplier_name"] = df["supplier_name"].fillna("").astype(str).str.strip()
    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def listar_purchases(_engine: Engine, ext_code_supplier: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        df = pd.read_sql(
            text(SQL_DISTINCT_PURCHASES_BY_SUPPLIER),
            conn,
            params={"ext_code_supplier": str(ext_code_supplier).strip()},
        )
    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def cargar_agile_header(_engine: Engine, purchase_code: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        df = pd.read_sql(
            text(SQL_AGILE_PURCHASE_HEADER),
            conn,
            params={"purchase_code": str(purchase_code).strip()},
        )
    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def cargar_agile_detail(_engine: Engine, purchase_code: str, ext_code_supplier: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        df = pd.read_sql(
            text(SQL_AGILE_PURCHASE_DETAIL),
            conn,
            params={
                "purchase_code": str(purchase_code).strip(),
                "ext_code_supplier": str(ext_code_supplier).strip(),
            },
        )
    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def cargar_precarga_pg(_engine: Engine, purchase_code: str, ext_code_supplier: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        df = pd.read_sql(
            text(SQL_PRECARGA_PG),
            conn,
            params={
                "purchase_code": str(purchase_code).strip(),
                "ext_code_supplier": str(ext_code_supplier).strip(),
            },
        )
    return df


@st.cache_data(ttl=ttl, show_spinner=False)
def cargar_precarga_sqlserver(_engine: Engine, purchase_code: str, ext_code_supplier: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_cur = pd.DataFrame()
    df_hist = pd.DataFrame()

    raw = _engine.raw_connection()
    try:
        cur = raw.cursor()

        cur.execute(SQL_PRECARGA_SQL, (str(purchase_code).strip(), str(ext_code_supplier).strip()))
        cols = [c[0] for c in cur.description]
        df_cur = pd.DataFrame.from_records(cur.fetchall(), columns=cols)
        cur.close()

        cur = raw.cursor()
        cur.execute(SQL_PRECARGA_HIST_SQL, (str(purchase_code).strip(), str(ext_code_supplier).strip()))
        cols = [c[0] for c in cur.description]
        df_hist = pd.DataFrame.from_records(cur.fetchall(), columns=cols)
        cur.close()

    finally:
        try:
            raw.close()
        except Exception:
            pass

    return df_cur, df_hist


# =========================
# Helpers UI / formatos
# =========================

def normalizar_columnas_uuid(df: pd.DataFrame) -> pd.DataFrame:
    uuid_cols = ["id", "agile_purchase_id", "product_id", "site_id", "supplier_id"]
    for col in uuid_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def to_num(series):
    if series is None:
        return pd.Series(dtype="float64")
    return pd.to_numeric(series, errors="coerce").fillna(0)


def build_purchase_label(row: pd.Series) -> str:
    created = ""
    if pd.notna(row.get("created_at")):
        try:
            created = pd.to_datetime(row["created_at"]).strftime("%Y-%m-%d %H:%M")
        except Exception:
            created = str(row["created_at"])

    purchase_code = str(row.get("purchase_code", "") or "")
    status = str(row.get("status", "") or "")
    name = str(row.get("name", "") or "")

    label = f"{purchase_code} | {status}"
    if created:
        label += f" | {created}"
    if name.strip():
        label += f" | {name.strip()}"
    return label


def render_kpis_origen(df_det: pd.DataFrame):
    if df_det.empty:
        st.warning("No hay detalle para la compra seleccionada.")
        return

    total_lineas = len(df_det)
    total_solicitado = to_num(df_det.get("requested_units")).sum()
    total_comprado = to_num(df_det.get("purchased_units")).sum()
    total_confirmado = to_num(df_det.get("quantity_confirmed")).sum()
    total_pendiente = to_num(df_det.get("pending_quantity")).sum()
    total_importe = to_num(df_det.get("total_price")).sum()

    aprobadas = 0
    if "approved" in df_det.columns:
        approved_series = df_det["approved"].astype(str).str.upper().str.strip()
        aprobadas = int(approved_series.isin(["TRUE", "T", "1", "S", "Y"]).sum())

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Líneas", f"{total_lineas:,}")
    c2.metric("Aprobadas", f"{aprobadas:,}")
    c3.metric("Unid. solicitadas", f"{int(total_solicitado):,}")
    c4.metric("Unid. compradas", f"{int(total_comprado):,}")
    c5.metric("Unid. confirmadas", f"{int(total_confirmado):,}")
    c6.metric("Unid. pendientes", f"{int(total_pendiente):,}")
    c7.metric("Importe total", f"{float(total_importe):,.2f}")


def render_estado_global(df_hdr, df_det, df_pre_pg, ssql_ok, df_cur=None, df_hist=None):
    estado = []

    if not df_hdr.empty or not df_det.empty:
        estado.append("🟩 Compra Ágil registrada en Connexa")

    if not df_pre_pg.empty:
        estado.append("🟩 Consolidación precarga (diarco_data)")

    if ssql_ok:
        if df_cur is not None and not df_cur.empty:
            estado.append("🟨 En precarga SGM (T080)")
        if df_hist is not None and not df_hist.empty:
            estado.append("🟩 Transferida / Histórica SGM (T874)")
        if (
            df_cur is not None and df_hist is not None and
            df_cur.empty and df_hist.empty and not df_pre_pg.empty
        ):
            estado.append("🟧 Consolidada pero aún no visible en SGM")

    if estado:
        st.write("\n".join(estado))
    else:
        st.write("Sin señales de estado para mostrar.")


# =========================
# App principal
# =========================

def main():
    st.title("Control Compra Ágil CONNEXA_MS → SGM")
    st.caption("Compra Ágil / Compra Directa → Pre-carga Connexa → SGM")

    # -------------------------
    # Sidebar / conexiones
    # -------------------------
    with st.sidebar:
        st.subheader("Fuentes de datos")
        connexa_ok = diarco_ok = ssql_ok = False

        try:
            eng_connexa = get_connexa_engine()
            with eng_connexa.connect():
                connexa_ok = True
        except Exception as e:
            st.error(f"Connexa (Postgres) sin conexión: {e}")

        try:
            eng_diarco = get_diarco_engine()
            with eng_diarco.connect():
                diarco_ok = True
        except Exception as e:
            st.error(f"Diarco Data (Postgres) sin conexión: {e}")

        try:
            eng_sql = get_sqlserver_engine()
            with eng_sql.connect():
                ssql_ok = True
        except Exception as e:
            st.warning(f"SQL Server (SGM) no disponible: {e}")

        st.markdown("---")
        st.success(f"Connexa: {'OK' if connexa_ok else 'NO'}")
        st.success(f"Diarco Data: {'OK' if diarco_ok else 'NO'}")
        st.info(f"SQL Server SGM (opcional): {'OK' if ssql_ok else 'NO'}")

        if st.button("🔄 Refrescar datos (limpiar caché)"):
            st.cache_data.clear()
            st.success("Caché limpiada. Recalculando…")
            st.rerun()

    if not (connexa_ok and diarco_ok):
        st.stop()

    # -------------------------
    # 1) Selección Proveedor / Compra
    # -------------------------
    st.header("1) Selección de Proveedor y Compra Ágil")

    df_sup = listar_suppliers_agile(eng_connexa)
    if df_sup.empty:
        st.warning("No se encontraron proveedores en Compra Ágil.")
        st.stop()

    filtro_proveedor = st.text_input(
        "Buscar proveedor por código o nombre",
        value="",
        placeholder="Ej.: 925 o nombre del proveedor"
    ).strip().lower()

    df_sup_sel = df_sup.copy()
    if filtro_proveedor:
        mask = (
            df_sup_sel["ext_code_supplier"].astype(str).str.lower().str.contains(filtro_proveedor, na=False) |
            df_sup_sel["supplier_name"].astype(str).str.lower().str.contains(filtro_proveedor, na=False)
        )
        df_sup_sel = df_sup_sel[mask].copy()

    if df_sup_sel.empty:
        st.warning("No hay proveedores que coincidan con el filtro ingresado.")
        st.stop()

    df_sup_sel["supplier_label"] = (
        df_sup_sel["ext_code_supplier"].fillna("").astype(str).str.strip() +
        " - " +
        df_sup_sel["supplier_name"].fillna("").astype(str).str.strip()
    )

    supplier_label = st.selectbox(
        "Proveedor",
        options=df_sup_sel["supplier_label"].tolist(),
        index=0
    )

    ext_code_supplier = str(
        df_sup_sel.loc[
            df_sup_sel["supplier_label"] == supplier_label,
            "ext_code_supplier"
        ].iloc[0]
    ).strip()

    df_pur = listar_purchases(eng_connexa, ext_code_supplier)
    if df_pur.empty:
        st.warning("No se encontraron compras ágiles para el proveedor seleccionado.")
        st.stop()

    df_pur = df_pur.copy()
    df_pur["purchase_option"] = df_pur.apply(build_purchase_label, axis=1)

    purchase_option = st.selectbox(
        "Compra Ágil (purchase_code)",
        options=df_pur["purchase_option"].tolist(),
        index=0
    )

    purchase_code = str(
        df_pur.loc[
            df_pur["purchase_option"] == purchase_option,
            "purchase_code"
        ].iloc[0]
    ).strip()

    if not purchase_code:
        st.info("Seleccione una compra para continuar.")
        st.stop()

    c1, c2 = st.columns(2)
    c1.metric("Proveedor", ext_code_supplier)
    c2.metric("Compra seleccionada", purchase_code)

    # -------------------------
    # 2) Origen Connexa
    # -------------------------
    st.header("2) Origen Compra Ágil (Connexa)")

    df_hdr = cargar_agile_header(eng_connexa, purchase_code)
    df_det = cargar_agile_detail(eng_connexa, purchase_code, ext_code_supplier)

    if df_hdr.empty and df_det.empty:
        st.warning("No se encontraron datos de origen para la compra seleccionada.")
    else:
        if not df_hdr.empty:
            st.subheader("Cabecera")
            df_hdr_show = normalizar_columnas_uuid(df_hdr.copy())
            st.dataframe(df_hdr_show, width="stretch", hide_index=True)

            st.download_button(
                "Descargar CSV - Cabecera Compra Ágil",
                data=df_hdr_show.to_csv(index=False).encode("utf-8"),
                file_name=f"compra_agil_cabecera_{purchase_code}.csv",
                mime="text/csv",
            )

        if not df_det.empty:
            render_kpis_origen(df_det)

            st.subheader("Detalle")
            df_det_show = normalizar_columnas_uuid(df_det.copy())
            st.dataframe(df_det_show, width="stretch")

            st.download_button(
                "Descargar CSV - Detalle Compra Ágil",
                data=df_det_show.to_csv(index=False).encode("utf-8"),
                file_name=f"compra_agil_detalle_{purchase_code}.csv",
                mime="text/csv",
            )

            # Resumen por sitio
            with st.expander("Resumen por Sucursal / Sitio"):
                df_site = (
                    df_det.copy()
                    .assign(
                        requested_units_num=to_num(df_det["requested_units"]),
                        purchased_units_num=to_num(df_det["purchased_units"]),
                        quantity_confirmed_num=to_num(df_det["quantity_confirmed"]),
                        pending_quantity_num=to_num(df_det["pending_quantity"]),
                        total_price_num=to_num(df_det["total_price"]),
                    )
                    .groupby(["site_code", "site_name"], as_index=False)
                    .agg(
                        lineas=("product_code", "count"),
                        requested_units=("requested_units_num", "sum"),
                        purchased_units=("purchased_units_num", "sum"),
                        quantity_confirmed=("quantity_confirmed_num", "sum"),
                        pending_quantity=("pending_quantity_num", "sum"),
                        total_price=("total_price_num", "sum"),
                    )
                    .sort_values(["site_code", "site_name"])
                )

                st.dataframe(df_site, width="stretch")
                st.download_button(
                    "Descargar CSV - Resumen por Sitio",
                    data=df_site.to_csv(index=False).encode("utf-8"),
                    file_name=f"compra_agil_resumen_sitio_{purchase_code}.csv",
                    mime="text/csv",
                    key="dl_resumen_sitio"
                )

    # -------------------------
    # 3) Precarga en diarco_data
    # -------------------------
    st.header("3) Pre-carga consolidada (Postgres diarco_data)")

    df_pre_pg = cargar_precarga_pg(eng_diarco, purchase_code, ext_code_supplier)

    if df_pre_pg.empty:
        st.warning("No hay pre-carga registrada en diarco_data para esta compra/proveedor.")
    else:
        df_pre_pg_show = df_pre_pg.copy()

        df_consol = (
            df_pre_pg_show
            .assign(q_bultos_kilos_diarco_num=to_num(df_pre_pg_show["q_bultos_kilos_diarco"]))
            .groupby(["c_articulo", "c_sucu_empr"], as_index=False)
            .agg(q_bultos_kilos_diarco=("q_bultos_kilos_diarco_num", "sum"))
            .sort_values(["c_articulo", "c_sucu_empr"])
        )

        c1, c2 = st.columns((2, 1))
        with c1:
            st.subheader("Detalle consolidado por Artículo / Sucursal")
            st.dataframe(df_consol, width="stretch")
        with c2:
            st.metric(
                "Total bultos/kilos (precarga)",
                f"{float(df_consol['q_bultos_kilos_diarco'].sum()):,.2f}"
            )

        st.download_button(
            "Descargar CSV - Precarga consolidada",
            data=df_consol.to_csv(index=False).encode("utf-8"),
            file_name=f"precarga_consolidada_{purchase_code}_{ext_code_supplier}.csv",
            mime="text/csv",
        )

        with st.expander("Ver detalle completo de precarga"):
            st.dataframe(df_pre_pg_show, width="stretch")
            st.download_button(
                "Descargar CSV - Precarga completa",
                data=df_pre_pg_show.to_csv(index=False).encode("utf-8"),
                file_name=f"precarga_completa_{purchase_code}_{ext_code_supplier}.csv",
                mime="text/csv",
                key="dl_precarga_completa"
            )

    # -------------------------
    # 4) Estado en SGM
    # -------------------------
    st.header("4) Estado en SGM (SQL Server)")

    df_cur = pd.DataFrame()
    df_hist = pd.DataFrame()

    if not ssql_ok:
        st.info("Conexión a SQL Server no disponible. Esta sección se mostrará cuando el servidor esté accesible.")
    else:
        df_cur, df_hist = cargar_precarga_sqlserver(eng_sql, purchase_code, ext_code_supplier)

        tabs = st.tabs([
            "Pre-carga en curso (T080)",
            "Histórico aprobado (T874)",
            "OC generadas"
        ])

        with tabs[0]:
            if df_cur.empty:
                st.warning("Sin registros en T080 para la compra/proveedor seleccionados.")
            else:
                st.dataframe(df_cur, width="stretch")
                st.download_button(
                    "Descargar CSV - T080",
                    data=df_cur.to_csv(index=False).encode("utf-8"),
                    file_name=f"t080_precarga_{purchase_code}_{ext_code_supplier}.csv",
                    mime="text/csv",
                )

        with tabs[1]:
            if df_hist.empty:
                st.info("Sin registros en histórico para esta compra/proveedor aún.")
            else:
                st.dataframe(df_hist, width="stretch")
                st.download_button(
                    "Descargar CSV - T874",
                    data=df_hist.to_csv(index=False).encode("utf-8"),
                    file_name=f"t874_hist_{purchase_code}_{ext_code_supplier}.csv",
                    mime="text/csv",
                )

        with tabs[2]:
            df_oc = pd.concat([df_cur, df_hist], ignore_index=True)

            if df_oc.empty:
                st.info("Aún no se observan OCs asignadas en SGM.")
            else:
                df_oc = df_oc.copy()
                df_oc["u_prefijo_oc"] = df_oc["u_prefijo_oc"].fillna("").astype(str).str.strip()
                df_oc["u_sufijo_oc"] = df_oc["u_sufijo_oc"].fillna("").astype(str).str.strip()

                df_oc_valid = df_oc[
                    (df_oc["u_prefijo_oc"] != "") &
                    (df_oc["u_sufijo_oc"] != "")
                ].copy()

                if df_oc_valid.empty:
                    st.info("No se encontraron OCs generadas con prefijo/sufijo completos.")
                else:
                    df_oc_valid["oc_generada"] = df_oc_valid["u_prefijo_oc"] + "-" + df_oc_valid["u_sufijo_oc"]

                    df_oc_valid = (
                        df_oc_valid[["c_proveedor", "c_compra_kikker", "oc_generada"]]
                        .drop_duplicates()
                        .sort_values(["c_proveedor", "oc_generada"])
                    )

                    st.dataframe(df_oc_valid, width="stretch")
                    st.download_button(
                        "Descargar CSV - OCs generadas",
                        data=df_oc_valid.to_csv(index=False).encode("utf-8"),
                        file_name=f"ocs_generadas_{purchase_code}_{ext_code_supplier}.csv",
                        mime="text/csv",
                    )

    # -------------------------
    # 5) Estado global
    # -------------------------
    st.header("5) Estado global")
    render_estado_global(df_hdr, df_det, df_pre_pg, ssql_ok, df_cur, df_hist)

    st.markdown("---")
    st.caption("© Zeetrex / Connexa — Auditoría de Compra Ágil")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.exception(e)
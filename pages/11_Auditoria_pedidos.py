# -*- coding: utf-8 -*-
"""
Aplicación Streamlit: Auditoría de Pedidos (Connexa → Pre‑carga Kikker → SGM)

Objetivo
--------
Auditar de punta a punta el flujo de generación de pedidos desde la propuesta (Connexa),
su consolidación en pre‑carga (diarco_data) y su tránsito/confirmación en SGM (SQL Server).

Entradas
--------
1) Postgres (connexa_platform):
   - public.view_spl_supply_purchase_proposal_supplier_site

2) Postgres (diarco_data):
   - public.t080_oc_precarga_kikker

3) SQL Server (SGM):
   - [DIARCOP001].[DiarcoP].[dbo].[T080_OC_PRECARGA_KIKKER]
   - [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]

Parámetros de entorno (preferidos)
----------------------------------
# Connexa (Postgres)
CONNEXA_PG_HOST, CONNEXA_PG_PORT, CONNEXA_PG_DB, CONNEXA_PG_USER, CONNEXA_PG_PASSWORD
# Fallback alternativo (si ya existen en su entorno)
PGP_HOST, PGP_PORT, PGP_DB, PGP_USER, PGP_PASSWORD

# Diarco Data (Postgres)
DIARCO_PG_HOST, DIARCO_PG_PORT, DIARCO_PG_DB, DIARCO_PG_USER, DIARCO_PG_PASSWORD
# Fallback alternativo
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

# SQL Server (SGM)
SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD, SQL_ODBC_DRIVER (opcional; por defecto 'ODBC Driver 18 for SQL Server')

Notas
-----
- Todas las consultas parametrizadas para evitar SQL injection.
- Manejo de errores y señales de estado en la UI.
- Exportación a CSV.

Autor: Equipo Connexa / Zeetrex
"""

import os
import sys
import math
import logging
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Para SQL Server vía pyodbc/ODBC Driver 18
import urllib.parse

# =========================
# Configuración base / logging
# =========================
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("auditoria_pedidos")

st.set_page_config(page_title="Auditoría de Pedidos SGM", layout="wide")

# =========================
# Utilidades de conexión
# =========================

def _build_pg_url(host: str, port: str, db: str, user: str, pwd: str) -> str:
    return f"postgresql+psycopg2://{urllib.parse.quote_plus(user)}:{urllib.parse.quote_plus(pwd)}@{host}:{port}/{db}"

@st.cache_resource(show_spinner=False)
def get_connexa_engine() -> Engine:
    host = os.getenv("CONNEXA_PG_HOST", os.getenv("PGP_HOST"))
    port = os.getenv("CONNEXA_PG_PORT", os.getenv("PGP_PORT", "5432"))
    db   = os.getenv("CONNEXA_PG_DB",   os.getenv("PGP_DB"))
    usr  = os.getenv("CONNEXA_PG_USER", os.getenv("PGP_USER"))
    pwd  = os.getenv("CONNEXA_PG_PASSWORD", os.getenv("PGP_PASSWORD"))
    if not all([host, port, db, usr, pwd]):
        raise RuntimeError("Faltan variables de entorno para Postgres connexa_platform.")
    url = _build_pg_url(host, port, db, usr, pwd)
    return create_engine(url, pool_pre_ping=True)

@st.cache_resource(show_spinner=False)
def get_diarco_engine() -> Engine:
    host = os.getenv("DIARCO_PG_HOST", os.getenv("PG_HOST"))
    port = os.getenv("DIARCO_PG_PORT", os.getenv("PG_PORT", "5432"))
    db   = os.getenv("DIARCO_PG_DB",   os.getenv("PG_DB"))
    usr  = os.getenv("DIARCO_PG_USER", os.getenv("PG_USER"))
    pwd  = os.getenv("DIARCO_PG_PASSWORD", os.getenv("PG_PASSWORD"))
    if not all([host, port, db, usr, pwd]):
        raise RuntimeError("Faltan variables de entorno para Postgres diarco_data.")
    url = _build_pg_url(host, port, db, usr, pwd)
    return create_engine(url, pool_pre_ping=True)

@st.cache_resource(show_spinner=False)
def get_sqlserver_engine() -> Engine:
     # Opcional: habilitar solo si corresponde (Indicadores SGM)
    import urllib.parse
    host = os.getenv("SQL_SERVER")
    port = os.getenv("SQL_PORT", "1433")
    db   = os.getenv("SQL_DATABASE")
    user = os.getenv("SQL_USER")
    pw   = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER","ODBC Driver 18 for SQL Server")

    if not (host and db and user and pw):
        return None

    params = urllib.parse.quote_plus(
        f"DRIVER={driver};SERVER={host},{port};DATABASE={db};UID={user};PWD={pw};Encrypt=yes;TrustServerCertificate=yes;"
    )
    url = f"mssql+pyodbc:///?odbc_connect={params}"

    return create_engine(url, pool_pre_ping=True, fast_executemany=True)

# =========================
# Consultas
# =========================

SQL_VIEW_BASE = """
SELECT id, proposal_id, proposal_number, supplier_id, ext_code_supplier, name_supplier,
       site_id, ext_code_site, type_site, name,
       proposed_quantity, quantity_confirmed, total_amount, total_units, total_box, total_product,
       quantity_stock, occupation_pallet, occupation_pallet_proposed, approved
FROM public.view_spl_supply_purchase_proposal_supplier_site
WHERE 1=1
  {supplier_filter}
  {proposal_filter}
"""

SQL_DISTINCT_SUPPLIERS = """
SELECT DISTINCT ext_code_supplier
FROM public.view_spl_supply_purchase_proposal_supplier_site
ORDER BY 1
"""

SQL_DISTINCT_PROPOSALS = """
SELECT DISTINCT proposal_number
FROM public.view_spl_supply_purchase_proposal_supplier_site
WHERE ext_code_supplier = :supplier
ORDER BY 1 DESC
"""

SQL_PRECarga_PG = """
SELECT c_proveedor, c_articulo, c_sucu_empr, q_bultos_kilos_diarco, f_alta_sist,
       c_usuario_genero_oc, c_terminal_genero_oc, f_genero_oc, c_usuario_bloqueo,
       m_procesado, f_procesado, u_prefijo_oc, u_sufijo_oc, c_compra_kikker,
       c_usuario_modif, c_comprador, m_publicado
FROM public.t080_oc_precarga_kikker
WHERE c_compra_kikker = :proposal_number
"""

SQL_PRECarga_SQL = """
SELECT [C_PROVEEDOR]        AS c_proveedor,
       [C_ARTICULO]         AS c_articulo,
       [C_SUCU_EMPR]        AS c_sucu_empr,
       [Q_BULTOS_KILOS_DIARCO] AS q_bultos_kilos_diarco,
       [F_ALTA_SIST]        AS f_alta_sist,
       [C_USUARIO_GENERO_OC] AS c_usuario_genero_oc,
       [C_TERMINAL_GENERO_OC] AS c_terminal_genero_oc,
       [F_GENERO_OC]        AS f_genero_oc,
       [C_USUARIO_BLOQUEO]  AS c_usuario_bloqueo,
       [M_PROCESADO]        AS m_procesado,
       [F_PROCESADO]        AS f_procesado,
       [U_PREFIJO_OC]       AS u_prefijo_oc,
       [U_SUFIJO_OC]        AS u_sufijo_oc,
       [C_COMPRA_KIKKER]    AS c_compra_kikker,
       [C_USUARIO_MODIF]    AS c_usuario_modif,
       [C_COMPRADOR]        AS c_comprador
FROM [DIARCOP001].[DiarcoP].[dbo].[T080_OC_PRECARGA_KIKKER]
WHERE [C_COMPRA_KIKKER] = ?
"""

SQL_PRECarga_HIST_SQL = """
SELECT [C_PROVEEDOR]        AS c_proveedor,
       [C_ARTICULO]         AS c_articulo,
       [C_SUCU_EMPR]        AS c_sucu_empr,
       [Q_BULTOS_KILOS_DIARCO] AS q_bultos_kilos_diarco,
       [F_ALTA_SIST]        AS f_alta_sist,
       [C_USUARIO_GENERO_OC] AS c_usuario_genero_oc,
       [C_TERMINAL_GENERO_OC] AS c_terminal_genero_oc,
       [F_GENERO_OC]        AS f_genero_oc,
       [C_USUARIO_BLOQUEO]  AS c_usuario_bloqueo,
       [M_PROCESADO]        AS m_procesado,
       [F_PROCESADO]        AS f_procesado,
       [U_PREFIJO_OC]       AS u_prefijo_oc,
       [U_SUFIJO_OC]        AS u_sufijo_oc,
       [C_COMPRA_KIKKER]    AS c_compra_kikker,
       [C_USUARIO_MODIF]    AS c_usuario_modif,
       [C_COMPRADOR]        AS c_comprador
FROM [DIARCOP001].[DiarcoP].[dbo].[T874_OC_PRECARGA_KIKKER_HIST]
WHERE [C_COMPRA_KIKKER] = ?
"""

# =========================
# Capa de datos
# =========================
    
@st.cache_data(show_spinner=False)
def listar_suppliers(_engine: Engine) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(SQL_DISTINCT_SUPPLIERS, conn)
    
@st.cache_data(show_spinner=False)
def listar_proposals(_engine: Engine, supplier: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text(SQL_DISTINCT_PROPOSALS), conn, params={"supplier": supplier})

@st.cache_data(show_spinner=False)
def cargar_resumen_view(_engine: Engine, supplier: Optional[str], proposal: Optional[str]) -> pd.DataFrame:
    supplier_filter = ""
    proposal_filter = ""
    params = {}
    if supplier:
        supplier_filter = "AND ext_code_supplier = :supplier"
        params["supplier"] = supplier
    if proposal:
        proposal_filter = "AND proposal_number = :proposal"
        params["proposal"] = proposal
    query = SQL_VIEW_BASE.format(supplier_filter=supplier_filter, proposal_filter=proposal_filter)
    with _engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


@st.cache_data(show_spinner=False)
def cargar_precarga_pg(_engine: Engine, proposal: str) -> pd.DataFrame:
    with _engine.connect() as conn:
        return pd.read_sql(text(SQL_PRECarga_PG), conn, params={"proposal_number": proposal})


@st.cache_data(show_spinner=False)
def cargar_precarga_sqlserver(_engine: Engine, proposal: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lee T080 (corriente) y T874 (histórico) desde SQL Server.
    Nota: algunos backends de SQLAlchemy no soportan usar `raw_connection()` como context manager.
    Usamos manejo explícito con try/finally para cerrar el recurso.
    """
    df_cur = pd.DataFrame()
    df_hist = pd.DataFrame()

    raw = _engine.raw_connection()  # no usar `with`; cerrar manualmente
    try:
        cur = raw.cursor()
        cur.execute(SQL_PRECarga_SQL, (proposal,))
        cols = [c[0] for c in cur.description]
        df_cur = pd.DataFrame.from_records(cur.fetchall(), columns=cols)
        cur.close()

        cur = raw.cursor()
        cur.execute(SQL_PRECarga_HIST_SQL, (proposal,))
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
# UI Helpers
# =========================

def kpi_triplet(df: pd.DataFrame, label_prefix: str):
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric(f"{label_prefix} • Total líneas", f"{len(df):,}")
    with c2:
        total_units = pd.to_numeric(df.get("total_units"), errors="coerce").fillna(0).sum()
        st.metric(f"{label_prefix} • Unidades", f"{int(total_units):,}")
    with c3:
        total_amount = pd.to_numeric(df.get("total_amount"), errors="coerce").fillna(0).sum()
        st.metric(f"{label_prefix} • Importe", f"{round(float(total_amount),2):,}")

def normalizar_columnas_uuid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte a string las columnas que contienen UUIDs para que sean compatibles con PyArrow/Streamlit.
    """
    # ajustar la lista a los nombres reales
    uuid_cols = ["proposal_id", "proposal_detail_id", "id"]

    for col in uuid_cols:
        if col in df.columns:
            # Si hay UUID() adentro, esto los convierte a str sin problemas
            df[col] = df[col].astype(str)

    return df

# =========================
# App
# =========================

def main():
    st.title("Auditoría integral de Pedidos → SGM")
    st.caption("Connexa → Pre‑carga Kikker → SGM (SQL Server)")

    with st.sidebar:
        st.subheader("Fuentes de datos")
        # Estado conexiones
        connexa_ok = diarco_ok = ssql_ok = False
        try:
            eng_connexa = get_connexa_engine()
            with eng_connexa.connect() as _:
                connexa_ok = True
        except Exception as e:
            st.error(f"Connexa (Postgres) sin conexión: {e}")
        try:
            eng_diarco = get_diarco_engine()
            with eng_diarco.connect() as _:
                diarco_ok = True
        except Exception as e:
            st.error(f"Diarco Data (Postgres) sin conexión: {e}")
        try:
            eng_sql = get_sqlserver_engine()
            with eng_sql.connect() as _:
                ssql_ok = True
        except Exception as e:
            st.warning(f"SQL Server (SGM) no disponible: {e}")

        st.markdown("---")
        st.success(f"Connexa: {'OK' if connexa_ok else 'NO'}")
        st.success(f"Diarco Data: {'OK' if diarco_ok else 'NO'}")
        st.info(f"SQL Server SGM (opcional): {'OK' if ssql_ok else 'NO'}")

    if not (connexa_ok and diarco_ok):
        st.stop()

    # 1) Selección Supplier → Proposal
    st.header("1) Selección de Proveedor y Propuesta")
    df_sup = listar_suppliers(eng_connexa)
    suppliers = df_sup["ext_code_supplier"].tolist()
    supplier = st.selectbox("Proveedor (ext_code_supplier)", options=suppliers, index=0 if suppliers else None)

    proposal = None
    if supplier:
        df_prop = listar_proposals(eng_connexa, supplier)
        props = df_prop["proposal_number"].tolist()
        proposal = st.selectbox("Proposal Number", options=props, index=0 if props else None)

    if not proposal:
        st.info("Seleccione un proveedor y una propuesta para continuar.")
        st.stop()

    # 2) Resumen de la vista base (Connexa)
    st.header("2) Vista base de propuestas (Connexa)")
    df_view = cargar_resumen_view(eng_connexa, supplier, proposal)
    if df_view.empty:
        st.warning("No se encontraron líneas en la vista para la combinación seleccionada.")
    else:
        kpi_triplet(df_view, label_prefix="Propuesta")
        st.dataframe(df_view, width='stretch')
        st.download_button("Descargar CSV - Vista Connexa", data=df_view.to_csv(index=False).encode("utf-8"), file_name=f"connexa_view_{supplier}_{proposal}.csv", mime="text/csv")

    # 3) Precarga en diarco_data (consolidación por ARTÍCULO / SUCURSAL)
    st.header("3) Pre‑carga consolidada (Postgres diarco_data)")
    df_pre_pg = cargar_precarga_pg(eng_diarco, proposal)
    if df_pre_pg.empty:
        st.warning("No hay pre‑carga registrada en diarco_data para esta propuesta.")
    else:
        df_consol = (
            df_pre_pg
            .assign(q_bultos_kilos_diarco=pd.to_numeric(df_pre_pg["q_bultos_kilos_diarco"], errors="coerce").fillna(0))
            .groupby(["c_articulo", "c_sucu_empr"], as_index=False)["q_bultos_kilos_diarco"].sum()
            .sort_values(["c_articulo", "c_sucu_empr"]) 
        )
        c1, c2 = st.columns((2,1))
        with c1:
            st.subheader("Detalle consolidado por Artículo/Sucursal")
            st.dataframe(df_consol, width='stretch')
        with c2:
            st.metric("Total bultos/kilos (precarga)", f"{int(df_consol['q_bultos_kilos_diarco'].sum()):,}")
        st.download_button("Descargar CSV - Precarga consolidada", data=df_consol.to_csv(index=False).encode("utf-8"), file_name=f"precarga_consolidada_{proposal}.csv", mime="text/csv")

    # 4) Estado en SGM (SQL Server): corriente e histórico
    st.header("4) Estado en SGM (SQL Server)")
    if not ssql_ok:
        st.info("Conexión a SQL Server no disponible. Esta sección es opcional y se mostrará cuando el servidor esté accesible.")
    else:
        df_cur, df_hist = cargar_precarga_sqlserver(eng_sql, proposal)
        tabs = st.tabs(["Pre‑carga en curso (T080)", "Histórico aprobado (T874)", "OC generadas"])
        with tabs[0]:
            if df_cur.empty:
                st.warning("Sin registros en T080 para la propuesta (posible ya consolidada/transferida).")
            else:
                st.dataframe(df_cur, width='stretch')
                st.download_button("Descargar CSV - T080", data=df_cur.to_csv(index=False).encode("utf-8"), file_name=f"t080_precarga_{proposal}.csv", mime="text/csv")
        with tabs[1]:
            if df_hist.empty:
                st.info("Sin registros en histórico para esta propuesta aún.")
            else:
                st.dataframe(df_hist, width='stretch')
                st.download_button("Descargar CSV - T874", data=df_hist.to_csv(index=False).encode("utf-8"), file_name=f"t874_hist_{proposal}.csv", mime="text/csv")
        with tabs[2]:
            df_oc = pd.concat([df_cur, df_hist], ignore_index=True)
            if df_oc.empty:
                st.info("Aún no se observan OCs asignadas en SGM.")
            else:
                df_oc = normalizar_columnas_uuid(df_oc)
                
                df_oc["oc_generada"] = df_oc[["u_prefijo_oc", "u_sufijo_oc"]].fillna("").astype(str).agg("-".join, axis=1)
                # Filtrar combinaciones válidas
                df_oc_valid = df_oc[(df_oc["u_prefijo_oc"].notna()) & (df_oc["u_sufijo_oc"].notna()) & (df_oc["u_prefijo_oc"] != "") & (df_oc["u_sufijo_oc"] != "")]
                df_oc_valid = df_oc_valid[["c_proveedor", "c_compra_kikker", "oc_generada"]].drop_duplicates().sort_values(["c_proveedor", "oc_generada"]) 
                st.dataframe(df_oc_valid, width='stretch')
                st.download_button("Descargar CSV - OCs generadas", data=df_oc_valid.to_csv(index=False).encode("utf-8"), file_name=f"ocs_generadas_{proposal}.csv", mime="text/csv")

    # 5) Estado global / semáforos
    st.header("5) Estado global")
    estado = []
    if not df_view.empty:
        estado.append("🟩 Propuesta generada en Connexa")
    if not df_pre_pg.empty:
        estado.append("🟩 Consolidación precarga (diarco_data)")
    if ssql_ok:
        if 'df_cur' in locals() and not df_cur.empty:
            estado.append("🟨 En precarga SGM (T080)")
        if 'df_hist' in locals() and not df_hist.empty:
            estado.append("🟩 Transferida/Histórica SGM (T874)")
        if ('df_cur' in locals() and df_cur.empty) and ('df_hist' in locals() and df_hist.empty) and not df_pre_pg.empty:
            estado.append("🟧 Consolidada pero aún no visible en SGM")
    st.write("\n".join(estado) if estado else "Sin señales de estado para mostrar.")

    st.markdown("---")
    st.caption("© Zeetrex / Connexa — Auditoría de Pedidos")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.exception(e)


# -*- coding: utf-8 -*-
"""
Aplicación interactiva (Streamlit) para ALTA MASIVA de relaciones Comprador–Proveedor
para Connexa.

Características:
- Lista desplegable con compradores activos (public.prc_buyer.active = TRUE).
- Selector de archivo Excel con columnas: ext_code, name.
- Procesamiento con idempotencia: inserta vínculos faltantes y omite los existentes
  (requiere UNIQUE (buyer_id, supplier_id) en public.prc_buyer_supplier).
- Resumen: insertados, ya existentes (omitidos) y proveedores no encontrados.
- Reportes tabulares de los no encontrados y de los que quedaron vinculados.

Ejecución:
    streamlit run app_buyer_supplier_streamlit.py

Dependencias:
    pip install streamlit pandas psycopg2-binary python-dotenv openpyxl

Conexión a Postgres por variables de entorno (.env opcional):
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

Nota: La app intenta crear la extensión pgcrypto y la restricción UNIQUE si no existen.
"""

import os
import io
from typing import List, Tuple, Optional
from contextlib import contextmanager

import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ====================== CONFIG ======================
load_dotenv()

PG_HOST = os.getenv("PGP_HOST", "186.158.182.54")
PG_PORT = int(os.getenv("PGP_PORT", "5432"))
PG_DB = os.getenv("PGP_DB", "connexa_platform")
PG_USER = os.getenv("PGP_USER", "postgres")
PG_PASSWORD = os.getenv("PGP_PASSWORD", "postgres")

# ====================== DB UTILS ====================
@contextmanager
def get_conn():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )
    try:
        yield conn
    finally:
        conn.close()


def init_db_objects():
    """Crea extensión y restricción UNIQUE si no existen."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM   pg_constraint c
                        JOIN   pg_class     t ON c.conrelid = t.oid
                        JOIN   pg_namespace n ON n.oid = t.relnamespace
                        WHERE  n.nspname = 'public'
                        AND    t.relname = 'prc_buyer_supplier'
                        AND    c.conname = 'uq_prc_buyer_supplier'
                    ) THEN
                        ALTER TABLE public.prc_buyer_supplier
                        ADD CONSTRAINT uq_prc_buyer_supplier UNIQUE (buyer_id, supplier_id);
                    END IF;
                END$$;
                """
            )
        conn.commit()


def fetch_active_buyers():
    """Retorna lista de compradores activos [(id, label), ...]."""
    sql = (
        "SELECT id, COALESCE(ext_code,'') AS code, COALESCE(ext_user_login,'') AS login "
        "FROM public.prc_buyer WHERE active = TRUE ORDER BY code, login"
    )
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    buyers = []
    for _id, code, login in rows:
        label = f"{code} — {login}" if login else (code or str(_id))
        buyers.append((_id, label))
    return buyers


def load_excel(file) -> pd.DataFrame:
    """Lee el Excel cargado (file_uploader) y normaliza columnas."""
    df = pd.read_excel(file, dtype={"ext_code": str, "name": str})
    if "ext_code" not in df.columns:
        raise ValueError("La planilla debe contener la columna 'ext_code'.")
    if "name" not in df.columns:
        # Acepta ausencia de name, crea como vacío para informes.
        df["name"] = ""

    df["ext_code"] = df["ext_code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["ext_code"] != ""].copy()
    df = df.drop_duplicates(subset=["ext_code"]).reset_index(drop=True)
    return df


def process_upload(buyer_id: str, df_upload: pd.DataFrame):
    """Inserta vínculos para buyer_id en base a df_upload (ext_code, name).

    Estrategia:
      - Crea TEMP TABLE _tmp_upload(ext_code text, name text)
      - COPY via execute_values
      - INSERT ... SELECT con JOIN a fnd_supplier y ON CONFLICT DO NOTHING RETURNING supplier_id
      - Calcula métricas y devuelve detalle.
    """
    report = {
        "inserted": 0,
        "omitted": 0,
        "missing": 0,
        "missing_list": pd.DataFrame(columns=["ext_code", "name"]),
        "linked_list": pd.DataFrame(columns=["supplier_id", "ext_code", "name"]),
    }

    rows = list(df_upload[["ext_code", "name"]].itertuples(index=False, name=None))

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) Temp table
            cur.execute("CREATE TEMP TABLE _tmp_upload (ext_code text NOT NULL, name text);")

            # 2) Carga a temp
            execute_values(cur, "INSERT INTO _tmp_upload (ext_code, name) VALUES %s", rows)

            # 3) Insert con RETURNING para contar insertados y listar vinculados
            cur.execute(
                """
                WITH inserted AS (
                    INSERT INTO public.prc_buyer_supplier (id, "timestamp", buyer_id, supplier_id)
                    SELECT gen_random_uuid(), NOW(), %s, fs.id
                    FROM _tmp_upload u
                    JOIN public.fnd_supplier fs ON fs.ext_code = u.ext_code
                    ON CONFLICT (buyer_id, supplier_id) DO NOTHING
                    RETURNING supplier_id
                )
                SELECT COUNT(*) FROM inserted;
                """,
                (buyer_id,),
            )
            inserted = cur.fetchone()[0] # type: ignore

            # 4) Total matcheados (existieran o no previamente)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM _tmp_upload u
                JOIN public.fnd_supplier fs ON fs.ext_code = u.ext_code;
                """
            )
            matched = cur.fetchone()[0] # type: ignore

            # 5) Missing: proveedores de la planilla que no existen en catálogo
            cur.execute(
                """
                SELECT u.ext_code, u.name
                FROM _tmp_upload u
                LEFT JOIN public.fnd_supplier fs ON fs.ext_code = u.ext_code
                WHERE fs.id IS NULL
                ORDER BY u.ext_code;
                """
            )
            missing_rows = cur.fetchall()
            missing_df = pd.DataFrame(missing_rows, columns=["ext_code", "name"]) if missing_rows else pd.DataFrame(columns=["ext_code", "name"]) 

            # 6) Listado de vinculados (match con catálogo) para mostrar
            cur.execute(
                """
                SELECT fs.id AS supplier_id, fs.ext_code, COALESCE(u.name, '') AS name
                FROM _tmp_upload u
                JOIN public.fnd_supplier fs ON fs.ext_code = u.ext_code
                ORDER BY fs.ext_code;
                """
            )
            linked_rows = cur.fetchall()
            linked_df = pd.DataFrame(linked_rows, columns=["supplier_id", "ext_code", "name"]) if linked_rows else pd.DataFrame(columns=["supplier_id", "ext_code", "name"]) 

        conn.commit()

    omitted = matched - inserted

    report["inserted"] = inserted
    report["omitted"] = omitted
    report["missing"] = len(missing_df)
    report["missing_list"] = missing_df
    report["linked_list"] = linked_df
    return report


# ====================== UI ==========================
st.set_page_config(page_title="Alta masiva Comprador–Proveedor (Connexa)", layout="centered")
st.title("Alta masiva de relaciones Comprador–Proveedor")
st.caption("Connexa · Carga desde Excel (ext_code, name)")

with st.sidebar:
    st.subheader("Conexión")
    st.text_input("Host", PG_HOST, key="pg_host")
    st.text_input("DB", PG_DB, key="pg_db")
    st.text_input("Usuario", PG_USER, key="pg_user")
    st.text_input("Puerto", str(PG_PORT), key="pg_port")
    st.text_input("Password", PG_PASSWORD, type="password", key="pg_pwd")
    st.caption("*Los cambios en esta barra no reconfiguran la sesión actual. Use variables de entorno.")

# Inicialización de objetos DB (extensión + UNIQUE)
try:
    init_db_objects()
except Exception as e:
    st.warning(f"No fue posible verificar/crear objetos auxiliares: {e}")

# Cargar compradores activos
buyers = []
try:
    buyers = fetch_active_buyers()
except Exception as e:
    st.error(f"Error al obtener compradores activos: {e}")

if not buyers:
    st.stop()

buyer_labels = {bid: label for bid, label in buyers}

buyer_id = st.selectbox(
    "1) Seleccione el comprador activo",
    options=[bid for bid, _ in buyers],
    format_func=lambda x: buyer_labels.get(x, str(x)),
)

uploaded_file = st.file_uploader(
    "2) Seleccione el archivo Excel de proveedores (ext_code, name)",
    type=["xlsx", "xls"],
)

process = st.button("3) Procesar")

if process:
    if not uploaded_file:
        st.warning("Por favor, carguen primero un archivo Excel.")
        st.stop()

    try:
        df = load_excel(uploaded_file)
        st.write(f"Proveedores en planilla (únicos por ext_code): **{len(df)}**")
    except Exception as e:
        st.error(f"Error leyendo Excel: {e}")
        st.stop()

    with st.spinner("Procesando carga..."):
        try:
            report = process_upload(buyer_id, df) # type: ignore
        except Exception as e:
            st.error(f"Fallo durante el procesamiento: {e}")
        else:
            st.success("Proceso finalizado.")
            col1, col2, col3 = st.columns(3)
            col1.metric("Insertados nuevos", report["inserted"])
            col2.metric("Omitidos (ya existían)", report["omitted"])
            col3.metric("No encontrados en catálogo", report["missing"])

            st.markdown("---")
            st.subheader("Detalle de proveedores encontrados (vinculados o ya existentes)")
            st.dataframe(report["linked_list"], width='stretch')

            if report["missing"] > 0:
                st.subheader("No encontrados en catálogo (revisar fnd_supplier.ext_code)")
                st.dataframe(report["missing_list"], width='stretch')

            st.info("La relación existente se omite gracias a la restricción única (buyer_id, supplier_id) y ON CONFLICT DO NOTHING.")

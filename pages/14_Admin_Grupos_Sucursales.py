# 14_Admin_Grupos_Sucursales.py 
# -*- coding: utf-8 -*-
import os
import pandas as pd
import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from modules.db import get_connexa_engine
from modules.ui import render_header

st.set_page_config(
    page_title="Administración de Grupos de Sucursales",
    page_icon="🏬",
    layout="wide",
)
render_header("Administración de Grupos de Sucursales")

TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))


# ============================================================
# Queries
# ============================================================
Q_GROUPS = text("""
SELECT g.id, g.name
FROM supply_planning.spl_group g
ORDER BY g.name;
""")

Q_SITES = text("""
SELECT
    s.id,
    s.code,
    s.name,
    s.address,
    s.type,
    s.company_id
FROM supply_planning.spl_site s
ORDER BY s.code, s.name;
""")

Q_GROUP_SITES = text("""
SELECT
    sg.group_id,
    sg.site_id
FROM supply_planning.spl_site_group sg;
""")

Q_CREATE_GROUP = text("""
INSERT INTO supply_planning.spl_group (name)
VALUES (:name)
RETURNING id;
""")

Q_RENAME_GROUP = text("""
UPDATE supply_planning.spl_group
SET name = :name
WHERE id = :group_id;
""")

Q_DELETE_GROUP_REL = text("""
DELETE FROM supply_planning.spl_site_group
WHERE group_id = :group_id;
""")

Q_DELETE_GROUP = text("""
DELETE FROM supply_planning.spl_group
WHERE id = :group_id;
""")

Q_ADD_SITES = text("""
INSERT INTO supply_planning.spl_site_group (id, site_id, group_id)
VALUES (gen_random_uuid(), :site_id, :group_id)
ON CONFLICT DO NOTHING;
""")

Q_REMOVE_SITES = text("""
DELETE FROM supply_planning.spl_site_group
WHERE group_id = :group_id
  AND site_id = :site_id;
""")

Q_DUP_CHECK = text("""
SELECT LOWER(TRIM(name)) AS name_norm, COUNT(*) AS qty
FROM supply_planning.spl_group
GROUP BY LOWER(TRIM(name))
HAVING COUNT(*) > 1;
""")


# ============================================================
# Data access
# ============================================================
@st.cache_data(ttl=TTL)
def fetch_groups() -> pd.DataFrame:
    eng = get_connexa_engine()
    with eng.connect() as con:
        return pd.read_sql(Q_GROUPS, con)


@st.cache_data(ttl=TTL)
def fetch_sites() -> pd.DataFrame:
    eng = get_connexa_engine()
    with eng.connect() as con:
        df = pd.read_sql(Q_SITES, con)
    if not df.empty:
        df["site_label"] = (
            df["code"].fillna("").astype(str).str.strip()
            + " — "
            + df["name"].fillna("").astype(str).str.strip()
        )
    return df


@st.cache_data(ttl=TTL)
def fetch_group_sites() -> pd.DataFrame:
    eng = get_connexa_engine()
    with eng.connect() as con:
        return pd.read_sql(Q_GROUP_SITES, con)


@st.cache_data(ttl=TTL)
def fetch_duplicate_names() -> pd.DataFrame:
    eng = get_connexa_engine()
    with eng.connect() as con:
        return pd.read_sql(Q_DUP_CHECK, con)


# ============================================================
# Helpers
# ============================================================
def clear_all_cache() -> None:
    fetch_groups.clear()
    fetch_sites.clear()
    fetch_group_sites.clear()
    fetch_duplicate_names.clear()



def normalize_name(name: str) -> str:
    return " ".join((name or "").strip().split())



def get_assigned_unassigned(group_id: str, df_sites: pd.DataFrame, df_rel: pd.DataFrame):
    df_rel_x = df_rel.copy()
    if not df_rel_x.empty:
        df_rel_x["group_id"] = df_rel_x["group_id"].astype(str)
        df_rel_x["site_id"] = df_rel_x["site_id"].astype(str)

    assigned_ids = set(
        df_rel_x.loc[df_rel_x["group_id"] == str(group_id), "site_id"].tolist()
    )

    df_sites_x = df_sites.copy()
    df_sites_x["id"] = df_sites_x["id"].astype(str)

    assigned = df_sites_x[df_sites_x["id"].isin(assigned_ids)].copy()
    unassigned = df_sites_x[~df_sites_x["id"].isin(assigned_ids)].copy()

    assigned = assigned.sort_values(["code", "name"], na_position="last")
    unassigned = unassigned.sort_values(["code", "name"], na_position="last")
    return assigned, unassigned


# ============================================================
# Bootstrap
# ============================================================
try:
    df_groups = fetch_groups()
    df_sites = fetch_sites()
    df_rel = fetch_group_sites()
    df_dup = fetch_duplicate_names()
except Exception as e:
    st.error(f"No fue posible conectar a connexa_platform: {e}")
    st.stop()

if not df_dup.empty:
    st.warning("Se detectaron nombres de grupo duplicados a nivel lógico (ignorando mayúsculas/minúsculas y espacios).")
    st.dataframe(df_dup, width="stretch", hide_index=True)

# ============================================================
# Resumen de grupos
# ============================================================
st.subheader("1. Grupos")

if df_groups.empty:
    st.info("Todavía no existen grupos configurados.")
else:
    df_groups_x = df_groups.copy()
    df_groups_x["id"] = df_groups_x["id"].astype(str)

    if df_rel.empty:
        df_count = pd.DataFrame(columns=["group_id", "cantidad_sucursales"])
    else:
        df_rel_x = df_rel.copy()
        df_rel_x["group_id"] = df_rel_x["group_id"].astype(str)
        df_count = (
            df_rel_x.groupby("group_id", as_index=False)
            .agg(cantidad_sucursales=("site_id", "nunique"))
        )

    df_group_summary = (
        df_groups_x.merge(df_count, left_on="id", right_on="group_id", how="left")
        [["id", "name", "cantidad_sucursales"]]
        .rename(columns={"id": "group_id", "name": "grupo"})
    )
    df_group_summary["cantidad_sucursales"] = df_group_summary["cantidad_sucursales"].fillna(0).astype(int)
    df_group_summary = df_group_summary.sort_values(["grupo"]).reset_index(drop=True)

    st.dataframe(
        df_group_summary[["grupo", "cantidad_sucursales"]],
        width="stretch",
        hide_index=True,
    )

with st.expander("Agregar nuevo grupo"):
    with st.form("form_create_group", clear_on_submit=True):
        new_group_name = st.text_input(
            "Nombre del grupo",
            placeholder="Ej.: AMBA Norte / Formato Mayorista / Tiendas Piloto",
        )
        submitted_create = st.form_submit_button("Crear grupo", type="primary")

        if submitted_create:
            group_name = normalize_name(new_group_name)
            if not group_name:
                st.error("El nombre del grupo no puede estar vacío.")
            else:
                exists = False
                if not df_groups.empty:
                    exists = any(
                        df_groups["name"].fillna("").astype(str).str.strip().str.lower() == group_name.lower()
                    )

                if exists:
                    st.error("Ya existe un grupo con ese nombre.")
                else:
                    try:
                        eng = get_connexa_engine()
                        with eng.begin() as con:
                            con.execute(Q_CREATE_GROUP, {"name": group_name})
                        clear_all_cache()
                        st.success(f"Grupo creado correctamente: {group_name}")
                        st.rerun()
                    except SQLAlchemyError as e:
                        st.error(f"No fue posible crear el grupo: {e}")

st.divider()

# ============================================================
# Administración del grupo seleccionado
# ============================================================
st.subheader("2. Sucursales por grupo")

if df_groups.empty:
    st.info("Primero deben crear al menos un grupo.")
    st.stop()

options = {f"{row['name']}": str(row['id']) for _, row in df_groups.sort_values('name').iterrows()}
selected_label = st.selectbox("Seleccionar grupo", options=list(options.keys()))
selected_group_id = options[selected_label]
selected_group_name = selected_label

assigned, unassigned = get_assigned_unassigned(selected_group_id, df_sites, df_rel)

col_top_1, col_top_2 = st.columns([1.5, 1])
with col_top_1:
    st.metric("Sucursales actualmente en el grupo", int(len(assigned)))
with col_top_2:
    st.metric("Sucursales disponibles para agregar", int(len(unassigned)))

col_left, col_right = st.columns(2)

with col_left:
    st.markdown(f"#### Sucursales del grupo: {selected_group_name}")
    filter_remove = st.text_input(
        "Buscar dentro del grupo",
        key="filter_remove",
        placeholder="Código, nombre o dirección",
    )

    df_remove = assigned.copy()
    if filter_remove:
        f = filter_remove.strip().lower()
        df_remove = df_remove[
            df_remove["site_label"].fillna("").str.lower().str.contains(f)
            | df_remove["address"].fillna("").astype(str).str.lower().str.contains(f)
        ]

    remove_options = df_remove["site_label"].tolist()
    selected_to_remove = st.multiselect(
        "Seleccionar sucursales a quitar",
        options=remove_options,
        key="to_remove",
        help="Las sucursales seleccionadas se desvincularán del grupo.",
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Quitar seleccionadas", use_container_width=True):
            if not selected_to_remove:
                st.warning("Deben seleccionar al menos una sucursal para quitar.")
            else:
                try:
                    df_sel = df_remove[df_remove["site_label"].isin(selected_to_remove)].copy()
                    eng = get_connexa_engine()
                    with eng.begin() as con:
                        for _, row in df_sel.iterrows():
                            con.execute(Q_REMOVE_SITES, {"site_id": str(row["id"]), "group_id": selected_group_id})
                    clear_all_cache()
                    st.success(f"Se quitaron {len(df_sel)} sucursales del grupo.")
                    st.rerun()
                except SQLAlchemyError as e:
                    st.error(f"No fue posible quitar sucursales: {e}")
    with c2:
        if st.button("Quitar todas las filtradas", use_container_width=True):
            if df_remove.empty:
                st.warning("No hay sucursales filtradas para quitar.")
            else:
                try:
                    eng = get_connexa_engine()
                    with eng.begin() as con:
                        for _, row in df_remove.iterrows():
                            con.execute(Q_REMOVE_SITES, {"site_id": str(row["id"]), "group_id": selected_group_id})
                    clear_all_cache()
                    st.success(f"Se quitaron {len(df_remove)} sucursales del grupo.")
                    st.rerun()
                except SQLAlchemyError as e:
                    st.error(f"No fue posible quitar sucursales: {e}")

    st.dataframe(
        df_remove[["code", "name", "address", "type"]],
        width="stretch",
        hide_index=True,
    )

with col_right:
    st.markdown("#### Sucursales disponibles")
    filter_add = st.text_input(
        "Buscar sucursales disponibles",
        key="filter_add",
        placeholder="Código, nombre o dirección",
    )

    df_add = unassigned.copy()
    if filter_add:
        f = filter_add.strip().lower()
        df_add = df_add[
            df_add["site_label"].fillna("").str.lower().str.contains(f)
            | df_add["address"].fillna("").astype(str).str.lower().str.contains(f)
        ]

    add_options = df_add["site_label"].tolist()
    selected_to_add = st.multiselect(
        "Seleccionar sucursales a agregar",
        options=add_options,
        key="to_add",
        help="Las sucursales seleccionadas se asociarán al grupo.",
    )

    c3, c4 = st.columns([1, 1])
    with c3:
        if st.button("Agregar seleccionadas", type="primary", use_container_width=True):
            if not selected_to_add:
                st.warning("Deben seleccionar al menos una sucursal para agregar.")
            else:
                try:
                    df_sel = df_add[df_add["site_label"].isin(selected_to_add)].copy()
                    eng = get_connexa_engine()
                    with eng.begin() as con:
                        for _, row in df_sel.iterrows():
                            con.execute(Q_ADD_SITES, {"site_id": str(row["id"]), "group_id": selected_group_id})
                    clear_all_cache()
                    st.success(f"Se agregaron {len(df_sel)} sucursales al grupo.")
                    st.rerun()
                except SQLAlchemyError as e:
                    st.error(f"No fue posible agregar sucursales: {e}")
    with c4:
        if st.button("Agregar todas las filtradas", use_container_width=True):
            if df_add.empty:
                st.warning("No hay sucursales filtradas para agregar.")
            else:
                try:
                    eng = get_connexa_engine()
                    with eng.begin() as con:
                        for _, row in df_add.iterrows():
                            con.execute(Q_ADD_SITES, {"site_id": str(row["id"]), "group_id": selected_group_id})
                    clear_all_cache()
                    st.success(f"Se agregaron {len(df_add)} sucursales al grupo.")
                    st.rerun()
                except SQLAlchemyError as e:
                    st.error(f"No fue posible agregar sucursales: {e}")

    st.dataframe(
        df_add[["code", "name", "address", "type"]],
        width="stretch",
        hide_index=True,
    )

st.divider()
st.subheader("3. Administración del grupo seleccionado")

col_admin_1, col_admin_2 = st.columns(2)

with col_admin_1:
    with st.form("form_rename_group"):
        rename_value = st.text_input("Renombrar grupo", value=selected_group_name)
        submitted_rename = st.form_submit_button("Guardar nombre")
        if submitted_rename:
            new_name = normalize_name(rename_value)
            if not new_name:
                st.error("El nombre no puede quedar vacío.")
            else:
                duplicated = any(
                    (df_groups["id"].astype(str) != selected_group_id)
                    & (df_groups["name"].fillna("").astype(str).str.strip().str.lower() == new_name.lower())
                )
                if duplicated:
                    st.error("Ya existe otro grupo con ese nombre.")
                else:
                    try:
                        eng = get_connexa_engine()
                        with eng.begin() as con:
                            con.execute(Q_RENAME_GROUP, {"group_id": selected_group_id, "name": new_name})
                        clear_all_cache()
                        st.success("Nombre actualizado correctamente.")
                        st.rerun()
                    except SQLAlchemyError as e:
                        st.error(f"No fue posible renombrar el grupo: {e}")

with col_admin_2:
    with st.expander("Eliminar grupo"):
        st.caption("Esta acción borra primero las relaciones con sucursales y luego elimina el grupo.")
        if st.button("Eliminar grupo seleccionado", type="secondary"):
            try:
                eng = get_connexa_engine()
                with eng.begin() as con:
                    con.execute(Q_DELETE_GROUP_REL, {"group_id": selected_group_id})
                    con.execute(Q_DELETE_GROUP, {"group_id": selected_group_id})
                clear_all_cache()
                st.success("Grupo eliminado correctamente.")
                st.rerun()
            except SQLAlchemyError as e:
                st.error(f"No fue posible eliminar el grupo: {e}")

st.divider()
st.subheader("4. Resumen general")

if df_rel.empty:
    st.info("Aún no existen asociaciones entre grupos y sucursales.")
else:
    df_groups_x = df_groups.copy()
    df_groups_x["id"] = df_groups_x["id"].astype(str)
    df_sites_x = df_sites.copy()
    df_sites_x["id"] = df_sites_x["id"].astype(str)
    df_rel_x = df_rel.copy()
    df_rel_x["group_id"] = df_rel_x["group_id"].astype(str)
    df_rel_x["site_id"] = df_rel_x["site_id"].astype(str)

    resumen = (
        df_rel_x.merge(df_groups_x, left_on="group_id", right_on="id", how="left")
               .merge(df_sites_x[["id", "code", "name", "type"]], left_on="site_id", right_on="id", how="left", suffixes=("_group", "_site"))
               [["name_group", "code", "name_site", "type"]]
               .rename(columns={"name_group": "grupo", "code": "codigo_sucursal", "name_site": "sucursal", "type": "tipo"})
               .sort_values(["grupo", "codigo_sucursal", "sucursal"])
    )

    st.dataframe(resumen, width="stretch", hide_index=True)
    st.download_button(
        "Descargar relaciones CSV",
        data=resumen.to_csv(index=False).encode("utf-8"),
        file_name="grupos_sucursales.csv",
        mime="text/csv",
    )

st.caption("Sugerencia técnica: definir una restricción única sobre (site_id, group_id) y una regla de unicidad lógica para el nombre del grupo.")

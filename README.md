
# CONNEXA Monitor — Tablero de Control (Streamlit)

Aplicación Python (Streamlit) para monitorear el uso de la plataforma **CONNEXA**, con menús y paneles para KPIs.
Incluye el primer indicador (*OC generadas desde CONNEXA*) y stubs para extender al resto.

## Características
- Dashboard dinámico con filtros (rango de fechas, comprador, proveedor).
- KPIs clave:
  1) **Generación de OC desde CONNEXA** (implementado).
  2) **Aprobación de OC en SGM** (stub — requiere SQL Server y tablas SGM).
  3) **Proporción OC CONNEXA vs total SGM** (stub con lógica de mapeo sugerida).
  4) **Proveedores reabastecidos con Comprador Inteligente** (stub).
  5) **Ranking de Compradores que más usan el sistema** (implementado usando fuente CONNEXA).
- Arquitectura modular: `modules/db.py`, `modules/queries.py`, `modules/ui.py`.
- Cache de consultas con TTL para alivianar la BD.
- Timezone: *America/Argentina/Buenos_Aires*.

## Requisitos
- Python 3.10+
- PostgreSQL accesible (credenciales en `.env`).
- (Opcional) SQL Server para métricas SGM (credenciales en `.env`).

## Instalación
```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Editar .env con credenciales reales (PG_* y si corresponde SQLSERVER_*)
```

## Ejecución
```bash
streamlit run app.py
```

## Estructura
```
connexa_monitor/
  app.py
  requirements.txt
  .env.example
  modules/
    db.py
    queries.py
    ui.py
  pages/
    01_Indicador_1_OC_Generadas.py
    02_Indicador_2_OC_Aprobadas_SGM.py
    03_Indicador_3_Proporcion_CI_vs_SGM.py
    04_Indicador_4_Proveedores_CI.py
    05_Indicador_5_Ranking_Compradores.py
```

## Notas de Datos
- Fuente CONNEXA: `public.t080_oc_precarga_kikker`.
- Se sugiere crear `mon` (schema) para vistas auxiliares (ver `queries.py`).
- SGM: definir origen/tablas reales (placeholders en `queries.py`).

## Licencia
Uso interno.

from modules.queries.datos_logisticos import (
    SQL_CATALOGO_FILTROS,
    SQL_CATALOGO_PROVEEDORES,
    construir_consulta_datos_logisticos,
)


def test_catalogos_no_restringen_el_universo_a_una_lista_fija():
    for catalogo in (SQL_CATALOGO_FILTROS, SQL_CATALOGO_PROVEEDORES):
        sql = str(catalogo)
        assert "proveedores_habilitados" not in sql
        assert "p.abastecimiento = 3" not in sql
        assert "p.c_sucu_empr >= 300" not in sql


def test_consulta_sin_proveedores_incluye_todo_el_universo():
    consulta, params = construir_consulta_datos_logisticos((), (), (), None)

    assert "p.c_proveedor_primario IN" not in str(consulta)
    assert params == {}


def test_consulta_aplica_la_seleccion_manual_de_proveedores():
    consulta, params = construir_consulta_datos_logisticos(
        (), (275, 327), (), None
    )

    assert "p.c_proveedor_primario IN" in str(consulta)
    assert params == {"proveedores": (275, 327)}


def test_consulta_incluye_parametros_de_stock_por_clave_logistica():
    consulta, _ = construir_consulta_datos_logisticos((), (), (), None)
    sql = str(consulta)

    assert "LEFT JOIN src.base_stock_sucursal bs" in sql
    assert "bs.codigo_articulo = p.c_articulo" in sql
    assert "bs.codigo_sucursal = p.c_sucu_empr" in sql
    assert "bs.codigo_proveedor = p.c_proveedor_primario" in sql
    for columna in (
        "q_dias_stock",
        "q_dias_sobre_stock",
        "dias_preparacion",
        "importe_minimo",
        "bultos_minimo",
    ):
        assert f"bs.{columna}" in sql


def test_consulta_aplica_abastecimiento_y_sucursales_seleccionados():
    consulta, params = construir_consulta_datos_logisticos(
        (), (), (101, 205), 2
    )
    sql = str(consulta)

    assert "p.abastecimiento = :abastecimiento" in sql
    assert "p.c_sucu_empr IN" in sql
    assert params == {"abastecimiento": 2, "sucursales": (101, 205)}


def test_consulta_todos_no_restringe_abastecimiento_ni_sucursales():
    consulta, params = construir_consulta_datos_logisticos((), (), (), None)
    sql = str(consulta)

    assert "p.abastecimiento = :abastecimiento" not in sql
    assert "p.c_sucu_empr IN" not in sql
    assert "p.abastecimiento = 3" not in sql
    assert "p.c_sucu_empr >= 300" not in sql
    assert params == {}

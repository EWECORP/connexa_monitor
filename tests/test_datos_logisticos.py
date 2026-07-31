from modules.queries.datos_logisticos import (
    SQL_CATALOGO_FILTROS,
    SQL_CATALOGO_PROVEEDORES,
    construir_consulta_datos_logisticos,
)


def test_catalogos_no_restringen_el_universo_a_una_lista_fija():
    assert "proveedores_habilitados" not in str(SQL_CATALOGO_FILTROS)
    assert "proveedores_habilitados" not in str(SQL_CATALOGO_PROVEEDORES)


def test_consulta_sin_proveedores_incluye_todo_el_universo():
    consulta, params = construir_consulta_datos_logisticos((), (), ())

    assert "p.c_proveedor_primario IN" not in str(consulta)
    assert params == {}


def test_consulta_aplica_la_seleccion_manual_de_proveedores():
    consulta, params = construir_consulta_datos_logisticos((), (275, 327), ())

    assert "p.c_proveedor_primario IN" in str(consulta)
    assert params == {"proveedores": (275, 327)}

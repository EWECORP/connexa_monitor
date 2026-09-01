from modules.queries.stock_sucursal import construir_consulta_stock


def test_consulta_stock_incluye_todas_las_columnas_y_sucursal():
    consulta, parametros = construir_consulta_stock(12)
    sql = str(consulta)

    assert "bs.*" in sql
    assert "FROM src.base_stock_sucursal bs" in sql
    assert "bs.codigo_sucursal = :sucursal" in sql
    assert "codigo_proveedor IN" not in sql
    assert parametros == {"sucursal": 12}


def test_consulta_stock_aplica_proveedores_parametrizados():
    consulta, parametros = construir_consulta_stock(7, (101, 202))
    sql = str(consulta)

    assert "bs.codigo_proveedor IN" in sql
    assert parametros == {"sucursal": 7, "proveedores": (101, 202)}

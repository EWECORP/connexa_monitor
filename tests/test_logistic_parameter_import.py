from io import BytesIO
import unittest

import pandas as pd

from modules.logistic_parameter_import import read_and_validate_excel


def workbook(rows: list[dict]) -> BytesIO:
    output = BytesIO()
    pd.DataFrame(rows).to_excel(output, index=False, engine="openpyxl")
    output.seek(0)
    return output


class LogisticParameterValidationTests(unittest.TestCase):
    def test_maps_columns_and_pallet_dimensions(self):
        result = read_and_validate_excel(workbook([{
            "Cod Prov": 1074, "SUC": 1, "ARTICULO": 30387,
            "DIAS STK": 25, "DIAS SSTK": 0, "PISO PALLET": 20,
            "ALTURA PALLET": 4, "DIAS DE PREPARACION": 5,
        }]))

        self.assertTrue(result.errors.empty)
        row = result.valid_rows.iloc[0]
        self.assertEqual(row["number_of_boxes_per_layer"], 20)
        self.assertEqual(row["number_of_layers"], 4)

    def test_rejects_blank_and_negative_values(self):
        result = read_and_validate_excel(workbook([{
            "Cod Prov": 1074, "SUC": 1, "ARTICULO": 30387,
            "DIAS STK": None, "DIAS SSTK": -1, "PISO PALLET": 20,
            "ALTURA PALLET": 4, "DIAS DE PREPARACION": 5,
        }]))

        self.assertTrue(result.valid_rows.empty)
        self.assertIn("DIAS STK", result.errors.iloc[0]["errores"])
        self.assertIn("DIAS SSTK", result.errors.iloc[0]["errores"])

    def test_rejects_conflicting_duplicate_keys(self):
        base = {
            "Cod Prov": 1074, "SUC": 1, "ARTICULO": 30387,
            "DIAS STK": 25, "DIAS SSTK": 0, "PISO PALLET": 20,
            "ALTURA PALLET": 4, "DIAS DE PREPARACION": 5,
        }
        changed = dict(base, **{"DIAS STK": 30})
        result = read_and_validate_excel(workbook([base, changed]))

        self.assertTrue(result.valid_rows.empty)
        self.assertEqual(len(result.errors), 2)

    def test_removes_identical_duplicate_keys(self):
        row = {
            "Cod Prov": 1074, "SUC": 1, "ARTICULO": 30387,
            "DIAS STK": 25, "DIAS SSTK": 0, "PISO PALLET": 20,
            "ALTURA PALLET": 4, "DIAS DE PREPARACION": 5,
        }
        result = read_and_validate_excel(workbook([row, row]))

        self.assertTrue(result.errors.empty)
        self.assertEqual(len(result.valid_rows), 1)
        self.assertEqual(result.duplicate_rows_removed, 1)


if __name__ == "__main__":
    unittest.main()

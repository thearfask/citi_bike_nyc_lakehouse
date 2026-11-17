import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]

class ContractShapeTests(unittest.TestCase):
    def test_four_layer_contracts(self):
        for layer in ("audit", "bronze", "silver", "gold"):
            data = json.loads((ROOT / "config" / f"{layer}_schema.json").read_text())
            self.assertEqual(data["evolution_policy"], "controlled")
            self.assertIn("tables", data)
    def test_bronze_columns_unique(self):
        d = json.loads((ROOT / "config" / "bronze_schema.json").read_text())
        cols = d["tables"]["raw_trips_tbl"]["columns"]
        self.assertEqual(len(cols), len({c["name"] for c in cols}))
        self.assertEqual(len(cols), 13)
    def test_audit_tables_present(self):
        d = json.loads((ROOT / "config" / "audit_schema.json").read_text())
        self.assertEqual(set(d["tables"]), {"audit_events_tbl", "audit_files_tbl"})

if __name__ == "__main__":
    unittest.main()

"""
courtesy of ac1982@no-response.github.com

Exercise top-level imports as both runtime and typed client code.
"""

import influxdb_client
import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class PublicExportsTest(unittest.TestCase):
    def test_public_imports(self):
        root = Path(__file__).resolve().parents[1]
        fixture = root / "tests" / "typing" / "public_exports.py"
        runpy.run_path(str(fixture), run_name="__main__")
        with tempfile.TemporaryDirectory() as cache:
            result = subprocess.run(
                [sys.executable, "-m", "mypy", "--no-implicit-reexport",
                 "--follow-imports=silent", "--ignore-missing-imports",
                 "--cache-dir", cache, str(fixture)],
                cwd=str(root), capture_output=True, text=True,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_all_exported(self):
        all_symbols = [name for name in vars(influxdb_client) if not name.startswith("_") and not name[0].islower()]
        for name in all_symbols:
            assert name in influxdb_client.__all__, \
                f"'{name}' follows public pattern but is not in influxdb_client.__all__"

    def test_all_public_exports_valid(self):
        for name in influxdb_client.__all__:
            assert hasattr(influxdb_client, name), f"'{name}' in __all__ but not defined on influxdb_client"

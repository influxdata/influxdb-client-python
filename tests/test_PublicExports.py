"""Exercise top-level imports as both runtime and typed client code."""

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

"""Fixture orchestration faults do not qualify native database behavior."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


class SurrealFixtureTests(unittest.TestCase):
    def test_cold_reopen_timeout_retains_diagnostics_and_owned_path(self):
        source = Path(__file__).resolve().parents[2] / "tests/surreal_legacy_store/fixture.py"
        spec = importlib.util.spec_from_file_location("surreal_fixture", source)
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        for phase in ("seed", "read"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                owned = Path(directory)
                (owned / "seed.json").write_text("[]")
                timeout = subprocess.TimeoutExpired("native-child", 60, b"partial output\n", b"native diagnostic\n")
                outcomes = [timeout] if phase == "seed" else [subprocess.CompletedProcess([], 0, "seeded", ""), timeout]
                with patch.object(fixture.tempfile, "mkdtemp", return_value=directory), \
                     patch.object(fixture.subprocess, "run", side_effect=outcomes) as run, \
                     patch.dict(os.environ, {"SURREAL_EXPERIMENTAL_UPGRADE": "true"}):
                    with self.assertRaisesRegex(RuntimeError, f"fresh {phase}.*evidence={directory}"):
                        fixture.LegacyStore.fresh("native-child")
                self.assertEqual((owned / (phase + ".log")).read_text(), "partial output\nnative diagnostic\n")
                self.assertEqual(json.loads((owned / (phase + "-timeout.json")).read_text()),
                                 {"phase": phase, "timeout_seconds": 60})
                self.assertEqual(run.call_count, 1 if phase == "seed" else 2)
                for call in run.call_args_list:
                    self.assertEqual(call.kwargs["timeout"], 60)
                    self.assertNotIn("SURREAL_EXPERIMENTAL_UPGRADE", call.kwargs["env"])


if __name__ == "__main__":
    unittest.main()

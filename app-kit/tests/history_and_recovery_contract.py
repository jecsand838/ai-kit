"""Exercise the real history example across independently owned processes.

Usage: python3 app-kit/tests/history_and_recovery_contract.py CARGO_BUILD_JSONL
Build the example with Cargo's --message-format=json before running this file.
The harness never invokes Cargo and never substitutes a storage implementation.
"""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid


class HistoryAndRecoveryContract(unittest.TestCase):
    build_log = None
    executable = None
    source_output = "Source count: 2; copied count: 40; completed fork count: 41"

    @classmethod
    def setUpClass(cls):
        artifacts = []
        finished = []
        for line in cls.build_log.read_text(encoding="utf-8").splitlines():
            # The release-stage runner may print evidence messages alongside
            # Cargo's JSON stream. Only structured Cargo records select a binary.
            if not line.startswith("{"):
                continue
            row = json.loads(line)
            if row.get("reason") == "build-finished":
                finished.append(row.get("success"))
            target = row.get("target", {})
            if (row.get("reason") == "compiler-artifact"
                    and target.get("name") == "history_and_recovery"
                    and target.get("kind") == ["example"]
                    and row.get("executable")):
                artifacts.append(Path(row["executable"]))
        if finished != [True] or len(artifacts) != 1 or not artifacts[0].is_file():
            raise RuntimeError("expected one history_and_recovery executable from one successful Cargo build")
        cls.executable = artifacts[0].resolve()

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="ai-kit history "))
        self.directory = self.root / "history"
        self.child = None
        self.addCleanup(self.cleanup_directory)

    def reap_child(self):
        if self.child is not None:
            if self.child.poll() is None:
                self.child.kill()
            self.child.communicate(timeout=15)
            self.child = None

    def cleanup_directory(self):
        # A failed join leaves the directory in place and reports a cleanup
        # error, rather than deleting files a surviving process could still use.
        self.reap_child()
        shutil.rmtree(self.root)

    def invoke(self, *arguments, succeeds=True):
        process = subprocess.Popen(
            [str(self.executable), *map(str, arguments)],
            cwd=self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.child = process
        try:
            stdout, stderr = process.communicate(timeout=60)
            self.child = None
        except BaseException as primary:
            try:
                self.reap_child()
            except BaseException as cleanup:
                primary.add_note(f"example process cleanup also failed: {cleanup!r}")
                raise primary from cleanup
            raise
        details = f"arguments: {arguments!r}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        if succeeds:
            self.assertEqual(process.returncode, 0, details)
        else:
            self.assertNotEqual(process.returncode, 0, details)
            self.assertTrue(stderr.strip(), "failed invocation must explain its error")
        return stdout

    def create(self):
        output = self.invoke("create", self.directory)
        self.assertIn(self.source_output, output)
        receipt_path = self.directory / "run.json"
        self.assertTrue(receipt_path.is_file(), "create must publish public lookup identities")
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["workflow"], "history-counter")
        self.assertRegex(receipt["workspace"], r"^appws_[0-9a-f-]+$")
        for name in ("source_id", "fork_id"):
            self.assertRegex(receipt[name], r"^run_[0-9a-f-]+$")
        for name in ("source_occurrence", "fork_occurrence"):
            self.assertRegex(receipt[name], r"^[0-9a-f]{32}$")
        self.assertNotEqual(receipt["source_id"], receipt["fork_id"])
        self.assertIn(f"Saved fork {receipt['fork_id']} at count 41", output)
        self.assertTrue((self.directory / "app-state").is_dir())
        return receipt

    def assert_reopened(self, receipt):
        output = self.invoke("reopen", self.directory)
        self.assertIn(
            f"Reopened fork {receipt['fork_id']} at count 41; source lineage remains readable",
            output,
        )
        self.assertNotIn(self.source_output, output, "reopen must not run the source workflow again")
        self.assertEqual(json.loads((self.directory / "run.json").read_text()), receipt)

    def test_memory_mode_keeps_its_documented_result(self):
        output = self.invoke()
        self.assertIn(self.source_output, output)
        self.assertRegex(output, r"Reopened fork run_[0-9a-f-]+ at count 41; source lineage remains readable")
        self.assertEqual(list(self.root.iterdir()), [], "memory mode must not write a persistent demo")

    def test_completed_fork_reopens_in_a_new_process(self):
        receipt = self.create()
        self.assert_reopened(receipt)

    def test_repeated_reopen_preserves_exact_identity_and_result(self):
        receipt = self.create()
        before = (self.directory / "run.json").read_bytes()
        self.assert_reopened(receipt)
        self.assert_reopened(receipt)
        self.assertEqual((self.directory / "run.json").read_bytes(), before)

    def test_missing_exact_run_cleans_up_and_preserves_the_saved_result(self):
        receipt = self.create()
        receipt_path = self.directory / "run.json"
        before = receipt_path.read_bytes()
        absent_id = f"run_{uuid.uuid4()}"
        while absent_id in (receipt["source_id"], receipt["fork_id"]):
            absent_id = f"run_{uuid.uuid4()}"
        absent_occurrence = f"{(int(receipt['fork_occurrence'], 16) + 1) % (1 << 128):032x}"
        for key, value in (("fork_id", absent_id), ("fork_occurrence", absent_occurrence)):
            with self.subTest(selector=key):
                contents = json.dumps(dict(receipt, **{key: value})).encode()
                receipt_path.write_bytes(contents)
                try:
                    # These identities pass format validation. The lookup fails
                    # after App admission and must still join owned work.
                    self.invoke("reopen", self.directory, succeeds=False)
                    self.assertEqual(receipt_path.read_bytes(), contents)
                finally:
                    receipt_path.write_bytes(before)
                self.assert_reopened(receipt)

    def test_duplicate_create_preserves_existing_data(self):
        receipt = self.create()
        before = (self.directory / "run.json").read_bytes()
        sentinel = self.directory / "caller-owned.txt"
        sentinel.write_text("retain caller data", encoding="utf-8")
        self.invoke("create", self.directory, succeeds=False)
        self.assertEqual((self.directory / "run.json").read_bytes(), before)
        self.assertEqual(sentinel.read_text(), "retain caller data")
        self.assert_reopened(receipt)

    def test_missing_receipt_does_not_initialize_storage(self):
        self.invoke("reopen", self.directory, succeeds=False)
        self.assertFalse(self.directory.exists())
        self.directory.mkdir()
        sentinel = self.directory / "caller-owned.txt"
        sentinel.write_text("retain caller data", encoding="utf-8")
        self.invoke("reopen", self.directory, succeeds=False)
        self.assertEqual(list(self.directory.iterdir()), [sentinel])
        self.assertEqual(sentinel.read_text(), "retain caller data")

    def test_malformed_receipts_fail_without_replacing_saved_data(self):
        receipt = self.create()
        receipt_path = self.directory / "run.json"
        before = receipt_path.read_bytes()
        # The oversized case remains valid JSON, so only the receipt read bound
        # should reject it rather than an incidental parse error.
        oversized = before + b" " * (4097 - len(before))
        invalid = [b"not JSON", b"{}", oversized]
        for key, value in (
            ("workspace", "not-a-workspace"),
            ("workflow", "different-workflow"),
            ("source_id", "not-a-run"),
            ("fork_id", "not-a-run"),
            ("source_occurrence", "not-an-occurrence"),
            ("fork_occurrence", "not-an-occurrence"),
        ):
            invalid.append(json.dumps(dict(receipt, **{key: value})).encode())
        try:
            for index, contents in enumerate(invalid):
                with self.subTest(case=index, bytes=len(contents)):
                    receipt_path.write_bytes(contents)
                    self.invoke("reopen", self.directory, succeeds=False)
                    self.assertEqual(receipt_path.read_bytes(), contents)
        finally:
            receipt_path.write_bytes(before)
        self.assert_reopened(receipt)

    def test_missing_store_is_not_silently_recreated(self):
        self.create()
        receipt_path = self.directory / "run.json"
        before = receipt_path.read_bytes()
        shutil.rmtree(self.directory / "app-state")
        self.invoke("reopen", self.directory, succeeds=False)
        self.assertFalse((self.directory / "app-state").exists())
        self.assertEqual(receipt_path.read_bytes(), before)
        (self.directory / "app-state").mkdir()
        self.invoke("reopen", self.directory, succeeds=False)
        self.assertEqual(list((self.directory / "app-state").iterdir()), [])
        self.assertEqual(receipt_path.read_bytes(), before)

    def test_invalid_arguments_fail_without_creating_state(self):
        for arguments in (
            ("create",),
            ("reopen",),
            ("unknown", self.directory),
            ("create", self.directory, "extra"),
            ("reopen", self.directory, "extra"),
            ("create", ""),
        ):
            with self.subTest(arguments=arguments):
                self.invoke(*arguments, succeeds=False)
                self.assertEqual(list(self.root.iterdir()), [])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: history_and_recovery_contract.py CARGO_BUILD_JSONL")
    HistoryAndRecoveryContract.build_log = Path(sys.argv[1])
    unittest.main(argv=[sys.argv[0]], verbosity=2)

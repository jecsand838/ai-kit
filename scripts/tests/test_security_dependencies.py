"""Registry policy and audit faults use small native-shaped scanner reports."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.security_dependencies import REGISTRY, SecurityAudit, SecurityError, SecurityPolicy


class AuditCommands:
    def __init__(self, vulnerabilities=(), unsound=(), warnings=(), mutate=None, fault=None):
        self.calls = []
        self.vulnerabilities = vulnerabilities
        self.unsound = unsound
        self.warnings = warnings
        self.mutate = mutate
        self.fault = fault

    @staticmethod
    def finding(advisory):
        return {"advisory": {"id": advisory, "title": "Fixture advisory"},
                "package": {"name": "fixture", "version": "1.0.0", "source": REGISTRY}}

    def __call__(self, command, **options):
        ignored = [command[index + 1] for index, argument in enumerate(command) if argument == "--ignore"]
        config = Path(options["cwd"]) / ".cargo/audit.toml"
        self.calls.append((command, options["cwd"], config.read_text()))
        if isinstance(self.fault, Exception):
            raise self.fault
        vulnerabilities = [self.finding(item) for item in self.vulnerabilities if item not in ignored]
        unsound = [dict(self.finding(item), kind="unsound") for item in self.unsound if item not in ignored]
        revision = subprocess.run(["git", "-C", command[command.index("--db") + 1], "rev-parse", "HEAD"],
                                  capture_output=True, text=True, check=True).stdout.strip()
        document = {
            "database": {"advisory-count": 10, "last-commit": None if "--no-fetch" in command else revision,
                         "last-updated": None if "--no-fetch" in command else "2026-10-27T11:02:42+01:00"},
            "lockfile": {"dependency-count": 1},
            "settings": {"ignore": ignored, "severity": None, "target_arch": [],
                         "target_os": [], "informational_warnings": ["unmaintained", "unsound", "notice"]},
            "vulnerabilities": {"found": bool(vulnerabilities), "count": len(vulnerabilities), "list": vulnerabilities},
            "warnings": {"unsound": unsound, "unmaintained": [dict(self.finding(item), kind="unmaintained") for item in self.warnings]},
        }
        if callable(self.fault):
            self.fault(document, command)
        options["stdout"].write("invalid JSON" if self.fault == "malformed" else json.dumps(document))
        options["stderr"].write("fixture scanner diagnostics\n")
        if self.mutate:
            self.mutate()
        code = 1 if vulnerabilities or unsound else 0
        if self.fault == "crash":
            code = -9
        if self.fault == "false-pass":
            code = 0
        return subprocess.CompletedProcess(command, code)


class SecurityDependencyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="security-dependencies-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve() / "checkout"
        self.root.mkdir()
        (self.root / "scripts").mkdir()
        self.config = self.root / "scripts/security_exceptions.toml"
        self.config.write_text("advisories = []\n")
        (self.root / "Cargo.toml").write_text('[workspace]\nmembers = ["storage-kit"]\n')
        (self.root / "storage-kit").mkdir()
        (self.root / "storage-kit/Cargo.toml").write_text('[package]\nname = "storage-kit"\nversion = "0.0.1"\n')
        self.lockfile = self.root / "Cargo.lock"
        self.lockfile.write_text('version = 4\n[[package]]\nname = "storage-kit"\nversion = "0.0.1"\n'
                                 '[[package]]\nname = "fixture"\nversion = "1.0.0"\n'
                                 f'source = "{REGISTRY}"\nchecksum = "' + "a" * 64 + '"\n')
        self.policy = SecurityPolicy(self.root)
        self.scanner = self.root.parent / "cargo-audit"
        self.scanner.write_text("scanner fixture\n")
        self.scanner.chmod(0o755)
        self.database = self.root.parent / "advisory-db"
        subprocess.run(["git", "init", "--quiet", str(self.database)], check=True, capture_output=True)
        self.commit_database()

    def commit_database(self):
        subprocess.run(["git", "-C", str(self.database), "-c", "user.name=Audit Fixture",
                        "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=/dev/null",
                        "commit", "--quiet", "--allow-empty", "-m", "fixture database"], check=True, capture_output=True)

    def audit(self, commands=None):
        commands = commands or AuditCommands()
        output = self.root.parent / "evidence"
        audit = SecurityAudit(self.root, self.scanner, self.root.parent / "advisory-db", output, execute=commands)
        with contextlib.redirect_stdout(io.StringIO()):
            code = audit.run()
        return code, json.loads((output / "report.json").read_text()), commands

    def test_registry_sources_and_explicit_empty_policy_pass(self):
        self.policy.verify()
        self.assertEqual(self.policy.inputs(), ("scripts/security_exceptions.toml",))

    def test_only_full_unique_advisory_ids_are_accepted(self):
        for content in ('advisories = ["RUSTSEC-2026"]', 'advisories = ["RUSTSEC-2026-0001", "RUSTSEC-2026-0001"]',
                        'advisories = ["*"]', 'advisories = "RUSTSEC-2026-0001"', 'ignore = []',
                        'advisories = []\nseverity = "critical"'):
            with self.subTest(content=content):
                self.config.write_text(content)
                with self.assertRaises(SecurityError):
                    self.policy.inputs()

    def test_missing_and_symlinked_policy_cannot_authorize_receipts(self):
        self.config.unlink()
        with self.assertRaises(SecurityError):
            self.policy.inputs()
        self.config.symlink_to(self.root / "Cargo.toml")
        with self.assertRaises(SecurityError):
            self.policy.inputs()

    def test_patch_tables_are_rejected_even_without_path_overrides(self):
        manifest = self.root / "Cargo.toml"
        for declaration in ('[patch]\n', '[patch.crates-io]\nfixture = "1.1"\n', '[replace]\n'):
            with self.subTest(declaration=declaration):
                manifest.write_text(declaration)
                with self.assertRaisesRegex(SecurityError, "patch|replace"):
                    self.policy.verify()

    def test_foreign_path_git_and_registry_lock_sources_are_rejected(self):
        original = self.lockfile.read_text()
        for source in ('', 'source = "git+https://example.invalid/crate"\n', 'source = "registry+https://example.invalid/index"\n'):
            with self.subTest(source=source):
                self.lockfile.write_text(original + '\n[[package]]\nname = "foreign"\nversion = "1.0.0"\n' + source)
                with self.assertRaises(SecurityError):
                    self.policy.verify()

    def test_missing_lock_package_inventory_fails(self):
        self.lockfile.write_text("version = 4\n")
        with self.assertRaises(SecurityError):
            self.policy.verify()

    def test_two_scans_preserve_unfiltered_findings_and_qualify_only_exact_ids(self):
        self.config.write_text('advisories = ["RUSTSEC-2026-0001"]\n')
        code, report, commands = self.audit(AuditCommands(vulnerabilities=["RUSTSEC-2026-0001"]))
        self.assertEqual(code, 0)
        self.assertEqual(report["status"], "PASS_WITH_EXCEPTIONS")
        self.assertEqual([row["advisory"]["id"] for row in report["accepted_findings"]], ["RUSTSEC-2026-0001"])
        self.assertEqual(len(commands.calls), 2)
        raw, qualified = [row[0] for row in commands.calls]
        self.assertNotIn("--ignore", raw)
        self.assertNotIn("--no-fetch", raw)
        self.assertIn("--no-fetch", qualified)
        self.assertEqual(qualified[qualified.index("--ignore") + 1], "RUSTSEC-2026-0001")
        for command, cwd, config in commands.calls:
            self.assertFalse(Path(cwd).is_relative_to(self.root))
            self.assertEqual(config, "")
            self.assertEqual(command[command.index("--file") + 1], str(self.lockfile))
            self.assertIn("--json", command)
            self.assertIn("--no-yanked", command)
            self.assertEqual(command[command.index("--deny") + 1], "unsound")
        raw_document = json.loads(Path(report["scans"][0]["json"]).read_text())
        self.assertEqual(raw_document["vulnerabilities"]["count"], 1)

    def test_unused_exceptions_do_not_create_qualified_pass(self):
        self.config.write_text('advisories = ["RUSTSEC-2026-0001"]\n')
        code, report, _ = self.audit()
        self.assertEqual((code, report["status"]), (0, "PASS"))
        self.assertEqual(report["accepted_findings"], [])

    def test_unapproved_vulnerabilities_and_unsound_findings_fail(self):
        self.config.write_text('advisories = ["RUSTSEC-2026-0001"]\n')
        for commands in (AuditCommands(vulnerabilities=["RUSTSEC-2026-0001", "RUSTSEC-2026-0002"]),
                         AuditCommands(unsound=["RUSTSEC-2026-0002"])):
            with self.subTest(commands=commands):
                code, report, _ = self.audit(commands)
                self.assertEqual((code, report["status"]), (1, "FAIL"))

    def test_explicit_unsound_exception_keeps_the_raw_warning_visible(self):
        self.config.write_text('advisories = ["RUSTSEC-2026-0001"]\n')
        code, report, _ = self.audit(AuditCommands(unsound=["RUSTSEC-2026-0001"]))
        self.assertEqual((code, report["status"]), (0, "PASS_WITH_EXCEPTIONS"))
        self.assertEqual(report["accepted_findings"][0]["kind"], "unsound")

    def test_maintenance_warnings_are_visible_but_do_not_fail(self):
        code, report, _ = self.audit(AuditCommands(warnings=["RUSTSEC-2026-0002"]))
        self.assertEqual((code, report["status"]), (0, "PASS"))
        document = json.loads(Path(report["scans"][0]["json"]).read_text())
        self.assertEqual(len(document["warnings"]["unmaintained"]), 1)

    def test_crash_timeout_malformed_report_and_false_success_never_pass(self):
        for fault in ("crash", "malformed", "false-pass", subprocess.TimeoutExpired("scanner", 120), OSError("unavailable")):
            with self.subTest(fault=fault):
                code, report, _ = self.audit(AuditCommands(vulnerabilities=["RUSTSEC-2026-0001"], fault=fault))
                self.assertEqual(code, 1)
                self.assertEqual(report["status"], "INCOMPLETE")

    def test_injected_filters_cannot_hide_raw_findings(self):
        for name, value in (("ignore", ["RUSTSEC-2026-0001"]), ("severity", "critical"), ("target_arch", ["x86_64"]), ("target_os", ["linux"]), ("informational_warnings", None)):
            with self.subTest(name=name):
                def tamper(document, _command):
                    document["settings"][name] = value
                code, report, _ = self.audit(AuditCommands(fault=tamper))
                self.assertEqual((code, report["status"]), (1, "INCOMPLETE"))

    def test_input_changes_during_scans_invalidate_audit(self):
        for path in (self.config, self.lockfile, self.root / "Cargo.toml"):
            with self.subTest(path=path):
                code, report, _ = self.audit(AuditCommands(mutate=lambda: path.write_text(path.read_text() + "\n")))
                self.assertEqual((code, report["status"]), (1, "INCOMPLETE"))
                self.assertIn("changed", report["error"])

    def test_changed_advisory_inventory_invalidates_the_two_scan_result(self):
        def tamper(document, command):
            if "--no-fetch" in command:
                document["database"]["advisory-count"] += 1
        code, report, _ = self.audit(AuditCommands(fault=tamper))
        self.assertEqual((code, report["status"]), (1, "INCOMPLETE"))
        self.assertIn("database changed", report["error"])

    def test_changed_database_commit_invalidates_the_two_scan_result(self):
        def tamper(document, command):
            if "--no-fetch" in command:
                self.commit_database()
        code, report, _ = self.audit(AuditCommands(fault=tamper))
        self.assertEqual((code, report["status"]), (1, "INCOMPLETE"))
        self.assertIn("database changed", report["error"])


if __name__ == "__main__":
    unittest.main()

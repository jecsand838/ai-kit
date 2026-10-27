"""Provenance and advisory-coverage faults use small, authentic-shaped fixtures."""

import contextlib
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest

from scripts.security_dependencies import (
    APPROVED,
    REGISTRY,
    SecurityAudit,
    SecurityError,
    SecurityPatches,
)


class PatchFixture:
    """Small test-local sources, never substitutes for native runtime evidence."""

    def __init__(self, root):
        self.root = Path(root)
        self.manifest = self.root / "third-party/security/manifest.json"
        self.packages = []
        for name, (version, checksum) in APPROVED.items():
            relative = f"third-party/security/{name}-{version}"
            directory = self.root / relative
            directory.mkdir(parents=True)
            (directory / "src").mkdir()
            (directory / "src/lib.rs").write_text("pub type Upstream = usize;\n")
            (directory / "fixture.bin").write_bytes(b"\0upstream-build-input\xff")
            (directory / "LICENSE").write_text("Fixture license text\n")
            manifest = f'[package]\nname = "{name}"\nversion = "{version}"\nlicense = "Apache-2.0"\n'
            (directory / "Cargo.toml").write_text(manifest)
            (directory / "Cargo.toml.orig").write_text(manifest)
            diff = f"third-party/security/patches/{name}-{version}.patch"
            (self.root / diff).parent.mkdir(parents=True, exist_ok=True)
            (self.root / diff).write_text("--- a/Cargo.toml\n+++ b/Cargo.toml\n@@ -1 +1 @@\n-old\n+patched\n")
            self.packages.append({
                "name": name, "version": version, "path": relative,
                "source": REGISTRY,
                "archive_url": f"https://static.crates.io/crates/{name}/{name}-{version}.crate",
                "archive_sha256": checksum,
                "license_files": ["LICENSE"],
                "patch": {"path": diff, "sha256": ""},
                "files": {},
            })
        self.roots = tuple(item["path"] for item in self.packages)
        manifest = self.root / "Cargo.toml"
        contents = manifest.read_text() if manifest.exists() else '[workspace]\nmembers = []\n'
        exclusions = "exclude = " + json.dumps(list(self.roots)) + "\n"
        if "[workspace]" in contents:
            contents = contents.replace("[workspace]\n", "[workspace]\n" + exclusions, 1)
        else:
            contents += "\n[workspace]\n" + exclusions
        contents += "\n[patch.crates-io]\n"
        for item in self.packages:
            key = {"object_store": "object_store_013", "geo": "geo_032"}.get(item["name"], item["name"])
            alias = f'package = "{item["name"]}", ' if key != item["name"] else ""
            contents += f'{key} = {{ {alias}path = "{item["path"]}" }}\n'
        manifest.write_text(contents)
        self.refresh()
        self.write_lock()

    def refresh(self):
        for package in self.packages:
            directory = self.root / package["path"]
            package["files"] = {
                file.relative_to(directory).as_posix(): {
                    "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
                    "executable": bool(file.stat().st_mode & 0o111),
                }
                for file in sorted(directory.rglob("*")) if file.is_file()
            }
            package["patch"]["sha256"] = hashlib.sha256(
                (self.root / package["patch"]["path"]).read_bytes()).hexdigest()
        self.save()

    def save(self):
        self.manifest.write_text(json.dumps({"schema": 1, "packages": self.packages}, indent=2) + "\n")

    def write_lock(self, extra=""):
        lock = "version = 4\n"
        for package in self.packages:
            lock += f'\n[[package]]\nname = "{package["name"]}"\nversion = "{package["version"]}"\n'
        lock += '\n[[package]]\nname = "storage-sdk"\nversion = "0.1.0"\n'
        lock += '\n[[package]]\nname = "fixture-registry"\nversion = "1.0.0"\n'
        lock += f'source = "{REGISTRY}"\nchecksum = "{"a" * 64}"\n'
        (self.root / "Cargo.lock").write_text(lock + extra)


class AuditCommands:
    def __init__(self, normal_code=0, parent_code=0, mutate=None):
        self.calls = []
        self.normal_code = normal_code
        self.parent_code = parent_code
        self.mutate = mutate

    def __call__(self, command, **options):
        lock = Path(command[command.index("--file") + 1])
        document = tomllib.loads(lock.read_text())
        self.calls.append((command, options["cwd"], document))
        parent_scan = lock.name == "patched-parents.lock"
        code = self.parent_code if parent_scan else self.normal_code
        options["stdout"].write("fixture advisory found\n" if code else "fixture audit passed\n")
        if self.mutate is not None:
            self.mutate()
        return subprocess.CompletedProcess(command, code)


class SecurityDependencyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="security-dependencies-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.fixture = PatchFixture(self.root)
        self.patches = SecurityPatches(self.root)

    def test_verified_roots_and_all_input_bytes_are_explicit(self):
        self.assertEqual(self.patches.verify(), self.fixture.roots)
        inputs = self.patches.inputs()
        self.assertIn("third-party/security/manifest.json", inputs)
        for package in self.fixture.packages:
            self.assertIn(package["patch"]["path"], inputs)
            self.assertIn(package["path"] + "/fixture.bin", inputs)
            self.assertIn(package["path"] + "/Cargo.toml.orig", inputs)

    def test_approved_geo_identity_keeps_original_name_and_workspace_alias(self):
        self.assertEqual(APPROVED.get("geo"), (
            "0.32.0", "3f3901269ec6d4f6068d3f09e5f02f995bd076398dcd1dfec407cd230b02d11b"))
        self.assertIn("third-party/security/geo-0.32.0", self.patches.verify())
        workspace = tomllib.loads((self.root / "Cargo.toml").read_text())
        self.assertEqual(workspace["patch"]["crates-io"]["geo_032"], {
            "package": "geo", "path": "third-party/security/geo-0.32.0"})

    def test_approved_surrealdb_expr_identity_is_a_separate_audited_parent(self):
        self.assertEqual(APPROVED.get("surrealdb-expr"), (
            "3.3.0", "2e494e4696ac372ead8c0a8b38f830db056a34c8f4192be4cfadeea3089c8687"))
        parents = self.patches.audit_parents(self.root / "Cargo.lock")
        self.assertTrue(any(package["name"] == "surrealdb-expr" for package in parents))

    def test_source_binary_license_diff_and_executable_tampering_fail(self):
        package = self.fixture.packages[0]
        for suffix in ("src/lib.rs", "fixture.bin", "LICENSE"):
            path = self.root / package["path"] / suffix
            original = path.read_bytes()
            with self.subTest(suffix=suffix):
                path.write_bytes(original + b"changed")
                with self.assertRaises(SecurityError):
                    self.patches.verify()
            path.write_bytes(original)
        path = self.root / package["patch"]["path"]
        original = path.read_bytes()
        path.write_bytes(original + b"changed")
        with self.assertRaises(SecurityError):
            self.patches.verify()
        path.write_bytes(original)
        path = self.root / package["path"] / "src/lib.rs"
        path.chmod(0o755)
        with self.assertRaises(SecurityError):
            self.patches.verify()

    def test_missing_and_extra_files_fail(self):
        directory = self.root / self.fixture.roots[0]
        extra = directory / "unexpected.rs"
        extra.write_text("extra input\n")
        with self.assertRaises(SecurityError):
            self.patches.verify()
        extra.unlink()
        (directory / "fixture.bin").unlink()
        with self.assertRaises(SecurityError):
            self.patches.verify()

    def test_unapproved_inputs_anywhere_in_patch_area_fail(self):
        for relative in ("extra.rs", "extra-package/Cargo.toml"):
            with self.subTest(relative=relative):
                path = self.root / "third-party/security" / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("unapproved patch input\n")
                with self.assertRaises(SecurityError):
                    self.patches.verify()
                path.unlink()

    def test_symlinks_in_files_directories_and_metadata_fail(self):
        directory = self.root / self.fixture.roots[0]
        target = self.root / "outside"
        target.mkdir()
        link = directory / "escape"
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaises(SecurityError):
            self.patches.verify()
        link.unlink()
        source = directory / "src/lib.rs"
        source.unlink()
        source.symlink_to(directory / "LICENSE")
        with self.assertRaises(SecurityError):
            self.patches.verify()
        source.unlink()
        source.write_text("pub type Upstream = usize;\n")
        external = self.root / "external-manifest.json"
        self.fixture.manifest.rename(external)
        self.fixture.manifest.symlink_to(external)
        with self.assertRaisesRegex(SecurityError, "symlink"):
            self.patches.verify()

    def test_identity_upstream_checksum_and_schema_cannot_drift(self):
        for field, value in (("name", "other"), ("version", "99.0.0"),
                             ("source", "registry+https://example.invalid/index"),
                             ("archive_sha256", "0" * 64),
                             ("archive_url", "https://example.invalid/source.crate"),
                             ("path", "third-party/security/arrow-ipc-58.4.1/../escape")):
            with self.subTest(field=field):
                original = self.fixture.packages[0][field]
                self.fixture.packages[0][field] = value
                self.fixture.save()
                with self.assertRaises(SecurityError):
                    self.patches.verify()
                self.fixture.packages[0][field] = original
        self.fixture.save()
        document = json.loads(self.fixture.manifest.read_text())
        document["unexpected"] = True
        self.fixture.manifest.write_text(json.dumps(document))
        with self.assertRaises(SecurityError):
            self.patches.verify()

    def test_duplicate_json_and_package_entries_fail(self):
        original = self.fixture.manifest.read_text()
        self.fixture.manifest.write_text(original.replace('"schema": 1', '"schema": 1, "schema": 1'))
        with self.assertRaises(SecurityError):
            self.patches.verify()
        self.fixture.packages.append(self.fixture.packages[0])
        self.fixture.save()
        with self.assertRaises(SecurityError):
            self.patches.verify()

    def test_package_manifest_identity_is_checked_even_with_updated_inventory(self):
        package = self.fixture.packages[0]
        path = self.root / package["path"] / "Cargo.toml"
        path.write_text('[package]\nname = "masquerade"\nversion = "58.4.1"\n')
        self.fixture.refresh()
        with self.assertRaises(SecurityError):
            self.patches.verify()

    def test_license_and_original_manifest_are_required(self):
        package = self.fixture.packages[0]
        for field, value in (("license_files", []), ("license_files", ["missing"])):
            with self.subTest(value=value):
                original = package[field]
                package[field] = value
                self.fixture.save()
                with self.assertRaises(SecurityError):
                    self.patches.verify()
                package[field] = original
        (self.root / package["path"] / "Cargo.toml.orig").unlink()
        self.fixture.refresh()
        with self.assertRaises(SecurityError):
            self.patches.verify()

    def test_path_traversal_and_malformed_file_metadata_fail(self):
        package = self.fixture.packages[0]
        for name in ("../outside", "/outside", "./fixture.bin", "src//lib.rs", "src\\lib.rs"):
            with self.subTest(name=name):
                package["files"][name] = {"sha256": "a" * 64, "executable": False}
                self.fixture.save()
                with self.assertRaises(SecurityError):
                    self.patches.verify()
                del package["files"][name]
        package["files"]["fixture.bin"]["executable"] = 0
        self.fixture.save()
        with self.assertRaises(SecurityError):
            self.patches.verify()

    def test_root_patch_entries_and_workspace_exclusions_must_match_exactly(self):
        path = self.root / "Cargo.toml"
        original = path.read_text()
        for changed in (
            original + 'other = { path = "third-party/other" }\n',
            original.replace('package = "object_store", ', ''),
            original.replace('path = "third-party/security/arrow-ipc-58.4.1"',
                             'path = "./third-party/security/arrow-ipc-58.4.1"'),
            original.replace('"third-party/security/arrow-ipc-58.4.1", ', '', 1),
            original + '\n[patch.other-registry]\nfoo = { version = "1" }\n',
        ):
            with self.subTest(changed=changed[-80:]):
                path.write_text(changed)
                with self.assertRaisesRegex(SecurityError, "Cargo.toml:"):
                    self.patches.verify()
        path.write_text(original)

    def test_missing_manifest_only_allows_an_unpatched_legacy_workspace(self):
        self.fixture.manifest.unlink()
        with self.assertRaisesRegex(SecurityError, "Cargo.toml:"):
            self.patches.verify()
        (self.root / "Cargo.toml").write_text("[workspace]\nmembers = []\n[patch]\n")
        with self.assertRaisesRegex(SecurityError, "Cargo.toml:"):
            self.patches.verify()
        (self.root / "Cargo.toml").write_text("[workspace]\nmembers = []\n")
        self.assertEqual(self.patches.verify(), ())
        self.assertEqual(self.patches.inputs(), ())
        (self.root / "Cargo.toml").unlink()
        self.assertEqual(self.patches.verify(), ())

    def audit(self, commands):
        scanner = self.root / "scanner"
        scanner.write_text("#!/bin/sh\nexit 0\n")
        scanner.chmod(0o755)
        return SecurityAudit(self.root, scanner, self.root / "database",
                             self.root / "target/security-audit", execute=commands)

    def test_both_audits_keep_original_registry_identities_and_policy(self):
        commands = AuditCommands()
        audit = self.audit(commands)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(audit.run(), 0)
        self.assertEqual(len(commands.calls), 2)
        ordinary, supplemental = commands.calls
        self.assertEqual(ordinary[0][ordinary[0].index("--file") + 1], str(self.root / "Cargo.lock"))
        self.assertNotIn("--no-fetch", ordinary[0])
        self.assertIn("--no-fetch", supplemental[0])
        self.assertEqual(len(supplemental[2]["package"]), len(APPROVED))
        for package in supplemental[2]["package"]:
            version, checksum = APPROVED[package["name"]]
            self.assertEqual(package["version"], version)
            self.assertEqual(package["source"], REGISTRY)
            self.assertEqual(package["checksum"], checksum)
        for command, cwd, _ in commands.calls:
            self.assertNotEqual(Path(cwd), self.root)
            self.assertIn("--no-yanked", command)
            self.assertEqual(command[command.index("--deny") + 1], "unsound")
            self.assertNotIn("--ignore", command)
            self.assertNotIn("--target-os", command)
        report = json.loads((audit.output / "report.json").read_text())
        self.assertEqual(report["status"], "PASS")

    def test_injected_parent_advisory_blocks_when_normal_audit_passes(self):
        commands = AuditCommands(parent_code=1)
        audit = self.audit(commands)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(audit.run(), 1)
        self.assertEqual(len(commands.calls), 2)
        report = json.loads((audit.output / "report.json").read_text())
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual([stage["returncode"] for stage in report["scans"]], [0, 1])
        self.assertIn("advisory found", (audit.output / "patched-parents.log").read_text())

    def test_normal_audit_failure_does_not_skip_parent_audit(self):
        commands = AuditCommands(normal_code=1)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.audit(commands).run(), 1)
        self.assertEqual(len(commands.calls), 2)

    def test_unapproved_path_or_git_dependency_and_missing_parent_fail_before_scans(self):
        for extra in (
            '\n[[package]]\nname = "unknown-local"\nversion = "1.0.0"\n',
            '\n[[package]]\nname = "unknown-git"\nversion = "1.0.0"\nsource = "git+https://example.invalid/repo#123"\n',
        ):
            with self.subTest(extra=extra):
                self.fixture.write_lock(extra)
                commands = AuditCommands()
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(self.audit(commands).run(), 1)
                self.assertEqual(commands.calls, [])
        self.fixture.write_lock()
        lock = self.root / "Cargo.lock"
        lock.write_text(lock.read_text().replace('name = "lance-encoding"\nversion = "13.0.0"', 'name = "storage-sdk"\nversion = "0.1.1"'))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.audit(AuditCommands()).run(), 1)

    def test_audit_capture_process_errors_and_input_changes_cannot_pass(self):
        for failure in (OSError("scanner unavailable"), subprocess.TimeoutExpired(["scanner"], 1)):
            def execute(*_args, **_kwargs):
                raise failure
            audit = self.audit(execute)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(audit.run(), 1)
        commands = AuditCommands(mutate=lambda: (self.root / "Cargo.lock").write_text("version = 4\n"))
        audit = self.audit(commands)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(audit.run(), 1)
        self.assertNotEqual(json.loads((audit.output / "report.json").read_text())["status"], "PASS")

    def test_local_compatibility_requires_complete_nonignored_execution(self):
        from scripts.check_security_compatibility import Compatibility

        names = ["zstd_reads_pre_update_frame_and_round_trips", "include_flate_reads_pre_update_frame_and_round_trips",
                 "arrow_reads_pre_update_compressed_ipc_and_round_trips", "object_store_reads_escaped_s3_xml",
                 "object_store_rejects_malformed_xml_and_unresolved_entities"]
        listing = "\n".join(name + ": test" for name in names)
        self.assertEqual(Compatibility.inventory("compatibility", listing), names)
        for incomplete in ("", "\n".join(listing.splitlines()[1:]), listing + "\n" + names[0] + ": test"):
            with self.subTest(listing=incomplete), self.assertRaises(ValueError):
                Compatibility.inventory("compatibility", incomplete)
        good = "\n".join("test " + name + " ... ok" for name in names)
        good += "\ntest result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"
        Compatibility.execution({"returncode": 0}, good, names)
        for incomplete in ("", good.replace("5 passed", "0 passed"), good.replace("0 ignored", "1 ignored"),
                           good.replace("0 filtered out", "1 filtered out"), good.replace(" ... ok", " ... ignored", 1)):
            with self.subTest(output=incomplete), self.assertRaises(ValueError):
                Compatibility.execution({"returncode": 0}, incomplete, names)


class ParentAdvisoryProof:
    """Opt-in actual-scanner regression; normal unittest discovery never runs it."""

    @staticmethod
    def execute(command, **options):
        # This isolated database intentionally contains only a synthetic local
        # advisory. Production SecurityAudit still fetches the real database.
        if "--no-fetch" not in command:
            command.append("--no-fetch")
        return subprocess.run(command, **options)

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(description=cls.__doc__, allow_abbrev=False)
        parser.add_argument("--parent-advisory-proof", action="store_true", required=True)
        parser.add_argument("--scanner", type=Path, required=True)
        parser.add_argument("--output", type=Path, required=True)
        args = parser.parse_args()
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=False)
        workspace = output / "workspace"
        workspace.mkdir()
        PatchFixture(workspace)
        database = output / "advisory-db"
        advisory = database / "crates/arrow-ipc/RUSTSEC-2026-9999.md"
        advisory.parent.mkdir(parents=True)
        advisory.write_text('''```toml
[advisory]
id = "RUSTSEC-2026-9999"
package = "arrow-ipc"
date = "2026-01-01"
url = "https://example.invalid/security-test"

[versions]
patched = []
```

# Injected patched-parent advisory

This is a local checker regression fixture, not a published advisory.
''')
        for command in (
            ["git", "init", "--quiet"],
            ["git", "add", "crates"],
            ["git", "-c", "user.name=Security regression", "-c", "user.email=security-test@example.invalid",
             "-c", "commit.gpgsign=false", "commit", "--quiet", "-m", "Local advisory regression fixture"],
        ):
            subprocess.run(command, cwd=database, capture_output=True, check=True, timeout=30)
        audit = SecurityAudit(workspace, args.scanner.resolve(), database, output / "audit", execute=cls.execute)
        code = audit.run()
        observed = [stage.get("returncode") for stage in audit.report["scans"]]
        parent_log = (audit.output / "patched-parents.log").read_text()
        passed = code == 1 and observed == [0, 1] and len(audit.report["parents"]) == len(APPROVED) and "RUSTSEC-2026-9999" in parent_log
        proof = {"status": "PASS" if passed else "FAIL", "parent_count": len(APPROVED),
                 "expected_returncodes": [0, 1], "observed_returncodes": observed,
                 "scanner_sha256": SecurityPatches.digest(args.scanner), "offline_synthetic_database": True}
        (output / "proof.json").write_text(json.dumps(proof, indent=2) + "\n")
        print(f"{proof['status']}: actual-scanner injected-parent regression; evidence: {output}")
        return 0 if passed else 1


if __name__ == "__main__":
    if "--parent-advisory-proof" in sys.argv:
        sys.exit(ParentAdvisoryProof.main())
    unittest.main()

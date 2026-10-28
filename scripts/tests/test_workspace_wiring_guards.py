import pathlib
import shutil
import subprocess
import tempfile
import unittest


class WorkspaceWiringGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ai-kit-wiring-guards-")
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        source = pathlib.Path(__file__).resolve().parents[1]
        (self.root / "scripts").mkdir()
        for name in ("check_no_path_wiring.sh", "check_no_type_aliases.sh", "security_dependencies.py"):
            shutil.copyfile(source / name, self.root / "scripts" / name)
        subprocess.run(
            ["git", "init", "--quiet", str(self.root)],
            check=True,
            capture_output=True,
            timeout=10,
        )

    def write(self, path, content):
        destination = self.root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content)

    def guard(self, name, succeeds, offending=None):
        result = subprocess.run(
            ["/bin/bash", str(self.root / "scripts" / name)],
            cwd=self.root,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0 if succeeds else 1, result.stderr)
        if offending is not None:
            self.assertIn(offending, result.stderr)

    def test_root_dependency_patch_is_rejected(self):
        self.write(
            "Cargo.toml",
            '[patch.crates-io]\ngeo = { path = "third-party/geo-0.32.0" }\n',
        )
        self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_existing_workspace_paths_remain_allowed(self):
        self.write(
            "Cargo.toml",
            '[workspace.dependencies]\n'
            'agents-kit = { path = "agents-kit", version = "=0.0.1" }\n'
            'providers-kit = { path = "providers-kit", version = "0.0.1" }\n'
            'storage-kit = { path = "storage-kit", version = "0.0.1", default-features = false }\n'
            'tools-kit = { path = "tools-kit", version = "0.0.1" }\n',
        )
        self.guard("check_no_path_wiring.sh", True)

    def test_former_override_rust_paths_are_rejected(self):
        self.write("third-party/geo-0.32.0/src/lib.rs", '#[path = "upstream.rs"]\nmod upstream;\n')
        self.guard("check_no_path_wiring.sh", False, "third-party/geo-0.32.0/src/lib.rs")

    def test_former_override_item_aliases_are_rejected(self):
        self.write("third-party/geo-0.32.0/src/lib.rs", "pub type Native = usize;\n")
        self.guard("check_no_type_aliases.sh", False, "third-party/geo-0.32.0/src/lib.rs")

    def test_direct_geo_dependency_is_rejected_in_every_other_context(self):
        for header in (
            "", "[dependencies]", "[dev-dependencies]", "[build-dependencies]",
            "[workspace.dependencies]", "[target.'cfg(unix)'.dependencies]",
            "[patch.other-registry]",
        ):
            with self.subTest(header=header):
                self.write("Cargo.toml", header + '\ngeo = { path = "third-party/geo-0.32.0" }\n')
                self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_multiple_path_dependencies_are_each_rejected(self):
        self.write(
            "Cargo.toml",
            '[patch.crates-io]\ngeo = { path = "third-party/geo-0.32.0" }\n'
            '[dependencies]\ngeo = { path = "third-party/geo-0.32.0" }\n',
        )
        self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_security_exceptions_do_not_permit_source_wiring_or_aliases(self):
        self.write("scripts/security_exceptions.toml", 'advisories = ["RUSTSEC-2026-0001"]\n')
        path = "third-party/security/fixture/src/lib.rs"
        self.write(path, '#[path = "native.rs"]\nmod native;\npub type Upstream = usize;\n')
        self.guard("check_no_path_wiring.sh", False, path)
        self.guard("check_no_type_aliases.sh", False, path)

    def test_dependency_path_cannot_be_used_directly_in_root_or_crate(self):
        for path in ("Cargo.toml", "providers-kit/Cargo.toml"):
            with self.subTest(path=path):
                manifest = self.root / path
                original = manifest.read_text() if manifest.exists() else ""
                self.write(path, original + '\n[dependencies]\narrow-ipc = { path = "third-party/security/arrow-ipc" }\n')
                self.guard("check_no_path_wiring.sh", False, path + ":")
                self.write(path, original)

    def test_empty_and_registry_only_patch_tables_are_rejected(self):
        for contents in ("[patch]\n", '[patch.crates-io]\nfixture = "1.1.0"\n', '[replace]\n'):
            with self.subTest(contents=contents):
                self.write("Cargo.toml", contents)
                self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_escaped_path_keys_cannot_bypass_structural_checks(self):
        self.write("Cargo.toml", '[dependencies.example]\n"pa\\u0074h" = "../outside"\n')
        self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_nested_manifest_path_patch_is_rejected(self):
        path = "storage-kit/Cargo.toml"
        self.write(path, '[patch.crates-io]\ngeo = { path = "third-party/geo-0.32.0" }\n')
        self.guard("check_no_path_wiring.sh", False, path + ":")

    def test_other_crate_and_near_miss_geo_paths_are_rejected(self):
        for declaration in (
            'other = { path = "third-party/geo-0.32.0" }',
            'geo = { path = "./third-party/geo-0.32.0" }',
            'geo = { path = "third-party/geo-0.32.1" }',
            'geo = { path = "third-party/geo-0.32.0-other" }',
            'geo = { path = "third-party/geo-0.32.0/../other" }',
            'geo = { path = "third-party/geo-0.32.0", default-features = false }',
            'geo = { path = "third-party/geo-0.32.0" } trailing',
        ):
            with self.subTest(declaration=declaration):
                self.write("Cargo.toml", "[patch.crates-io]\n" + declaration + "\n")
                self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_malformed_patch_headers_do_not_hide_path_keys(self):
        for header in ("[patch.crates-io", "[[patch.crates-io]]", "[patch.crates-io.extra]", "[patch.crates-io] trailing"):
            with self.subTest(header=header):
                self.write("Cargo.toml", header + '\ngeo = { path = "third-party/geo-0.32.0" }\n')
                self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_patch_header_inside_multiline_string_does_not_hide_path_keys(self):
        for quotes in ('"""', "'''"):
            with self.subTest(quotes=quotes):
                self.write(
                    "Cargo.toml",
                    "[workspace.metadata]\nnote = " + quotes + "\n[patch.crates-io]\n"
                    + quotes + '\ngeo = { path = "third-party/geo-0.32.0" }\n',
                )
                self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_project_and_other_dependency_rust_paths_remain_banned(self):
        for path in ("storage-kit/src/lib.rs", "third-party/other/src/lib.rs", "third-party/geo-0.32.0-other/src/lib.rs"):
            with self.subTest(path=path):
                self.write(path, '#[path = "other.rs"]\nmod other;\n')
                self.guard("check_no_path_wiring.sh", False, path)
                (self.root / path).unlink()

    def test_project_and_other_dependency_aliases_remain_banned(self):
        for path in ("storage-kit/src/lib.rs", "third-party/other/src/lib.rs", "third-party/geo-0.32.0-other/src/lib.rs"):
            with self.subTest(path=path):
                self.write(path, "pub(crate) type Unrelated = usize;\n")
                self.guard("check_no_type_aliases.sh", False, path)
                (self.root / path).unlink()

    def test_dependency_spec_aliases_remain_banned(self):
        path = "third-party/geo-0.32.0/specs/contract.md"
        self.write(path, "type Forbidden = usize;\n")
        self.guard("check_no_type_aliases.sh", False, path)

    def test_associated_types_remain_allowed(self):
        self.write(
            "storage-kit/src/lib.rs",
            "trait Backend {\n    type Child;\n}\n"
            "impl Backend for Concrete {\n    type Child = usize;\n}\n",
        )
        self.guard("check_no_type_aliases.sh", True)



if __name__ == "__main__":
    unittest.main()

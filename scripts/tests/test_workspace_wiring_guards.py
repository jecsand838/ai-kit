import pathlib
import shutil
import subprocess
import tempfile
import unittest

from scripts.tests.test_security_dependencies import PatchFixture


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
            'agent-sdk = { path = "agent-sdk", version = "=0.1.0" }\n'
            'provider-sdk = { path = "provider-sdk", version = "0.1.0" }\n'
            'storage-sdk = { path = "storage-sdk", version = "0.1.0", default-features = false }\n'
            'tool-sdk = { path = "tool-sdk", version = "0.1.0" }\n',
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

    def test_verified_upstream_conventions_are_allowed_only_in_approved_roots(self):
        fixture = PatchFixture(self.root)
        path = fixture.roots[0] + "/src/lib.rs"
        self.write(path, '#[path = "native.rs"]\nmod native;\npub type Upstream = usize;\n')
        fixture.refresh()
        self.guard("check_no_path_wiring.sh", True)
        self.guard("check_no_type_aliases.sh", True)
        for path in ("provider-sdk/src/lib.rs", fixture.roots[0] + "-other/src/lib.rs"):
            with self.subTest(path=path):
                self.write(path, '#[path = "native.rs"]\nmod native;\npub type Upstream = usize;\n')
                self.guard("check_no_path_wiring.sh", False, path)
                self.guard("check_no_type_aliases.sh", False, path)
                (self.root / path).unlink()

    def test_upstream_provenance_failure_blocks_both_guards(self):
        fixture = PatchFixture(self.root)
        path = fixture.roots[0] + "/src/lib.rs"
        self.write(path, "pub type Tampered = usize;\n")
        for guard in ("check_no_path_wiring.sh", "check_no_type_aliases.sh"):
            with self.subTest(guard=guard):
                self.guard(guard, False, "inventory")

    def test_approved_patch_path_cannot_be_reused_as_a_direct_dependency(self):
        fixture = PatchFixture(self.root)
        for path in ("Cargo.toml", "provider-sdk/Cargo.toml"):
            with self.subTest(path=path):
                manifest = self.root / path
                original = manifest.read_text() if manifest.exists() else ""
                self.write(path, original + '\n[dependencies]\narrow-ipc = { path = "' + fixture.roots[0] + '" }\n')
                self.guard("check_no_path_wiring.sh", False, path + ":")
                self.write(path, original)

    def test_missing_provenance_blocks_even_empty_patch_tables(self):
        self.write("Cargo.toml", "[patch]\n")
        for guard in ("check_no_path_wiring.sh", "check_no_type_aliases.sh"):
            with self.subTest(guard=guard):
                self.guard(guard, False, "Cargo.toml:")

    def test_escaped_path_keys_cannot_bypass_structural_checks(self):
        self.write("Cargo.toml", '[dependencies.example]\n"pa\\u0074h" = "../outside"\n')
        self.guard("check_no_path_wiring.sh", False, "Cargo.toml:")

    def test_nested_manifest_path_patch_is_rejected(self):
        path = "storage-sdk/Cargo.toml"
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
        for path in ("storage-sdk/src/lib.rs", "third-party/other/src/lib.rs", "third-party/geo-0.32.0-other/src/lib.rs"):
            with self.subTest(path=path):
                self.write(path, '#[path = "other.rs"]\nmod other;\n')
                self.guard("check_no_path_wiring.sh", False, path)
                (self.root / path).unlink()

    def test_project_and_other_dependency_aliases_remain_banned(self):
        for path in ("storage-sdk/src/lib.rs", "third-party/other/src/lib.rs", "third-party/geo-0.32.0-other/src/lib.rs"):
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
            "storage-sdk/src/lib.rs",
            "trait Backend {\n    type Child;\n}\n"
            "impl Backend for Concrete {\n    type Child = usize;\n}\n",
        )
        self.guard("check_no_type_aliases.sh", True)



if __name__ == "__main__":
    unittest.main()

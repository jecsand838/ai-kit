"""Repository documentation inventory and navigation, not prose snapshots."""

import re
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch


class DocumentationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workspace = Path(__file__).resolve().parents[2]
        with (cls.workspace / "docs/documentation-ledger.toml").open("rb") as handle:
            cls.ledger = tomllib.load(handle)

    def canonical_paths(self) -> set[Path]:
        return {
            self.workspace / path
            for paths in self.ledger["canonical_owners"].values()
            for path in paths
        }

    def active_paths(self) -> set[Path]:
        paths = self.canonical_paths()
        for pattern in self.ledger.get("document_sets", {}).get("active", []):
            matches = set(self.workspace.glob(pattern))
            self.assertTrue(matches, f"empty active documentation pattern: {pattern}")
            paths.update(matches)
        return paths

    def test_declared_documents_exist(self) -> None:
        for path in sorted(self.documented_paths()):
            with self.subTest(path=path.relative_to(self.workspace)):
                self.assertTrue(path.is_file(), f"missing document: {path}")

    def documented_paths(self) -> set[Path]:
        # Explicit adoption checks files without activating historical workflows
        # or recursively treating their ordinary links as adopted requirements.
        return self.active_paths() | {
            self.workspace / record["path"]
            for record in self.ledger.get("adopted_requirements", [])
        }

    def test_adopted_requirements_have_explicit_provenance(self) -> None:
        records = self.ledger.get("adopted_requirements", [])
        paths = [record["path"] for record in records]
        required = {
            f"app-kit/plans/2026-09-13/{name}.md"
            for name in ("COMPLETION_PLAN", "ACCEPTANCE_MATRIX", "HISTORY_AND_CONTROLS")
        }
        self.assertEqual(sorted(required - set(paths)), [], "adopted requirements missing from the ledger")
        self.assertEqual(len(paths), len(set(paths)), "duplicate adopted requirement path")
        owners = self.active_paths() | {self.workspace / path for path in paths}
        for record in records:
            with self.subTest(path=record["path"]):
                path = Path(record["path"])
                self.assertFalse(path.is_absolute() or ".." in path.parts)
                self.assertFalse(any(character in str(path) for character in "*?[]"))
                self.assertTrue((self.workspace / path).is_file())
                self.assertTrue(record["scope"].strip(), "adoption scope must be explicit")
                self.assertTrue(record["adopted_by"], "adoption needs an owning reference")
                for owner in record["adopted_by"]:
                    self.assertNotEqual(owner, record["path"], "a document cannot adopt itself")
                    self.assertIn(self.workspace / owner, owners, "adoption owner is not inventoried")
                    self.assertTrue((self.workspace / owner).is_file())

    def missing_current_documents(self) -> list[str]:
        expected = set()
        # Root guides need explicit ownership too. Classifying a dated receipt
        # as historical does not activate or recursively inspect its contents.
        inactive_patterns = [
            pattern
            for category in ("historical", "templates", "excluded")
            for pattern in self.ledger.get("document_sets", {}).get(category, [])
        ]
        expected.update(
            path for path in (self.workspace / "docs").glob("*.md")
            if not any(path.relative_to(self.workspace).match(pattern) for pattern in inactive_patterns)
        )
        for crate in ("agents-kit", "app-kit", "providers-kit", "storage-kit", "tools-kit"):
            root = self.workspace / crate
            expected.update(root / name for name in ("README.md", "ARCHITECTURE.md", "AGENTS.md"))
            expected.update((root / "docs").rglob("*.md"))
            expected.update((root / "specs").rglob("*.md"))
        expected.add(self.workspace / "providers-kit/CONTRIBUTING.md")
        expected.update((self.workspace / ".agents/skills").glob("*/SKILL.md"))
        expected.update((self.workspace / ".agents/skills").glob("*/references/*.md"))
        expected.update((self.workspace / ".agents/skills").glob("*/agents/openai.yaml"))
        expected.update((self.workspace / ".codex/agents").glob("*.toml"))
        expected.add(self.workspace / "agents-kit/.codex/development/PROCESS.md")
        expected.update((self.workspace / "agents-kit/.codex/development/dispatch").glob("*.md"))
        return sorted(str(path.relative_to(self.workspace)) for path in expected - self.active_paths())

    def test_current_guides_specs_and_skills_are_inventoried(self) -> None:
        self.assertEqual(self.missing_current_documents(), [], "active documentation missing from the ledger")

    def test_root_guides_require_ownership_without_activating_history(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            (workspace / "docs").mkdir()
            for name in ("guide.md", "receipt.md"):
                (workspace / "docs" / name).write_text("", encoding="utf-8")
            ledger = {
                "canonical_owners": {"workspace": ["docs/guide.md"]},
                "document_sets": {"historical": ["docs/receipt.md"]},
            }
            with patch.object(self, "workspace", workspace), patch.object(self, "ledger", ledger):
                self.assertNotIn("docs/guide.md", self.missing_current_documents())
                self.assertNotIn("docs/receipt.md", self.missing_current_documents())
                with patch.object(self, "ledger", {**ledger, "canonical_owners": {"workspace": []}}):
                    self.assertIn("docs/guide.md", self.missing_current_documents())
                    self.assertNotIn("docs/receipt.md", self.missing_current_documents())

    def validation_entrypoint_failures(self) -> list[str]:
        failures = []
        validation = self.ledger.get("validation", {})
        for role in (
            "workspace_script", "primary_ci_script", "mock_contracts_script",
            "runtime_qualification_script", "docs_script", "path_guard", "alias_guard",
            "release_qualification_script", "release_evidence_script", "release_import_script",
            "strict_scripts",
        ):
            configured = validation.get(role)
            paths = configured if role == "strict_scripts" else [configured]
            if not isinstance(paths, list) or not paths:
                failures.append(role)
                continue
            for value in paths:
                if not isinstance(value, str) or not value:
                    failures.append(role)
                    continue
                path = Path(value)
                target = self.workspace / path
                if (path.is_absolute() or ".." in path.parts
                        or any(character in value for character in "*?[]")
                        or not target.resolve().is_relative_to(self.workspace.resolve())
                        or not target.is_file()):
                    failures.append(role)
        return failures

    def test_validation_entrypoints_exist(self) -> None:
        self.assertEqual(self.validation_entrypoint_failures(), [], "missing or invalid validation entrypoints")

    def test_validation_entrypoint_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            entrypoint = workspace / "check.sh"
            entrypoint.write_text("", encoding="utf-8")
            validation = {
                role: "check.sh" for role in (
                    "workspace_script", "primary_ci_script", "mock_contracts_script",
                    "runtime_qualification_script", "docs_script", "path_guard", "alias_guard",
                    "release_qualification_script", "release_evidence_script", "release_import_script",
                )
            }
            validation["strict_scripts"] = ["check.sh"]
            with patch.object(self, "workspace", workspace):
                with patch.object(self, "ledger", {"validation": validation}):
                    self.assertEqual(self.validation_entrypoint_failures(), [])
                for role in validation:
                    with self.subTest(missing=role), patch.object(self, "ledger", {
                        "validation": {key: value for key, value in validation.items() if key != role}
                    }):
                        self.assertIn(role, self.validation_entrypoint_failures())
                for value in ("", "missing.sh", "../check.sh", str(entrypoint), "*.sh", [], None):
                    with self.subTest(value=value), patch.object(self, "ledger", {
                        "validation": {**validation, "primary_ci_script": value}
                    }):
                        self.assertIn("primary_ci_script", self.validation_entrypoint_failures())
                for value in ([], "check.sh", ["missing.sh"]):
                    with self.subTest(strict=value), patch.object(self, "ledger", {
                        "validation": {**validation, "strict_scripts": value}
                    }):
                        self.assertIn("strict_scripts", self.validation_entrypoint_failures())

    def test_agent_guidance_cannot_disappear_from_inventory(self) -> None:
        for pattern in (
            ".codex/agents/*.toml",
            "agents-kit/.codex/development/PROCESS.md",
            "agents-kit/.codex/development/dispatch/*.md",
        ):
            with self.subTest(pattern=pattern):
                expected = str(sorted(self.workspace.glob(pattern))[0].relative_to(self.workspace))
                ledger = {**self.ledger, "document_sets": {
                    **self.ledger["document_sets"],
                    "active": [item for item in self.ledger["document_sets"]["active"] if item != pattern],
                }}
                with patch.object(self, "ledger", ledger):
                    self.assertIn(expected, self.missing_current_documents())

    def test_skill_catalog_covers_current_skills(self) -> None:
        catalog = (self.workspace / "SKILLS.md").read_text(encoding="utf-8")
        missing = []
        for path in sorted((self.workspace / ".agents/skills").glob("*/SKILL.md")):
            source = path.read_text(encoding="utf-8")
            name = re.search(r"^name:\s*(\S+)\s*$", source, re.MULTILINE)
            self.assertIsNotNone(name, f"skill name missing: {path}")
            if f"`{name[1]}`" not in catalog:
                missing.append(name[1])
        self.assertEqual(missing, [], "current skills missing from SKILLS.md")

    def agent_skill_failures(self) -> list[str]:
        failures = []
        active = {path.resolve() for path in self.active_paths()}
        for path in sorted((self.workspace / ".codex/agents").glob("*.toml")):
            with path.open("rb") as handle:
                agent = tomllib.load(handle)
            for binding in agent.get("skills", {}).get("config", []):
                if binding.get("enabled") is False:
                    continue
                configured = binding.get("path")
                if binding.get("enabled") is not True or not isinstance(configured, str) or not configured:
                    failures.append(f"{path.relative_to(self.workspace)}: invalid enabled skill binding")
                    continue
                skill = (path.parent / configured / "SKILL.md").resolve()
                if (Path(configured).is_absolute() or not skill.is_relative_to(self.workspace.resolve())
                        or not skill.is_file() or skill not in active):
                    failures.append(f"{path.relative_to(self.workspace)}: missing or uninventoried skill {configured}")
        return failures

    def test_enabled_agent_skills_resolve_to_inventoried_skills(self) -> None:
        self.assertEqual(self.agent_skill_failures(), [], "invalid custom-agent skill references")

    def test_agent_skill_reference_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            agent = workspace / ".codex/agents/reviewer.toml"
            skill = workspace / ".agents/skills/example/SKILL.md"
            agent.parent.mkdir(parents=True)
            skill.parent.mkdir(parents=True)
            skill.write_text("---\nname: example\n---\n", encoding="utf-8")
            ledger = {"canonical_owners": {"agents": [str(agent.relative_to(workspace)), str(skill.relative_to(workspace))]}}
            with patch.object(self, "workspace", workspace), patch.object(self, "ledger", ledger):
                for configured, enabled, valid in (
                    ("../../.agents/skills/example", True, True),
                    ("../../.agents/skills/missing", True, False),
                    (str(skill.parent), True, False),
                    ("../../../foreign", True, False),
                    ("../../.agents/skills/missing", False, True),
                ):
                    with self.subTest(configured=configured, enabled=enabled):
                        agent.write_text(f'[[skills.config]]\npath = "{configured}"\nenabled = {str(enabled).lower()}\n', encoding="utf-8")
                        self.assertEqual(self.agent_skill_failures() == [], valid)
                agent.write_text('[[skills.config]]\npath = "../../.agents/skills/example"\nenabled = true\n', encoding="utf-8")
                with patch.object(self, "ledger", {"canonical_owners": {"agents": [str(agent.relative_to(workspace))]}}):
                    self.assertTrue(self.agent_skill_failures(), "existing but uninventoried skills must fail")

    def local_evidence_roots(self) -> dict[Path, Path]:
        # Availability exceptions never validate or traverse the private receipts.
        roots = {}
        active = self.active_paths()
        workspace = self.workspace.resolve()
        for record in self.ledger.get("local_evidence_links", []):
            self.assertEqual(set(record), {"source", "target_prefix"})
            source, prefix = record["source"], record["target_prefix"]
            self.assertIsInstance(source, str)
            self.assertIsInstance(prefix, str)
            source_path, prefix_path = Path(source), Path(prefix)
            self.assertEqual(source_path.as_posix(), source, "evidence source must be normalized")
            self.assertEqual(prefix_path.as_posix() + "/", prefix, "evidence prefix must be normalized")
            for path in (source_path, prefix_path):
                self.assertFalse(path.is_absolute() or ".." in path.parts)
                self.assertFalse(any(character in str(path) for character in "*?[]"))
            owner = self.workspace / source_path
            self.assertIn(owner, active, "evidence source must be inventoried")
            self.assertTrue(owner.is_file(), "evidence source must exist")
            self.assertTrue(owner.resolve().is_relative_to(workspace), "foreign evidence source")
            self.assertNotIn(owner, roots, "duplicate evidence source")
            self.assertIn("target/**", self.ledger.get("document_sets", {}).get("excluded", []))
            self.assertTrue(prefix_path.is_relative_to(Path("target")), "only private target evidence is eligible")
            destination = (workspace / prefix_path).resolve()
            self.assertTrue(destination.is_relative_to(workspace / "target"), "evidence prefix escapes target")
            roots[owner] = destination
        return roots

    def local_link_failures(self, paths: set[Path]) -> list[str]:
        # Only ordinary inline file links outside code fences are checked.
        # Beyond explicitly adopted files, historical records are not traversed.
        # Anchors, external URLs and template placeholders are not validated.
        failures = []
        evidence_roots = self.local_evidence_roots()
        for path in sorted(paths):
            if path.suffix != ".md":
                continue
            fence = None
            for line in path.read_text(encoding="utf-8").splitlines():
                marker = re.match(r"^\s*(`{3,}|~{3,})", line)
                if marker:
                    if fence is None:
                        fence = marker[1]
                    elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                        fence = None
                    continue
                if fence is not None:
                    continue
                for match in re.finditer(r"\]\((<[^>]+>|[^\s)]+)(?:\s+\"[^\"]*\")?\)", line):
                    target = match[1].strip("<>")
                    if target.startswith("#") or re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target):
                        continue
                    target = target.split("#", 1)[0]
                    destination = path.parent / target
                    if destination.exists():
                        continue
                    prefix = evidence_roots.get(path)
                    if (prefix is not None and not Path(target).is_absolute()
                            and destination.resolve().is_relative_to(prefix)):
                        continue
                    failures.append(f"{path.relative_to(self.workspace)} -> {target}")
        return failures

    def test_documented_markdown_local_file_links_resolve(self) -> None:
        self.assertEqual(self.local_link_failures(self.documented_paths()), [], "broken documentation file links")

    def test_private_evidence_link_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary) / "workspace"
            report = workspace / "storage-kit/docs/feature-split-qualification.md"
            guide = workspace / "guide.md"
            report.parent.mkdir(parents=True)
            guide.write_text("", encoding="utf-8")
            (workspace / "target").mkdir()
            (workspace / "target/present.json").write_text("{}", encoding="utf-8")
            outside = Path(temporary) / "outside"
            outside.mkdir()
            (workspace / "target/escape").symlink_to(outside, target_is_directory=True)
            ledger = {
                "canonical_owners": {"evidence": [str(report.relative_to(workspace)), "guide.md"]},
                "document_sets": {"active": [], "excluded": ["target/**"]},
                "local_evidence_links": [{"source": str(report.relative_to(workspace)), "target_prefix": "target/"}],
            }
            cases = [
                (report, "../../target/absent.json", True),
                (report, "../../target/present.json", True),
                (report, "missing-guide.md", False),
                (guide, "target/absent.json", False),
                (report, "../../target-sibling/absent.json", False),
                (report, "../../target/../absent.json", False),
                (report, "../../../outside/absent.json", False),
                (report, "../../target/escape/absent.json", False),
                (report, str(workspace / "target/absent.json"), False),
            ]
            with patch.object(self, "workspace", workspace), patch.object(self, "ledger", ledger):
                for source, target, allowed in cases:
                    with self.subTest(source=source.name, target=target):
                        source.write_text(f"[evidence]({target})\n", encoding="utf-8")
                        self.assertEqual(self.local_link_failures({source}) == [], allowed)

    def test_invalid_local_evidence_declarations_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary) / "workspace"
            workspace.mkdir()
            report = workspace / "report.md"
            report.write_text("", encoding="utf-8")
            base = {"canonical_owners": {"evidence": ["report.md"]}, "document_sets": {"excluded": ["target/**"]}}
            good = {"source": "report.md", "target_prefix": "target/"}
            records = [
                {**good, "source": "missing.md"},
                {**good, "source": "../report.md"},
                {**good, "source": str(report)},
                {**good, "target_prefix": "docs/"},
                {**good, "target_prefix": "target/../outside/"},
                {**good, "target_prefix": "target-sibling/"},
                {**good, "target_prefix": "target"},
                {**good, "target_prefix": "target/**"},
                {**good, "target_prefix": str(workspace / "target") + "/"},
                {"source": "report.md"},
            ]
            with patch.object(self, "workspace", workspace):
                for record in records:
                    with self.subTest(record=record), patch.object(self, "ledger", {**base, "local_evidence_links": [record]}):
                        with self.assertRaises(AssertionError):
                            self.local_evidence_roots()
                with patch.object(self, "ledger", {**base, "local_evidence_links": [good, good]}):
                    with self.assertRaises(AssertionError):
                        self.local_evidence_roots()
                outside = Path(temporary) / "outside"
                outside.mkdir()
                (workspace / "target").symlink_to(outside, target_is_directory=True)
                with patch.object(self, "ledger", {**base, "local_evidence_links": [good]}):
                    with self.assertRaises(AssertionError):
                        self.local_evidence_roots()

"""Documentation inventory, navigation, and runnable toolchain contracts."""

import fnmatch
import re
import shlex
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote, urlsplit


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

    def instruction_paths(self) -> set[Path]:
        inactive = [
            pattern
            for category in ("historical", "templates", "excluded")
            for pattern in self.ledger.get("document_sets", {}).get(category, [])
        ]
        return {
            path for path in self.active_paths()
            if path.suffix == ".md" and not any(
                fnmatch.fnmatchcase(str(path.relative_to(self.workspace)), pattern)
                for pattern in inactive
            )
        }

    def shell_commands(self, source: str) -> list[list[str]]:
        # Inspect simple shell commands, not prose, quoted prompts, or shell
        # expansion. Retain continued lines and quoted multiline arguments.
        commands = []
        fence = None
        shell = False
        pending = ""
        here_documents = []
        for line in source.splitlines():
            marker = re.match(r"^\s*(`{3,}|~{3,})(.*)$", line)
            if marker:
                if fence is None:
                    fence = marker[1]
                    shell = marker[2].strip() in {"bash", "sh", "shell", "zsh"}
                elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence) and not marker[2].strip():
                    fence = None
                    shell = False
                    pending = ""
                    here_documents = []
                continue
            if not shell:
                continue
            if here_documents:
                delimiter, strip_tabs = here_documents[0]
                if (line.lstrip("\t") if strip_tabs else line) == delimiter:
                    here_documents.pop(0)
                continue
            pending += line + "\n"
            if line.rstrip().endswith("\\"):
                continue
            try:
                tokens = shlex.split(self.shell_continuations(pending), comments=True, posix=True)
            except ValueError:
                continue
            commands.append(tokens)
            # Here-document bodies are input data even when they contain text
            # that resembles a command. Preserve quotes while locating actual
            # redirection tokens so a quoted '<<PROMPT' argument is not one.
            raw = []
            if "<<" in pending:
                lexer = shlex.shlex(pending, posix=False, punctuation_chars="<")
                lexer.whitespace_split = True
                try:
                    raw = list(lexer)
                except ValueError:
                    # Unsupported redirection syntax stays opaque through the
                    # fence; this check never evaluates arbitrary shell input.
                    here_documents = [(None, False)]
            for index, token in enumerate(raw[:-1]):
                if token != "<<":
                    continue
                delimiter = raw[index + 1]
                strip_tabs = delimiter.startswith("-")
                if strip_tabs:
                    delimiter = delimiter[1:]
                if len(delimiter) >= 2 and delimiter[0] in "\"'" and delimiter[-1] == delimiter[0]:
                    delimiter = delimiter[1:-1]
                here_documents.append((delimiter, strip_tabs))
            pending = ""
        return commands

    def shell_continuations(self, source: str) -> str:
        # shlex retains escaped newlines; shells remove them outside single
        # quotes. Preserve literal quoted data and escaped quote characters.
        folded = []
        quote = None
        index = 0
        while index < len(source):
            character = source[index]
            if character == "#" and quote is None:
                end = source.find("\n", index)
                end = len(source) if end < 0 else end + 1
                folded.append(source[index:end])
                index = end
                continue
            if character == "\\" and quote != "'" and index + 1 < len(source):
                following = source[index + 1]
                if following != "\n":
                    folded.extend((character, following))
                index += 2
                continue
            if character in "\"'":
                if quote is None:
                    quote = character
                elif quote == character:
                    quote = None
            folded.append(character)
            index += 1
        return "".join(folded)

    def shell_toolchain_failures(self) -> list[str]:
        with (self.workspace / "rust-toolchain.toml").open("rb") as handle:
            pinned = tomllib.load(handle)["toolchain"]["channel"]
        failures = []
        for path in sorted(self.instruction_paths()):
            for command in self.shell_commands(path.read_text(encoding="utf-8")):
                tokens = list(command)
                if tokens and tokens[0] in {"env", "export"}:
                    tokens.pop(0)
                selections = []
                while tokens and re.match(r"^[A-Za-z_][A-Za-z_0-9]*=", tokens[0]):
                    name, value = tokens.pop(0).split("=", 1)
                    if name == "RUSTUP_TOOLCHAIN":
                        selections.append(value)
                if len(tokens) >= 2 and tokens[0] == "cargo" and tokens[1].startswith("+"):
                    selections.append(tokens[1][1:])
                for selected in selections:
                    if not any(marker in selected for marker in ("$", "`")) and selected != pinned:
                        failures.append(f"{path.relative_to(self.workspace)}: toolchain {selected!r} != {pinned!r}")
        return failures

    def test_active_shell_commands_use_pinned_toolchain(self) -> None:
        self.assertEqual(self.shell_toolchain_failures(), [], "stale explicit toolchain in active shell instructions")

    def test_shell_toolchain_guard_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            (workspace / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.99.0"\n', encoding="utf-8")
            guide = workspace / "guide.md"
            inactive = ["history/receipt.md", "templates/example.md", "tmp/nested/old.md"]
            for name in inactive:
                path = workspace / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("```bash\ncargo +1.94.1 test\n```\n", encoding="utf-8")
            ledger = {
                "canonical_owners": {"workspace": ["guide.md", *inactive]},
                "document_sets": {"historical": ["history/**"], "templates": ["templates/**"], "excluded": ["tmp/**"]},
            }
            cases = [
                ("```bash\ncargo +1.99.0 test\n```", True),
                ("```sh\ncargo +1.94.1 test\n```", False),
                ("```shell\nexport RUSTUP_TOOLCHAIN='1.99.0'\ncargo test\n```", True),
                ("```zsh\nenv RUSTUP_TOOLCHAIN=1.94.1 cargo test\n```", False),
                ('```bash\nRUSTUP_TOOLCHAIN="1.94.1" cargo test\n```', False),
                ("```bash\ncargo test --locked\n```", True),
                ('```bash\nexport RUSTUP_TOOLCHAIN="$selected"\ncargo +"${selected}" test\n```', True),
                ("```bash\ncargo \\\n  +1.94.1 test\n```", False),
                ("```bash\n# cargo +1.94.1 test\ncargo +1.99.0 test # current\ncargo +1.94.1 test\n```", False),
                ("Prose: `cargo +1.94.1 test`.\n```rust\ncargo +1.94.1 test\n```", True),
                ("```bash\ncodex exec $'Illustrative prompt:\ncargo +1.94.1 test'\n```", True),
                ("```bash\nprintf '%s\\n' 'RUSTUP_TOOLCHAIN=1.94.1'\n```", True),
                ("```bash\ncodex exec <<'PROMPT'\ncargo +1.94.1 test\nPROMPT\n```", True),
                ("```bash\ncat <<'DATA'\nRUSTUP_TOOLCHAIN=1.94.1\nDATA\ncargo +1.94.1 test\n```", False),
                ('```bash\ncodex exec <<-"PROMPT"\n\tcargo +1.94.1 test\n\tPROMPT\ncargo test\n```', True),
                ("```bash\nprintf '%s' '<<PROMPT'\ncargo +1.94.1 test\n```", False),
                ('```bash\ncargo +"1.\\\n99.0" test\n```', True),
                ("```bash\nRUSTUP_TOOLCHAIN='1.\\\n99.0' cargo test\n```", False),
                ("```bash\n# Don't interpret this comment as a quote.\ncargo \\\n  +1.94.1 test\n```", False),
                ("~~~sh\ncargo +1.94.1 test\n~~~", False),
            ]
            with patch.object(self, "workspace", workspace), patch.object(self, "ledger", ledger):
                self.assertEqual(self.instruction_paths(), {guide})
                for source, valid in cases:
                    with self.subTest(source=source):
                        guide.write_text(source + "\n", encoding="utf-8")
                        self.assertEqual(self.shell_toolchain_failures() == [], valid)

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

    def markdown_lines(self, path: Path):
        fence = None
        for line in path.read_text(encoding="utf-8").splitlines():
            marker = re.match(r"^\s*(`{3,}|~{3,})", line)
            if marker:
                if fence is None:
                    fence = marker[1]
                elif marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                    fence = None
            elif fence is None:
                yield line

    def markdown_links(self, path: Path):
        for line in self.markdown_lines(path):
            for match in re.finditer(r"\]\((<[^>]+>|[^\s)]+)(?:\s+\"[^\"]*\")?\)", line):
                yield match[1].strip("<>")

    def local_link_failures(self, paths: set[Path]) -> list[str]:
        # Only ordinary inline file links outside code fences are checked.
        # Beyond explicitly adopted files, historical records are not traversed.
        # Anchors, external URLs and template placeholders are not validated.
        failures = []
        evidence_roots = self.local_evidence_roots()
        for path in sorted(paths):
            if path.suffix != ".md":
                continue
            for target in self.markdown_links(path):
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

    def markdown_anchors(self, path: Path) -> set[str]:
        # Published README targets use ordinary ATX headings or explicit IDs;
        # this is not a general GitHub Markdown renderer.
        anchors, headings = set(), set()
        for line in self.markdown_lines(path):
            anchors.update(re.findall(r'\b(?:id|name)=[\"\']([^\"\']+)[\"\']', line))
            heading = re.match(r"^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
            if heading:
                slug = re.sub(r"[^\w\- ]", "", heading[1].lower()).replace(" ", "-")
                anchor, suffix = slug, 0
                while anchor in headings:
                    suffix += 1
                    anchor = f"{slug}-{suffix}"
                headings.add(anchor)
                anchors.add(anchor)
        return anchors

    def published_readme_link_failures(self, paths: set[Path]) -> list[str]:
        failures = []
        for path in sorted(paths):
            for target in self.markdown_links(path):
                url = urlsplit(target)
                destination, kind = None, "blob"
                if target.startswith("#"):
                    destination = path
                elif not url.scheme or not url.netloc:
                    failures.append(f"{path.relative_to(self.workspace)} -> {target}: relative publication link")
                    continue
                elif url.netloc == "github.com":
                    match = re.fullmatch(r"/jecsand838/ai-kit/(blob|tree)/main/(.+)", unquote(url.path))
                    if match:
                        kind = match[1]
                        destination = self.workspace / match[2]
                if destination is None:
                    continue
                destination = destination.resolve()
                exists = destination.is_file() if kind == "blob" else destination.is_dir()
                if not destination.is_relative_to(self.workspace.resolve()) or not exists:
                    failures.append(f"{path.relative_to(self.workspace)} -> {target}: missing {kind} target")
                elif url.fragment and (kind != "blob" or unquote(url.fragment) not in self.markdown_anchors(destination)):
                    failures.append(f"{path.relative_to(self.workspace)} -> {target}: missing anchor")
        return failures

    def test_packaged_readme_links_survive_publication(self) -> None:
        workspace = tomllib.loads((self.workspace / "Cargo.toml").read_text())["workspace"]
        readmes = set()
        for member in workspace["members"]:
            manifest = tomllib.loads((self.workspace / member / "Cargo.toml").read_text())
            readmes.add(self.workspace / member / manifest["package"]["readme"])
        self.assertEqual(self.published_readme_link_failures(readmes), [], "broken packaged README links")

    def test_published_readme_link_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            source = workspace / "README.md"
            guide = workspace / "docs/guide.md"
            guide.parent.mkdir()
            guide.write_text('# Get Started\n<a id="explicit"></a>\n~~~md\n# Fenced\n~~~\n', encoding="utf-8")
            base = "https://github.com/jecsand838/ai-kit/"
            cases = [
                ("docs/guide.md", False), ("../guide.md", False), ("/docs/guide.md", False),
                (base + "blob/main/docs/guide.md#get-started", True),
                (base + "blob/main/docs/guide.md#explicit", True),
                (base + "blob/main/docs/missing.md", False),
                (base + "blob/main/docs/guide.md#missing", False),
                (base + "blob/main/docs/guide.md#fenced", False),
                (base + "tree/main/docs", True), (base + "blob/main/docs", False),
                (base + "tree/main/docs/guide.md", False),
                ("#local", True), ("#missing", False),
                ("https://docs.rs/app-kit/0.0.4/app_kit/", True),
            ]
            with patch.object(self, "workspace", workspace):
                for target, allowed in cases:
                    with self.subTest(target=target):
                        source.write_text(f"# Local\n[guide]({target})\n```md\n[example](missing.md)\n```\n", encoding="utf-8")
                        self.assertEqual(self.published_readme_link_failures({source}) == [], allowed)

    def assert_storage_rustdoc_dependency_root(self) -> None:
        manifest = tomllib.loads((self.workspace / "storage-kit/Cargo.toml").read_text())
        packages = tomllib.loads((self.workspace / "Cargo.lock").read_text())["package"]
        storage = next(package for package in packages if package["name"] == "storage-kit")
        dependency = next(value.split() for value in storage["dependencies"] if value.split()[0] == "bitflags")
        versions = [package["version"] for package in packages if package["name"] == "bitflags"
                    and (len(dependency) == 1 or package["version"] == dependency[1])]
        self.assertEqual(len(versions), 1, "Storage's direct bitflags version must resolve unambiguously")
        args = manifest["package"]["metadata"]["docs"]["rs"].get("rustdoc-args", [])
        roots = []
        for index, arg in enumerate(args):
            if arg == "--extern-html-root-url":
                self.assertLess(index + 1, len(args), "external root flag needs a value")
                roots.append(args[index + 1])
            elif arg.startswith("--extern-html-root-url="):
                roots.append(arg.split("=", 1)[1])
        bitflags = [root for root in roots if root.startswith("bitflags=")]
        self.assertTrue(bitflags, "Storage needs an explicit bitflags documentation root")
        self.assertEqual(bitflags[-1], f"bitflags=https://docs.rs/bitflags/{versions[0]}")

    def test_storage_rustdoc_dependency_matches_locked_version(self) -> None:
        self.assert_storage_rustdoc_dependency_root()

    def test_storage_rustdoc_rejects_missing_or_wrong_dependency_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            (workspace / "storage-kit").mkdir()
            (workspace / "Cargo.lock").write_text('''[[package]]
name = "storage-kit"
dependencies = ["bitflags 2.11.0"]
[[package]]
name = "bitflags"
version = "1.3.2"
[[package]]
name = "bitflags"
version = "2.11.0"
''', encoding="utf-8")
            with patch.object(self, "workspace", workspace):
                for version in (None, "1.3.2", "2.11.0"):
                    with self.subTest(version=version):
                        args = [] if version is None else ["--extern-html-root-url", f"bitflags=https://docs.rs/bitflags/{version}"]
                        (workspace / "storage-kit/Cargo.toml").write_text(
                            f"[package.metadata.docs.rs]\nrustdoc-args = {args!r}\n", encoding="utf-8")
                        if version == "2.11.0":
                            self.assert_storage_rustdoc_dependency_root()
                        else:
                            with self.assertRaises(AssertionError):
                                self.assert_storage_rustdoc_dependency_root()

    def test_rustdoc_logos_work_outside_the_workspace_site(self) -> None:
        logos = set()
        for crate in ("agents-kit", "app-kit", "providers-kit", "storage-kit", "tools-kit"):
            with self.subTest(crate=crate):
                source = (self.workspace / crate / "src/lib.rs").read_text(encoding="utf-8")
                urls = re.findall(r'html_logo_url\s*=\s*"([^"]+)"', source)
                self.assertEqual(len(urls), 1, "each crate needs its shared rustdoc logo")
                # A relative URL changes meaning on nested/source pages and docs.rs
                # does not receive the workspace site's separately staged assets.
                match = re.fullmatch(
                    r"https://raw\.githubusercontent\.com/jecsand838/ai-kit/"
                    r"[0-9a-f]{40}/(docs/assets/[^/]+\.png)", urls[0],
                )
                self.assertIsNotNone(match, "use a public, commit-pinned image URL")
                self.assertTrue((self.workspace / match[1]).is_file())
                logos.add(urls[0])
        self.assertEqual(len(logos), 1, "all crates share the same logo")

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

import contextlib
import importlib.util
import io
import json
import math
import os
import re
import stat
import sys
import tempfile
import textwrap
import time
import tomllib
import unittest
from unittest import mock
import zipfile
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "vendor_update.py"


def load_vendor_update_module():
    spec = importlib.util.spec_from_file_location("vendor_update", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class AgentSdkProtocolAuthorityTests(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.workspace = SCRIPT_PATH.parents[1]
        cls.module = load_vendor_update_module()
        cls.updater = cls.module.VendorUpdater(cls.workspace)
        with (cls.workspace / "agent-sdk/vendor/vendor-lock.toml").open("rb") as handle:
            cls.vendor_lock = tomllib.load(handle)["vendors"]

    def test_selected_archives_match_lock_and_every_protocol_coordinate(self) -> None:
        protocols = {
            "codex": self.workspace / "agent-sdk/specs/codex/PROTOCOL.md",
            "opencode": self.workspace / "agent-sdk/specs/opencode/PROTOCOL.md",
        }
        failures: list[str] = []

        for vendor, protocol_path in protocols.items():
            rows = self._source_authority_rows(protocol_path)
            self.assertGreater(len(rows), 0, f"{protocol_path} has no source coordinates")
            seen_members: set[str] = set()

            with self._verified_archive(vendor) as archive:
                for member, anchors in rows:
                    if member in seen_members:
                        failures.append(f"{protocol_path} repeats archive member `{member}`")
                    seen_members.add(member)
                    source = self._archive_source(archive, member)

                    for anchor in anchors:
                        if anchor not in source:
                            failures.append(f"{member} does not contain declared anchor `{anchor}`")

        self.assertEqual(failures, [], "\n".join(failures))

    def test_agent_vendor_cargo_resolution_gates_are_locked(self) -> None:
        state = self.updater._load_crate_state("agent-sdk")
        for vendor, config in state.configs.items():
            for gate in config.completion_gates:
                if gate[:2] == ("cargo", "clippy"):
                    with self.subTest(vendor=vendor, gate=gate):
                        self.assertIn("--locked", gate)

    def test_selected_codex_wire_facts_are_sourced_without_becoming_public_api(self) -> None:
        with self._verified_archive("codex") as archive:
            for path, facts in [
                ("app-server-protocol/src/rpc.rs", [
                    "pub enum RequestId", "pub enum JSONRPCMessage",
                    "String(String)", "Integer(i64)",
                ]),
                ("app-server-protocol/src/protocol/common.rs", [
                    'ThreadStart => "thread/start"', 'TurnStart => "turn/start"',
                    'TurnInterrupt => "turn/interrupt"', 'ThreadDelete => "thread/delete"',
                ]),
                ("app-server/src/transport.rs", [
                    "codex_app_server_transport::start_stdio_connection",
                    "codex_app_server_transport::start_websocket_acceptor",
                ]),
                ("app-server-transport/src/transport/stdio.rs", [
                    "std::io::stdin().lock().lines()",
                ]),
                ("app-server-client/src/remote.rs", ["Message::Text(payload.into())"]),
                ("app-server-protocol/src/protocol/v1.rs", [
                    "pub experimental_api: bool",
                    "pub opt_out_notification_methods: Option<Vec<String>>",
                ]),
                ("app-server-protocol/src/protocol/v2/thread.rs", [
                    "pub dynamic_tools: Option<Vec<DynamicToolSpec>>",
                ]),
                ("protocol/src/dynamic_tools.rs", ["pub input_schema: JsonValue"]),
                ("app-server-protocol/src/protocol/v2/item.rs", [
                    "pub struct DynamicToolCallParams", "pub call_id: String",
                    "pub content_items: Vec<DynamicToolCallOutputContentItem>",
                ]),
            ]:
                source = self._archive_source(archive, f"codex-rs/{path}")
                for fact in facts:
                    self.assertIn(fact, source, f"selected authority lost {path}: {fact}")
            deletion = self._archive_source(
                archive, "codex-rs/app-server/src/request_processors/thread_delete.rs"
            )
            for fact in [
                ".delete_threads(StoreDeleteThreadsParams",
                "thread_ids.iter().skip(1).rev().copied().collect()",
            ]:
                self.assertIn(fact, deletion, f"selected deletion authority lost {fact}")
            self.assertLess(
                deletion.index(".send_response(request_id.clone(), response)"),
                deletion.index("self.send_thread_deleted_notifications(deleted_thread_ids)"),
                "native deletion response precedes lifecycle broadcast",
            )

        protocol = (self.workspace / "agent-sdk/specs/codex/PROTOCOL.md").read_text("utf-8")
        for contract in [
            "Typed wire decode and routing end at the protocol boundary.",
            "SDK-spawned stdio connections carry one JSON value per line.",
            "TCP and Unix-socket connections carry JSON-RPC values in WebSocket text",
            "Wire request identifiers, tool-call payloads, and response DTOs remain private.",
        ]:
            self.assertIn(contract, protocol, f"Codex protocol contract lost `{contract}`")

    def test_selected_opencode_wire_facts_are_sourced_without_becoming_public_api(self) -> None:
        with self._verified_archive("opencode") as archive:
            for path, facts in [
                ("packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts", [
                    "/session", "/prompt", '.handle("abort", abort)',
                ]),
                ("packages/opencode/src/server/routes/instance/httpapi/handlers/global.ts", [
                    '.handleRaw("event", event)', "server.heartbeat",
                ]),
                ("packages/schema/src/v1/question.ts", [
                    "QuestionRequest", "export const Reply", "export const Rejected",
                ]),
            ]:
                source = self._archive_source(archive, path)
                for fact in facts:
                    self.assertIn(fact, source, f"selected authority lost {path}: {fact}")

        protocol = (self.workspace / "agent-sdk/specs/opencode/PROTOCOL.md").read_text("utf-8")
        backend = (self.workspace / "agent-sdk/specs/opencode/BACKEND.md").read_text("utf-8")
        for contract in [
            "`GET /global/event` is the sole live event subscription.",
            "The selected foreground message HTTP response is prompt-terminal authority.",
            "Known structured non-success envelopes map through the route",
        ]:
            self.assertTrue(
                contract in protocol or contract in backend,
                f"OpenCode protocol contract lost `{contract}`",
            )

    def test_selected_opencode_inventory_and_terminal_changes_match_native_authority(self) -> None:
        with self._verified_archive("opencode") as archive:
            openapi = self._decode_openapi(self._archive_source(archive, "packages/sdk/openapi.json"))
            interleaved = openapi["components"]["schemas"]["Model"]["properties"][
                "capabilities"
            ]["properties"]["interleaved"]
            expected = {
                "anyOf": [
                    {"type": "boolean"},
                    {
                        "type": "object",
                        "properties": {"field": {"anyOf": [
                            {"type": "string", "enum": [
                                "reasoning", "reasoning_content", "reasoning_text",
                            ]},
                            {"type": "string"},
                        ]}},
                        "required": ["field"],
                        "additionalProperties": False,
                    },
                ]
            }
            # JSON serialization preserves the boolean/number distinction that
            # Python container equality would otherwise lose (False == 0).
            self.assertEqual(
                json.dumps(interleaved, sort_keys=True),
                json.dumps(expected, sort_keys=True),
            )
            for path, method in [
                ("/provider", "get"),
                ("/session", "post"),
                ("/session/{sessionID}/message", "post"),
                ("/session/{sessionID}/message/{messageID}", "get"),
                ("/permission/{requestID}/reply", "post"),
                ("/question/{requestID}/reply", "post"),
                ("/question/{requestID}/reject", "post"),
                ("/sync/history", "post"),
            ]:
                self.assertIsInstance(openapi["paths"][path][method], dict, f"{method} {path}")
            for path, facts in [
                ("packages/opencode/src/server/routes/instance/httpapi/handlers/provider.ts", [
                    "id in connected || credentials[id]",
                ]),
                ("packages/opencode/src/session/prompt.ts", [
                    "lastAssistant.parentID === lastUser.id",
                    '!["tool-calls", "unknown"].includes(lastAssistant.finish)',
                ]),
                ("packages/opencode/src/session/message-v2.ts", [
                    "info.time.created > other.time.created", "info.id > other.id",
                ]),
            ]:
                source = self._archive_source(archive, path)
                for fact in facts:
                    self.assertIn(fact, source, f"selected source lost {path}: {fact}")

    def test_openapi_rejects_non_json_constants_and_float_overflow(self) -> None:
        for value in ["NaN", "Infinity", "-Infinity", "1e999", "-1e999"]:
            with self.subTest(value=value):
                with self.assertRaisesRegex(AssertionError, "OpenAPI contains"):
                    self._decode_openapi('{"uninspected": ' + value + '}')

    def _decode_openapi(self, source: str):
        return json.loads(
            source,
            parse_constant=lambda value: self.fail(f"OpenAPI contains invalid JSON constant {value}"),
            parse_float=lambda value: float(value) if math.isfinite(float(value)) else self.fail(
                f"OpenAPI contains out-of-range JSON number {value}"
            ),
        )

    def test_archive_source_rejects_missing_and_ambiguous_members(self) -> None:
        for members in [[], ["first/src/wire.rs", "second/src/wire.rs"]]:
            with self.subTest(members=members), io.BytesIO() as buffer:
                with zipfile.ZipFile(buffer, "w") as archive:
                    for member in members:
                        archive.writestr(member, "source")
                with zipfile.ZipFile(buffer) as archive:
                    with self.assertRaisesRegex(AssertionError, "suffix matches"):
                        self._archive_source(archive, "src/wire.rs")

    def test_archive_checksum_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            archive_path = workspace / "agent-sdk/vendor/invalid.zip"
            archive_path.parent.mkdir(parents=True)
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("source/wire.rs", "source")
            lock = {"invalid": {"archive": "invalid.zip", "sha256": "0" * 64}}
            with mock.patch.object(self, "workspace", workspace), mock.patch.object(
                self, "vendor_lock", lock
            ):
                with self.assertRaisesRegex(AssertionError, "does not match vendor-lock.toml"):
                    with self._verified_archive("invalid"):
                        self.fail("mismatched archive must not open")

    def _verified_archive(self, vendor: str) -> zipfile.ZipFile:
        locked = self.vendor_lock[vendor]
        archive_path = self.workspace / "agent-sdk/vendor" / locked["archive"]
        self.assertTrue(archive_path.is_file(), f"missing {archive_path}")
        self.assertEqual(
            self.updater._sha256(archive_path),
            locked["sha256"],
            f"{archive_path} does not match vendor-lock.toml",
        )
        return zipfile.ZipFile(archive_path)

    def _archive_source(self, archive: zipfile.ZipFile, member: str) -> str:
        matching_members = [
            candidate for candidate in archive.namelist()
            if candidate == member or candidate.endswith(f"/{member}")
        ]
        self.assertEqual(
            len(matching_members), 1,
            f"{archive.filename} has {len(matching_members)} suffix matches for `{member}`",
        )
        return archive.read(matching_members[0]).decode("utf-8")

    def _source_authority_rows(self, protocol_path: Path) -> list[tuple[str, tuple[str, ...]]]:
        protocol = protocol_path.read_text(encoding="utf-8")
        heading = "## Source authority"
        self.assertEqual(protocol.count(heading), 1, f"{protocol_path} must have one {heading}")
        section = protocol.split(heading, 1)[1].split("\n## ", 1)[0]
        lines = section.splitlines()
        header = "| Archive member | Symbol anchors | Contract fact |"
        self.assertEqual(lines.count(header), 1, f"{protocol_path} must have one authority table")
        start = lines.index(header) + 2
        rows: list[tuple[str, tuple[str, ...]]] = []

        for line in lines[start:]:
            if not line.startswith("|"):
                break
            cells = [cell.strip() for cell in line[1:-1].split(" | ")]
            self.assertEqual(len(cells), 3, f"malformed authority row in {protocol_path}: {line}")
            member_spans = self._code_spans(cells[0])
            anchors = self._code_spans(cells[1])
            self.assertEqual(
                len(member_spans),
                1,
                f"authority member must be one code span in {protocol_path}: {line}",
            )
            self.assertGreater(
                len(anchors),
                0,
                f"authority row must declare symbol anchors in {protocol_path}: {line}",
            )
            rows.append((member_spans[0], anchors))

        return rows

    def _code_spans(self, cell: str) -> tuple[str, ...]:
        spans: list[str] = []
        for match in re.finditer(r"(?P<fence>`+)(?P<value>.*?)(?P=fence)", cell):
            value = match.group("value")
            if value.startswith(" ") and value.endswith(" ") and value.strip():
                value = value[1:-1]
            spans.append(value)
        return tuple(spans)


class VendorUpdateCliTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tempdir.name)
        self.bin_dir = self.workspace / "bin"
        self.bin_dir.mkdir()
        self.log_path = self.workspace / "tool-log.jsonl"
        self.config_path = self.workspace / "tool-config.json"
        self.module = load_vendor_update_module()
        self._write_fake_tools()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_storage_manifest_gates_use_locked_supported_workspace_lane(self) -> None:
        manifest = tomllib.loads(
            (SCRIPT_PATH.parents[1] / "storage-sdk/vendor/vendors.toml").read_text()
        )
        for name, config in manifest["vendors"].items():
            with self.subTest(vendor=name):
                self.assertIn("Cargo.lock", config["local_focus_paths"])
                clippy = next(gate for gate in config["completion_gates"]
                              if gate[:2] == ["cargo", "clippy"])
                self.assertIn("--locked", clippy)
                exclusions = [clippy[index + 1] for index, arg in enumerate(clippy)
                              if arg == "--exclude"]
                self.assertEqual(exclusions, ["storage-sdk", "app-sdk"])

    def test_storage_manifest_allows_scoped_cargo_lock_but_rejects_unrelated_edits(self) -> None:
        manifest = tomllib.loads(
            (SCRIPT_PATH.parents[1] / "storage-sdk/vendor/vendors.toml").read_text()
        )
        external = tuple(path for path in manifest["vendors"]["lancedb"]["local_focus_paths"]
                         if not path.startswith("storage-sdk/"))
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 7}, {"exit_code": 0}],
            external_focus_paths=external,
        )
        self.assertEqual(result["exit_code"], 1)
        (self.workspace / "Cargo.lock").write_text("# updated selected dependency graph\n")
        (self.workspace / "unrelated.txt").write_text("caller-owned data\n")
        config = json.loads(self.config_path.read_text())
        config["git"]["status_nul"] = " M Cargo.lock\0?? unrelated.txt\0"
        self._write_tool_config(config)
        code, error = self._resume(result["dossier"])
        self.assertEqual(code, 1)
        self.assertIn("outside", error)
        config["git"]["status_nul"] = " M Cargo.lock\0"
        self._write_tool_config(config)
        code, error = self._resume(result["dossier"])
        self.assertEqual(code, 0, error)
        stage = next(entry for entry in self._read_logs()
                     if entry["tool"] == "git" and entry["argv"][0] == "add")
        self.assertIn("Cargo.lock", stage["argv"])
        self.assertNotIn("unrelated.txt", stage["argv"])
        self.assertEqual((self.workspace / "unrelated.txt").read_text(), "caller-owned data\n")

    def test_check_uses_channel_specific_release_endpoints_and_reports_status(self) -> None:
        crate = self._write_crate(
            vendors_toml="""
            version = 1

            [vendors.demo]
            repo = "acme/demo"
            source_kind = "github-release-source"
            channel = "stable"
            archive_prefix = "demo"
            upstream_focus_paths = ["upstream/api"]
            local_focus_paths = ["demo-sdk/src"]
            plan_prompt_suffix = "Review for protocol drift."
            completion_gates = [["gate-ok"]]

            [vendors.alpha]
            repo = "acme/alpha"
            source_kind = "github-release-source"
            channel = "prerelease"
            archive_prefix = "alpha"
            upstream_focus_paths = ["upstream/alpha"]
            local_focus_paths = ["demo-sdk/src/alpha"]
            plan_prompt_suffix = "Review prerelease changes."
            completion_gates = [["gate-ok"]]
            """,
            vendor_lock_toml="""
            version = 1

            [vendors.demo]
            tag = "v1.0.0"
            archive = "demo-v1.0.0.zip"
            extracted_dir = "demo-v1.0.0"
            published_at = "2026-01-01T00:00:00Z"
            sha256 = "old-demo"

            [vendors.alpha]
            tag = "alpha-4"
            archive = "alpha-alpha-4.zip"
            extracted_dir = "alpha-alpha-4"
            published_at = "2026-01-02T00:00:00Z"
            sha256 = "old-alpha"
            """,
        )
        self._write_zip(crate / "vendor" / "demo-v1.0.0.zip", "demo-src-v1/README.md", "demo")
        self._write_zip(
            crate / "vendor" / "alpha-alpha-4.zip", "alpha-src-4/README.md", "alpha"
        )
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v1.1.0",
                            "published_at": "2026-02-01T12:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_list_by_repo": {
                        "acme/alpha": [
                            {
                                "tag_name": "alpha-5",
                                "published_at": "2026-02-02T12:00:00Z",
                                "prerelease": True,
                                "draft": False,
                            }
                        ]
                    }
                }
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["check", "--crate", "demo-sdk"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        output = stdout.getvalue()
        self.assertIn("demo-sdk/demo", output)
        self.assertIn("demo-sdk/alpha", output)
        self.assertIn("2026-02-01T12:00:00Z", output)
        self.assertIn("2026-02-02T12:00:00Z", output)

        gh_calls = [entry for entry in self._read_logs() if entry["tool"] == "gh"]
        demo_call = next(
            entry for entry in gh_calls if entry["argv"] == ["api", "repos/acme/demo/releases/latest"]
        )
        alpha_call = next(
            entry
            for entry in gh_calls
            if entry["argv"] == ["api", "repos/acme/alpha/releases?per_page=20&page=1"]
        )
        self.assertEqual(demo_call["argv"][1], "repos/acme/demo/releases/latest")
        self.assertEqual(alpha_call["argv"][1], "repos/acme/alpha/releases?per_page=20&page=1")

    def test_check_json_reports_machine_readable_status_in_manifest_order(self) -> None:
        crate = self._write_crate(
            vendors_toml="""
            version = 1

            [vendors.demo]
            repo = "acme/demo"
            source_kind = "github-release-source"
            channel = "stable"
            archive_prefix = "demo"
            upstream_focus_paths = ["upstream/api"]
            local_focus_paths = ["demo-sdk/src"]
            plan_prompt_suffix = "Review for protocol drift."
            completion_gates = [["gate-ok"]]

            [vendors.alpha]
            repo = "acme/alpha"
            source_kind = "github-release-source"
            channel = "prerelease"
            archive_prefix = "alpha"
            upstream_focus_paths = ["upstream/alpha"]
            local_focus_paths = ["demo-sdk/src/alpha"]
            plan_prompt_suffix = "Review prerelease changes."
            completion_gates = [["gate-ok"]]
            """,
            vendor_lock_toml="""
            version = 1

            [vendors.demo]
            tag = "v1.0.0"
            archive = "demo-v1.0.0.zip"
            extracted_dir = "demo-v1.0.0"
            published_at = "2026-01-01T00:00:00Z"
            sha256 = "old-demo"

            [vendors.alpha]
            tag = "alpha-4"
            archive = "alpha-alpha-4.zip"
            extracted_dir = "alpha-alpha-4"
            published_at = "2026-01-02T00:00:00Z"
            sha256 = "old-alpha"
            """,
        )
        self._write_zip(crate / "vendor" / "demo-v1.0.0.zip", "demo-src-v1/README.md", "demo")
        self._write_zip(
            crate / "vendor" / "alpha-alpha-4.zip", "alpha-src-4/README.md", "alpha"
        )
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v1.1.0",
                            "published_at": "2026-02-01T12:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_list_by_repo": {
                        "acme/alpha": [
                            {
                                "tag_name": "alpha-5",
                                "published_at": "2026-02-02T12:00:00Z",
                                "prerelease": True,
                                "draft": False,
                            }
                        ]
                    },
                }
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["check", "--crate", "demo-sdk", "--json"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            [entry["vendor"] for entry in payload],
            ["demo", "alpha"],
        )
        self.assertEqual(payload[0]["crate"], "demo-sdk")
        self.assertEqual(payload[0]["channel"], "stable")
        self.assertEqual(payload[0]["status"], "update-available")
        self.assertEqual(payload[0]["current"]["tag"], "v1.0.0")
        self.assertEqual(payload[0]["latest"]["tag"], "v1.1.0")
        self.assertEqual(payload[1]["channel"], "prerelease")
        self.assertEqual(payload[1]["status"], "update-available")
        self.assertEqual(payload[1]["current"]["published_at"], "2026-01-02T00:00:00Z")
        self.assertEqual(payload[1]["latest"]["published_at"], "2026-02-02T12:00:00Z")

    def test_stable_release_falls_back_to_release_list_when_latest_tag_is_on_wrong_track(self) -> None:
        crate = self._write_crate(
            vendors_toml="""
            version = 1

            [vendors.demo]
            repo = "acme/demo"
            source_kind = "github-release-source"
            channel = "stable"
            release_tag_prefix = "v"
            archive_prefix = "demo"
            upstream_focus_paths = ["upstream/api"]
            local_focus_paths = ["demo-sdk/src"]
            plan_prompt_suffix = "Review for protocol drift."
            completion_gates = [["gate-ok"]]
            """
        )
        self._write_zip(crate / "vendor" / "demo-v1.0.0.zip", "demo-src-v1/README.md", "demo")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "python-v3.1.0",
                            "published_at": "2026-02-03T12:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_list_by_repo": {
                        "acme/demo": [
                            {
                                "tag_name": "python-v3.1.0",
                                "published_at": "2026-02-03T12:00:00Z",
                                "prerelease": False,
                                "draft": False,
                            },
                            {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-02-02T12:00:00Z",
                                "prerelease": False,
                                "draft": False,
                            },
                        ]
                    },
                }
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["check", "--crate", "demo-sdk"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        self.assertIn("latest=v2.0.0 (2026-02-02T12:00:00Z)", stdout.getvalue())

        gh_calls = [entry for entry in self._read_logs() if entry["tool"] == "gh"]
        self.assertIn(
            {"tool": "gh", "argv": ["api", "repos/acme/demo/releases/latest"]},
            gh_calls,
        )
        self.assertIn(
            {"tool": "gh", "argv": ["api", "repos/acme/demo/releases?per_page=100&page=1"]},
            gh_calls,
        )

    def test_stable_release_rejects_semver_prerelease_tags_even_when_github_flags_are_clean(self) -> None:
        crate = self._write_crate(
            vendors_toml="""
            version = 1

            [vendors.demo]
            repo = "acme/demo"
            source_kind = "github-release-source"
            channel = "stable"
            release_tag_prefix = "v"
            archive_prefix = "demo"
            upstream_focus_paths = ["upstream/api"]
            local_focus_paths = ["demo-sdk/src"]
            plan_prompt_suffix = "Review for protocol drift."
            completion_gates = [["gate-ok"]]
            """
        )
        self._write_zip(crate / "vendor" / "demo-v1.0.0.zip", "demo-src-v1/README.md", "demo")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0-beta.1",
                            "published_at": "2026-02-03T12:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_list_by_repo": {
                        "acme/demo": [
                            {
                                "tag_name": "v2.0.0-beta.1",
                                "published_at": "2026-02-03T12:00:00Z",
                                "prerelease": False,
                                "draft": False,
                            },
                            {
                                "tag_name": "v1.9.0",
                                "published_at": "2026-02-02T12:00:00Z",
                                "prerelease": False,
                                "draft": False,
                            },
                        ]
                    },
                }
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["check", "--crate", "demo-sdk"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        self.assertIn("latest=v1.9.0 (2026-02-02T12:00:00Z)", stdout.getvalue())

        gh_calls = [entry for entry in self._read_logs() if entry["tool"] == "gh"]
        self.assertIn(
            {"tool": "gh", "argv": ["api", "repos/acme/demo/releases/latest"]},
            gh_calls,
        )
        self.assertIn(
            {"tool": "gh", "argv": ["api", "repos/acme/demo/releases?per_page=100&page=1"]},
            gh_calls,
        )

    def test_update_noops_when_latest_release_matches_lock(self) -> None:
        crate = self._write_crate()
        self._write_zip(crate / "vendor" / "demo-v1.0.0.zip", "demo-src-v1/README.md", "demo")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v1.0.0",
                            "published_at": "2026-01-01T00:00:00Z",
                            "prerelease": False,
                        }
                    }
                }
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        self.assertIn("already tracks v1.0.0", stdout.getvalue())

        tools = [entry["tool"] for entry in self._read_logs()]
        self.assertEqual(tools.count("gh"), 1)
        self.assertEqual(tools.count("git"), 1)
        self.assertNotIn("codex", tools)

    def test_update_downloads_canonical_names_runs_codex_and_cleans_old_vendor_on_success(self) -> None:
        crate = self._write_crate()
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        old_extract = crate / "vendor" / "demo-v1.0.0"
        self._write_zip(old_archive, "repo-sha/README.md", "old")

        new_archive_fixture = self.workspace / "download-source.zip"
        self._write_zip(new_archive_fixture, "owner-repo-deadbeef/src/api.txt", "new-api")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                    "download_archive": str(new_archive_fixture),
                    "downloaded_name": "downloaded-source.zip",
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                },
            }
        )

        env = self._env()
        env["EXPECTED_OLD_ARCHIVE"] = str(old_archive)
        env["EXPECTED_OLD_EXTRACT"] = str(old_extract)

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=env,
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        new_archive = crate / "vendor" / "demo-v2.0.0.zip"
        new_extract = crate / "vendor" / "demo-v2.0.0"
        self.assertFalse(old_archive.exists())
        self.assertFalse(old_extract.exists())
        self.assertTrue(new_archive.exists())
        self.assertTrue(new_extract.exists())
        self.assertEqual((new_extract / "src" / "api.txt").read_text(), "new-api")

        vendor_lock = (crate / "vendor" / "vendor-lock.toml").read_text()
        self.assertIn('tag = "v2.0.0"', vendor_lock)
        self.assertIn('archive = "demo-v2.0.0.zip"', vendor_lock)
        self.assertIn('extracted_dir = "demo-v2.0.0"', vendor_lock)

        dossier_root = self.workspace / "target" / "vendor-updates" / "demo-sdk" / "demo" / "v2.0.0"
        self.assertTrue((dossier_root / "plan.md").exists())
        self.assertTrue((dossier_root / "dossier.md").exists())
        self.assertTrue((dossier_root / "release.json").exists())

        codex_calls = [entry for entry in self._read_logs() if entry["tool"] == "codex"]
        self.assertEqual(len(codex_calls), 2)
        self.assertIn("read-only", codex_calls[0]["argv"])
        self.assertIn("workspace-write", codex_calls[1]["argv"])
        planning_prompt = codex_calls[0]["argv"][-1]
        implementation_prompt = codex_calls[1]["argv"][-1]
        self.assertIn("Use the `vendor-release-update` skill.", planning_prompt)
        self.assertIn(str(dossier_root / "dossier.md"), planning_prompt)
        self.assertIn(str(dossier_root / "plan.md"), planning_prompt)
        self.assertIn("Do not modify files.", planning_prompt)
        self.assertIn("Use the `vendor-release-update` skill.", implementation_prompt)
        self.assertIn(str(dossier_root / "dossier.md"), implementation_prompt)
        self.assertIn(str(dossier_root / "plan.md"), implementation_prompt)
        self.assertIn("Do not clean old vendor artifacts", implementation_prompt)
        self.assertIn("Do not create commits or push branches", implementation_prompt)

        git_calls = [entry["argv"] for entry in self._read_logs() if entry["tool"] == "git"]
        flattened = [" ".join(argv) for argv in git_calls]
        self.assertTrue(any("switch -c codex/vendor-demo-sdk-demo-v2.0.0" in call for call in flattened))
        self.assertTrue(any(call.startswith("add -A") for call in flattened))
        self.assertTrue(any(call.startswith("commit -m demo-sdk: update demo vendor to v2.0.0") for call in flattened))
        self.assertTrue(any(call.startswith("push -u origin codex/vendor-demo-sdk-demo-v2.0.0") for call in flattened))

    def test_update_strips_release_tag_prefix_from_archive_names_only(self) -> None:
        crate = self._write_crate(
            vendors_toml="""
            version = 1

            [vendors.demo]
            repo = "acme/demo"
            source_kind = "github-release-source"
            channel = "stable"
            release_tag_prefix = "v"
            archive_prefix = "demo"
            upstream_focus_paths = ["upstream/api"]
            local_focus_paths = ["demo-sdk/src"]
            plan_prompt_suffix = "Review for protocol drift."
            completion_gates = [["gate-ok"]]
            """,
            vendor_lock_toml="""
            version = 1

            [vendors.demo]
            tag = "v1.0.0"
            archive = "demo-1.0.0.zip"
            extracted_dir = "demo-1.0.0"
            published_at = "2026-01-01T00:00:00Z"
            sha256 = "old-demo"
            """,
        )
        old_archive = crate / "vendor" / "demo-1.0.0.zip"
        self._write_zip(old_archive, "repo-sha/README.md", "old")

        new_archive_fixture = self.workspace / "download-source.zip"
        self._write_zip(new_archive_fixture, "owner-repo-deadbeef/src/api.txt", "new-api")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                    "download_archive": str(new_archive_fixture),
                    "downloaded_name": "downloaded-source.zip",
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                },
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        self.assertTrue((crate / "vendor" / "demo-2.0.0.zip").exists())
        self.assertTrue((crate / "vendor" / "demo-2.0.0").exists())
        self.assertFalse((crate / "vendor" / "demo-v2.0.0.zip").exists())
        self.assertFalse((crate / "vendor" / "demo-v2.0.0").exists())

        vendor_lock = (crate / "vendor" / "vendor-lock.toml").read_text()
        self.assertIn('tag = "v2.0.0"', vendor_lock)
        self.assertIn('archive = "demo-2.0.0.zip"', vendor_lock)
        self.assertIn('extracted_dir = "demo-2.0.0"', vendor_lock)

    def test_update_current_branch_mode_commits_without_branch_creation_or_push(self) -> None:
        crate = self._write_crate()
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        old_extract = crate / "vendor" / "demo-v1.0.0"
        self._write_zip(old_archive, "repo-sha/README.md", "old")

        new_archive_fixture = self.workspace / "download-source.zip"
        self._write_zip(new_archive_fixture, "owner-repo-deadbeef/src/api.txt", "new-api")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                    "download_archive": str(new_archive_fixture),
                    "downloaded_name": "downloaded-source.zip",
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                    "branch_output": "feature/vendor-campaign\n",
                },
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo", "--git-mode", "current-branch"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        git_calls = [entry["argv"] for entry in self._read_logs() if entry["tool"] == "git"]
        flattened = [" ".join(argv) for argv in git_calls]
        self.assertIn("branch --show-current", flattened)
        self.assertFalse(any(call.startswith("switch -c") for call in flattened))
        self.assertFalse(any(call.startswith("push ") for call in flattened))
        self.assertTrue(any(call.startswith("add -A") for call in flattened))
        self.assertTrue(any(call.startswith("commit -m demo-sdk: update demo vendor to v2.0.0") for call in flattened))
        self.assertIn("updated demo-sdk/demo to v2.0.0 on feature/vendor-campaign", stdout.getvalue())

    def test_update_reuses_existing_target_archive_and_regenerates_missing_extract(self) -> None:
        crate = self._write_crate()
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        old_extract = crate / "vendor" / "demo-v1.0.0"
        self._write_zip(old_archive, "repo-sha/README.md", "old")

        target_archive = crate / "vendor" / "demo-v2.0.0.zip"
        self._write_zip(target_archive, "owner-repo-deadbeef/src/api.txt", "new-api")
        dossier_root = self.workspace / "target" / "vendor-updates" / "demo-sdk" / "demo" / "v2.0.0"
        dossier_root.mkdir(parents=True)
        (dossier_root / "plan.md").write_text("stale plan\n")

        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                },
            }
        )

        env = self._env()
        env["EXPECTED_OLD_ARCHIVE"] = str(old_archive)
        env["EXPECTED_OLD_EXTRACT"] = str(old_extract)

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=env,
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        target_extract = crate / "vendor" / "demo-v2.0.0"
        self.assertTrue(target_archive.exists())
        self.assertTrue(target_extract.exists())
        self.assertEqual((target_extract / "src" / "api.txt").read_text(), "new-api")
        self.assertEqual((dossier_root / "plan.md").read_text(), "# read-only\n")
        gh_calls = [entry["argv"] for entry in self._read_logs() if entry["tool"] == "gh"]
        self.assertFalse(any(argv[:2] == ["release", "download"] for argv in gh_calls))

    def test_update_extracts_source_archive_without_macos_metadata(self) -> None:
        crate = self._write_crate()
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        old_extract = crate / "vendor" / "demo-v1.0.0"
        self._write_zip(old_archive, "repo-sha/README.md", "old")

        target_archive = crate / "vendor" / "demo-v2.0.0.zip"
        self._write_zip_entries(
            target_archive,
            {
                "owner-repo-deadbeef/src/api.txt": "new-api",
                "__MACOSX/._owner-repo-deadbeef": "ignored root metadata",
                "__MACOSX/owner-repo-deadbeef/._src": "ignored dir metadata",
                "owner-repo-deadbeef/src/._api.txt": "ignored file metadata",
            },
        )
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                },
            }
        )

        env = self._env()
        env["EXPECTED_OLD_ARCHIVE"] = str(old_archive)
        env["EXPECTED_OLD_EXTRACT"] = str(old_extract)

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=env,
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        target_extract = crate / "vendor" / "demo-v2.0.0"
        self.assertEqual((target_extract / "src" / "api.txt").read_text(), "new-api")
        self.assertFalse((target_extract / "__MACOSX").exists())
        self.assertFalse((target_extract / "src" / "._api.txt").exists())
        self.assertFalse((target_extract / "._owner-repo-deadbeef").exists())

    def test_update_reuses_existing_target_extract_and_redownloads_missing_archive(self) -> None:
        crate = self._write_crate()
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        old_extract = crate / "vendor" / "demo-v1.0.0"
        self._write_zip(old_archive, "repo-sha/README.md", "old")

        target_extract = crate / "vendor" / "demo-v2.0.0"
        target_extract.mkdir(parents=True)
        (target_extract / "src").mkdir()
        (target_extract / "src" / "api.txt").write_text("new-api")
        new_archive_fixture = self.workspace / "download-source.zip"
        self._write_zip(new_archive_fixture, "owner-repo-deadbeef/src/api.txt", "new-api")

        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                    "download_archive": str(new_archive_fixture),
                    "downloaded_name": "downloaded-source.zip",
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                },
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertEqual(exit_code, 0, stderr.getvalue())
        target_archive = crate / "vendor" / "demo-v2.0.0.zip"
        self.assertTrue(target_archive.exists())
        self.assertEqual((target_extract / "src" / "api.txt").read_text(), "new-api")
        gh_calls = [entry["argv"] for entry in self._read_logs() if entry["tool"] == "gh"]
        self.assertTrue(any(argv[:2] == ["release", "download"] for argv in gh_calls))

    def test_update_preserves_old_archive_and_lock_when_gate_fails(self) -> None:
        crate = self._write_crate(
            vendors_toml="""
            version = 1

            [vendors.demo]
            repo = "acme/demo"
            source_kind = "github-release-source"
            channel = "stable"
            archive_prefix = "demo"
            upstream_focus_paths = ["upstream/api"]
            local_focus_paths = ["demo-sdk/src"]
            plan_prompt_suffix = "Review for protocol drift."
            completion_gates = [["gate-fail"]]
            """
        )
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        old_extract = crate / "vendor" / "demo-v1.0.0"
        self._write_zip(old_archive, "repo-sha/README.md", "old")
        old_extract.mkdir(parents=True)
        (old_extract / "README.md").write_text("old")

        new_archive_fixture = self.workspace / "download-source.zip"
        self._write_zip(new_archive_fixture, "owner-repo-deadbeef/src/api.txt", "new-api")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                    "download_archive": str(new_archive_fixture),
                    "downloaded_name": "downloaded-source.zip",
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                },
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=self._env(),
            )

        self.assertNotEqual(exit_code, 0)
        self.assertTrue(old_archive.exists())
        self.assertTrue(old_extract.exists())
        vendor_lock = (crate / "vendor" / "vendor-lock.toml").read_text()
        self.assertIn('tag = "v1.0.0"', vendor_lock)
        self.assertNotIn('tag = "v2.0.0"', vendor_lock)

    def test_no_regression_policy_preserves_strict_default_behavior(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict",
            gate_sequence=[
                {
                    "exit_code": 0,
                    "stdout": "strict gate passed\n",
                }
            ],
        )

        self.assertEqual(result["exit_code"], 0, result["stderr"])
        gate_calls = [
            entry for entry in self._read_logs() if entry["tool"] == "gate-sequence"
        ]
        self.assertEqual(len(gate_calls), 1)
        self.assertFalse((result["dossier"] / "completion-gates" / "report.json").exists())

    def test_no_regression_policy_accepts_baseline_and_post_passes(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {"exit_code": 0, "stdout": "gate passed\n"},
                {"exit_code": 0, "stdout": "gate passed\n"},
            ]
        )

        self.assertEqual(result["exit_code"], 0, result["stderr"])
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertEqual(report["policy"], "no-regression")
        self.assertEqual(report["comparisons"][0]["outcome"], "unchanged-pass")
        self.assertEqual(report["regressions"], [])
        self.assertTrue((result["dossier"] / "completion-gates" / "baseline-01.stdout.log").exists())
        self.assertTrue((result["dossier"] / "completion-gates" / "post-01.stdout.log").exists())
        metadata = json.loads((result["dossier"] / "metadata.json").read_text())
        self.assertEqual(metadata["completion_policy"], "no-regression")
        self.assertEqual(metadata["preflight_gates"], [["preflight-gate"]])
        codex_calls = [entry for entry in self._read_logs() if entry["tool"] == "codex"]
        self.assertEqual(len(codex_calls), 2)
        for call in codex_calls:
            prompt = call["argv"][-1]
            self.assertIn("completion-gates/report.json", prompt)
            self.assertIn("Baseline failures are not an implementation queue", prompt)
            self.assertIn("do not audit or implement unrelated specification backlog", prompt)
            self.assertIn("Do not use collaboration sub-agents", prompt)
        git_calls = [entry["argv"] for entry in self._read_logs() if entry["tool"] == "git"]
        self.assertTrue(any(argv and argv[0] == "commit" for argv in git_calls))

    def test_no_regression_policy_accepts_identical_pre_existing_failure(self) -> None:
        diagnostic = "error: existing failure\n  --> demo-sdk/src/lib.rs:10:4\n"
        result = self._run_no_regression_update(
            gate_sequence=[
                {"exit_code": 7, "stderr": diagnostic},
                {"exit_code": 7, "stderr": diagnostic.replace(":10:4", ":42:9")},
            ]
        )

        self.assertEqual(result["exit_code"], 0, result["stderr"])
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertEqual(report["comparisons"][0]["outcome"], "pre-existing-failure")
        self.assertEqual(report["regressions"], [])

    def test_no_regression_policy_accepts_lower_aggregate_diagnostic_counts(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {
                    "exit_code": 7,
                    "stderr": (
                        "error: existing failure\n"
                        "  --> demo-sdk/src/lib.rs:10:4\n"
                        "error: could not compile `demo-sdk` due to 2 previous errors\n"
                    ),
                },
                {
                    "exit_code": 7,
                    "stderr": (
                        "error: existing failure\n"
                        "  --> demo-sdk/src/lib.rs:42:9\n"
                        "error: could not compile `demo-sdk` due to 1 previous error\n"
                    ),
                },
            ]
        )

        self.assertEqual(result["exit_code"], 0, result["stderr"])
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertEqual(report["comparisons"][0]["outcome"], "pre-existing-failure")
        self.assertEqual(report["regressions"], [])

    def test_no_regression_policy_accepts_improved_failure(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {
                    "exit_code": 7,
                    "stderr": "error: existing failure\n  --> demo-sdk/src/lib.rs:10:4\n",
                },
                {"exit_code": 0, "stdout": "fixed\n"},
            ]
        )

        self.assertEqual(result["exit_code"], 0, result["stderr"])
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertEqual(report["comparisons"][0]["outcome"], "improved")

    def test_no_regression_policy_rejects_pass_to_fail_transition(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {"exit_code": 0, "stdout": "baseline passed\n"},
                {
                    "exit_code": 7,
                    "stderr": "error: new failure\n  --> demo-sdk/src/lib.rs:20:2\n",
                },
            ]
        )

        self.assertNotEqual(result["exit_code"], 0)
        self.assertIn("completion gate regressions", result["stderr"])
        vendor_lock = (result["crate"] / "vendor" / "vendor-lock.toml").read_text()
        self.assertIn('tag = "v1.0.0"', vendor_lock)
        self.assertNotIn('tag = "v2.0.0"', vendor_lock)
        git_calls = [entry["argv"] for entry in self._read_logs() if entry["tool"] == "git"]
        self.assertFalse(any(argv and argv[0] in {"add", "commit", "push"} for argv in git_calls))

    def test_no_regression_policy_rejects_new_diagnostic_on_failing_gate(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {
                    "exit_code": 7,
                    "stderr": "error: existing failure\n  --> demo-sdk/src/lib.rs:10:4\n",
                },
                {
                    "exit_code": 7,
                    "stderr": (
                        "error: existing failure\n"
                        "  --> demo-sdk/src/lib.rs:50:1\n"
                        "error: introduced failure\n"
                        "  --> demo-sdk/src/api.rs:8:3\n"
                    ),
                },
            ]
        )

        self.assertNotEqual(result["exit_code"], 0)
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertEqual(report["comparisons"][0]["outcome"], "regressed")
        self.assertIn("rust:demo-sdk/src/api.rs:introduced failure", report["regressions"])

    def test_no_regression_policy_rejects_changed_opaque_failure(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {"exit_code": 7, "stderr": "unclassified baseline payload\n"},
                {"exit_code": 7, "stderr": "unclassified post payload\n"},
            ]
        )

        self.assertNotEqual(result["exit_code"], 0)
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertEqual(report["comparisons"][0]["outcome"], "regressed")
        self.assertTrue(any(item.startswith("opaque:") for item in report["regressions"]))

    def test_no_regression_policy_rejects_changed_test_failure_message(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {
                    "exit_code": 7,
                    "stderr": (
                        "test strict_live::codex ... FAILED\n"
                        "thread 'strict_live::codex' (1234) panicked at tests/live.rs:10:4:\n"
                        "existing backend failure\n"
                    ),
                },
                {
                    "exit_code": 7,
                    "stderr": (
                        "test strict_live::codex ... FAILED\n"
                        "thread 'strict_live::codex' (9876) panicked at tests/live.rs:42:9:\n"
                        "new backend failure\n"
                    ),
                },
            ]
        )

        self.assertNotEqual(result["exit_code"], 0)
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertIn(
            "test-error:strict_live::codex:new backend failure",
            report["regressions"],
        )

    def test_no_regression_policy_rejects_baseline_prerequisite_failure(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {
                    "exit_code": 7,
                    "stderr": "curl: failed to connect to strict-live service\n",
                },
                {"exit_code": 0, "stdout": "unused post\n"},
            ]
        )

        self.assertNotEqual(result["exit_code"], 0)
        self.assertIn("unavailable prerequisites", result["stderr"])
        tools = [entry["tool"] for entry in self._read_logs()]
        self.assertNotIn("codex", tools)
        report = json.loads(
            (result["dossier"] / "completion-gates" / "report.json").read_text()
        )
        self.assertEqual(report["post"], [])

    def test_no_regression_policy_rejects_failed_preflight_before_baseline(self) -> None:
        result = self._run_no_regression_update(
            gate_sequence=[
                {"exit_code": 0, "stdout": "unused baseline\n"},
                {"exit_code": 0, "stdout": "unused post\n"},
            ],
            preflight_exit_code=9,
        )

        self.assertNotEqual(result["exit_code"], 0)
        self.assertIn("run preflight gate", result["stderr"])
        tools = [entry["tool"] for entry in self._read_logs()]
        self.assertIn("preflight-gate", tools)
        self.assertNotIn("gate-sequence", tools)
        self.assertNotIn("codex", tools)

    def test_update_timeout_preserves_old_lock_and_prepared_artifacts(self) -> None:
        crate = self._write_crate()
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        old_extract = crate / "vendor" / "demo-v1.0.0"
        self._write_zip(old_archive, "repo-sha/README.md", "old")

        new_archive_fixture = self.workspace / "download-source.zip"
        self._write_zip(new_archive_fixture, "owner-repo-deadbeef/src/api.txt", "new-api")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                    "download_archive": str(new_archive_fixture),
                    "downloaded_name": "downloaded-source.zip",
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                },
                "codex": {
                    "sleep_seconds_by_sandbox": {
                        "read-only": 0.2,
                    }
                },
            }
        )

        env = self._env()
        env["VENDOR_UPDATE_CODEX_PLANNING_TIMEOUT_SECONDS"] = "0.05"

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                ["update", "--crate", "demo-sdk", "--vendor", "demo"],
                workspace_root=self.workspace,
                env=env,
            )

        self.assertNotEqual(exit_code, 0)
        self.assertIn("timed out", stderr.getvalue())
        self.assertTrue(old_archive.exists())
        self.assertTrue(old_extract.exists())
        self.assertTrue((crate / "vendor" / "demo-v2.0.0.zip").exists())
        self.assertTrue((crate / "vendor" / "demo-v2.0.0").exists())
        dossier_root = self.workspace / "target" / "vendor-updates" / "demo-sdk" / "demo" / "v2.0.0"
        self.assertTrue((dossier_root / "dossier.md").exists())
        vendor_lock = (crate / "vendor" / "vendor-lock.toml").read_text()
        self.assertIn('tag = "v1.0.0"', vendor_lock)
        self.assertNotIn('tag = "v2.0.0"', vendor_lock)
        git_calls = [entry["argv"] for entry in self._read_logs() if entry["tool"] == "git"]
        self.assertFalse(any(argv and argv[0] in {"add", "commit", "push"} for argv in git_calls))

    def test_candidate_lock_is_visible_to_implementation_and_target_gates(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 0}],
        )
        self.assertEqual(result["exit_code"], 0, result["stderr"])
        calls = self._read_logs()
        implementation = next(entry for entry in calls if entry["tool"] == "codex"
                              and "workspace-write" in entry["argv"])
        self.assertEqual(implementation["lock_tag"], "v2.0.0")
        target_preflight = [entry for entry in calls if entry["tool"] == "preflight-gate"]
        self.assertEqual(len(target_preflight), 1)
        self.assertEqual(target_preflight[0]["lock_tag"], "v2.0.0")
        self.assertLess(calls.index(implementation), calls.index(target_preflight[0]))
        gate = next(entry for entry in calls if entry["tool"] == "gate-sequence")
        self.assertEqual(gate["lock_tag"], "v2.0.0")
        self.assertLess(calls.index(target_preflight[0]), calls.index(gate))

    def test_resume_preserves_edits_and_plan_without_resolving_release(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 7}, {"exit_code": 0}],
        )
        self.assertEqual(result["exit_code"], 1)
        original = (result["dossier"] / "original-vendor-lock.toml").read_bytes()
        lock = result["crate"] / "vendor/vendor-lock.toml"
        self.assertEqual(lock.read_bytes(), original)
        edit = result["crate"] / "src/lib.rs"
        edit.write_text("// retained implementation\n")
        plan = result["dossier"] / "plan.md"
        plan.write_text("# Keep the saved plan\n")
        config = json.loads(self.config_path.read_text())
        config["git"]["status_nul"] = " M demo-sdk/src/lib.rs\0"
        self._write_tool_config(config)
        calls_before = len(self._read_logs())
        code, error = self._resume(result["dossier"])
        self.assertEqual(code, 0, error)
        calls = self._read_logs()[calls_before:]
        self.assertFalse(any(entry["tool"] == "gh" for entry in calls))
        self.assertEqual(len([entry for entry in calls if entry["tool"] == "codex"]), 1)
        self.assertEqual(plan.read_text(), "# Keep the saved plan\n")
        self.assertEqual(edit.read_text(), "// retained implementation\n")
        stage = next(entry for entry in calls if entry["tool"] == "git" and entry["argv"][0] == "add")
        self.assertIn("--", stage["argv"])
        self.assertIn("demo-sdk/src/lib.rs", stage["argv"])
        self.assertNotIn(".", stage["argv"])

    def test_resume_rejects_changed_branch_head_archive_lock_and_unrelated_edits(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 7}],
        )
        self.assertEqual(result["exit_code"], 1)
        base_config = json.loads(self.config_path.read_text())
        for key, value, message in [
            ("branch_output", "different-branch", "branch"),
            ("head_output", "b" * 40, "HEAD"),
            ("status_nul", " M storage-sdk/src/lib.rs\0", "outside"),
        ]:
            with self.subTest(key=key):
                config = json.loads(json.dumps(base_config))
                config["git"][key] = value
                self._write_tool_config(config)
                code, error = self._resume(result["dossier"])
                self.assertEqual(code, 1)
                self.assertIn(message, error)
        self._write_tool_config(base_config)
        lock = result["crate"] / "vendor/vendor-lock.toml"
        old = lock.read_bytes()
        lock.write_bytes(old + b"# unexpected edit\n")
        code, error = self._resume(result["dossier"])
        self.assertEqual(code, 1)
        self.assertIn("lock", error)
        self.assertEqual(lock.read_bytes(), old + b"# unexpected edit\n")
        lock.write_bytes(old)
        archive = result["crate"] / "vendor/demo-v2.0.0.zip"
        archive.write_bytes(archive.read_bytes() + b"tampered")
        code, error = self._resume(result["dossier"])
        self.assertEqual(code, 1)
        self.assertIn("archive", error)

    def test_prerequisite_import_preserves_inputs_and_resumes_without_discovery(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        source = result["dossier"]
        saved = {path.relative_to(source).as_posix(): path.read_bytes()
                 for path in source.rglob("*") if path.is_file()}
        edit = result["crate"] / "src/lib.rs"
        edit.write_text("// retained vendor implementation\n")
        config = json.loads(self.config_path.read_text())
        config["git"]["status_nul"] = " M demo-sdk/src/lib.rs\0"
        self._write_tool_config(config)
        calls_before = len(self._read_logs())
        code, error = self._import_prerequisite(source, destination)
        self.assertEqual(code, 0, error)
        calls = self._read_logs()[calls_before:]
        self.assertTrue(all(call["tool"] == "git" for call in calls), calls)
        self.assertFalse(any(call["argv"][0] in {"add", "commit", "switch", "push"}
                             for call in calls))
        self.assertEqual((result["crate"] / "vendor/vendor-lock.toml").read_bytes(),
                         result["original_lock_bytes"])
        for name, content in saved.items():
            self.assertEqual((source / name).read_bytes(), content, name)
        imported = json.loads((destination / "resume.json").read_text())
        self.assertEqual(imported["head"], "b" * 40)
        self.assertEqual(imported["permitted_scope"], ["demo-sdk", "Cargo.lock"])
        self.assertEqual((destination / "source-resume.json").read_bytes(), saved["resume.json"])
        for name in ["plan.md", "metadata.json", "dossier.md", "release.json",
                     "original-vendor-lock.toml", "candidate-vendor-lock.toml"]:
            self.assertEqual((destination / name).read_bytes(), saved[name], name)
        code, error = self._resume(source)
        self.assertEqual(code, 1)
        self.assertIn("HEAD", error)
        calls_before = len(self._read_logs())
        code, error = self._resume(destination)
        self.assertEqual(code, 0, error)
        calls = self._read_logs()[calls_before:]
        self.assertFalse(any(call["tool"] == "gh" for call in calls))
        self.assertEqual(len([call for call in calls if call["tool"] == "codex"]), 1)
        self.assertEqual(len([call for call in calls if call["tool"] == "preflight-gate"]), 1)
        self.assertEqual(len([call for call in calls if call["tool"] == "gate-sequence"]), 1)
        self.assertEqual(edit.read_text(), "// retained vendor implementation\n")
        for name, content in saved.items():
            self.assertEqual((source / name).read_bytes(), content, name)
        prompt = next(call["argv"][-1] for call in calls if call["tool"] == "codex")
        self.assertIn("Do not write to the original imported dossier", prompt)
        self.assertIn(str(destination), prompt)

    def test_prerequisite_import_rejects_unrelated_history_and_vendor_commits(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        base = json.loads(self.config_path.read_text())
        for changes, paths, message in [
            ({"head_output": "c" * 40}, ["app-sdk/src/lib.rs"], "HEAD"),
            ({"branch_output": "another-branch"}, ["app-sdk/src/lib.rs"], "branch"),
            ({"prerequisite_parents": "c" * 40}, ["app-sdk/src/lib.rs"], "parent"),
            ({"prerequisite_parents": "a" * 40 + " " + "c" * 40},
             ["app-sdk/src/lib.rs"], "parent"),
            ({"prerequisite_paths": "demo-sdk/src/lib.rs\0"}, ["demo-sdk/src/lib.rs"], "scope"),
            ({"prerequisite_paths": "Cargo.lock\0"}, ["Cargo.lock"], "scope"),
            ({"prerequisite_paths": "agent-sdk/vendor/vendor-lock.toml\0"},
             ["agent-sdk/vendor/vendor-lock.toml"], "vendor"),
            ({"prerequisite_paths": "tmp/history.md\0"}, ["tmp/history.md"], "path"),
            ({"prerequisite_paths": "app-sdk/src/lib.rs\0unrelated.txt\0"},
             ["app-sdk/src/lib.rs"], "exact"),
            ({"prerequisite_paths": ""}, ["app-sdk/src/lib.rs"], "exact"),
            ({"prerequisite_new_mode": "120000"}, ["app-sdk/src/lib.rs"], "regular"),
            ({"prerequisite_old_mode": "160000"}, ["app-sdk/src/lib.rs"], "regular"),
        ]:
            with self.subTest(changes=changes):
                config = json.loads(json.dumps(base))
                config["git"].update(changes)
                self._write_tool_config(config)
                code, error = self._import_prerequisite(result["dossier"], destination, paths=paths)
                self.assertEqual(code, 1, error)
                self.assertIn(message, error)
                self.assertFalse(destination.exists())

    def test_prerequisite_import_rejects_broad_or_ambiguous_path_allowlists(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        for paths in [[], ["app-sdk"], ["app-sdk/src/../src/lib.rs"], ["/app-sdk/src/lib.rs"],
                      ["app-sdk/src/lib.rs", "app-sdk/src/lib.rs"]]:
            with self.subTest(paths=paths):
                code, error = self._import_prerequisite(result["dossier"], destination, paths=paths)
                self.assertNotEqual(code, 0, error)
                self.assertFalse(destination.exists())

    def test_prerequisite_import_forbids_cargo_lock_without_manifest_scope(self) -> None:
        result, destination = self._prerequisite_import_fixture(external_focus_paths=())
        config = json.loads(self.config_path.read_text())
        config["git"]["prerequisite_paths"] = "Cargo.lock\0"
        self._write_tool_config(config)
        code, error = self._import_prerequisite(result["dossier"], destination, paths=["Cargo.lock"])
        self.assertEqual(code, 1, error)
        self.assertIn("scope", error)
        self.assertFalse(destination.exists())

    def test_prerequisite_import_rejects_dirty_prerequisite_and_changed_frozen_inputs(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        source = result["dossier"]
        config = json.loads(self.config_path.read_text())
        config["git"]["status_nul"] = " M app-sdk/src/lib.rs\0"
        self._write_tool_config(config)
        code, error = self._import_prerequisite(source, destination)
        self.assertEqual(code, 1)
        self.assertIn("outside", error)
        config["git"]["status_nul"] = ""
        self._write_tool_config(config)
        for path in [source / "metadata.json", source / "candidate-vendor-lock.toml",
                     result["crate"] / "vendor/vendors.toml",
                     result["crate"] / "vendor/demo-v2.0.0.zip"]:
            with self.subTest(path=path):
                original = path.read_bytes()
                path.write_bytes(original + b"\n")
                code, error = self._import_prerequisite(source, destination)
                self.assertEqual(code, 1, error)
                self.assertFalse(destination.exists())
                path.write_bytes(original)

    def test_prerequisite_import_rejects_unsupported_modes_and_existing_imports(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        path = result["dossier"] / "resume.json"
        original = path.read_bytes()
        for key, value in [("version", 2), ("git_mode", "managed"),
                           ("completion_policy", "no-regression"),
                           ("prerequisite_import", {})]:
            with self.subTest(key=key):
                state = json.loads(original)
                state[key] = value
                path.write_text(json.dumps(state))
                code, error = self._import_prerequisite(result["dossier"], destination)
                self.assertEqual(code, 1)
                self.assertIn("strict current-branch", error)
                self.assertFalse(destination.exists())
        path.write_bytes(original)

    def test_prerequisite_import_requires_new_ignored_unredirected_destination(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        destination.mkdir()
        sentinel = destination / "untouched"
        sentinel.write_text("preserve")
        code, error = self._import_prerequisite(result["dossier"], destination)
        self.assertEqual(code, 1)
        self.assertIn("new", error)
        self.assertEqual(sentinel.read_text(), "preserve")
        config = json.loads(self.config_path.read_text())
        config["git"]["check_ignore_exit"] = 1
        self._write_tool_config(config)
        code, error = self._import_prerequisite(result["dossier"], destination.with_name("not-ignored"))
        self.assertEqual(code, 1)
        self.assertIn("ignored", error)
        config["git"]["check_ignore_exit"] = 0
        self._write_tool_config(config)
        with tempfile.TemporaryDirectory() as outside_dir:
            link = destination.with_name("redirect")
            link.symlink_to(outside_dir, target_is_directory=True)
            for target in [link / "new", Path(outside_dir) / "new",
                           result["dossier"] / "nested"]:
                code, error = self._import_prerequisite(result["dossier"], target)
                self.assertEqual(code, 1, error)
                self.assertFalse(target.exists())

    def test_prerequisite_resume_revalidates_lineage_inputs_and_commit_proof(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        code, error = self._import_prerequisite(result["dossier"], destination)
        self.assertEqual(code, 0, error)
        for owner in [result["dossier"], destination]:
            for name in ["plan.md", "release.json", "dossier.md"]:
                with self.subTest(owner=owner, name=name):
                    path = owner / name
                    original = path.read_bytes()
                    path.write_bytes(original + b"\nchanged")
                    before = len(self._read_logs())
                    code, error = self._resume(destination)
                    self.assertEqual(code, 1)
                    self.assertIn("input", error)
                    self.assertTrue(all(call["tool"] == "git" for call in self._read_logs()[before:]))
                    path.write_bytes(original)
        config = json.loads(self.config_path.read_text())
        config["git"]["prerequisite_paths"] = "app-sdk/src/lib.rs\0Cargo.lock\0"
        self._write_tool_config(config)
        code, error = self._resume(destination)
        self.assertEqual(code, 1)
        self.assertIn("exact", error)

    def test_prerequisite_resume_rejects_updater_output_symlinks_before_writes(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        code, error = self._import_prerequisite(result["dossier"], destination)
        self.assertEqual(code, 0, error)
        with tempfile.TemporaryDirectory() as outside_dir:
            outside = Path(outside_dir) / "owned-elsewhere"
            for name in ["resume.json", "implementation.md", "original-archive.zip"]:
                with self.subTest(name=name):
                    path = destination / name
                    original = path.read_bytes() if path.exists() else None
                    if original is not None:
                        path.unlink()
                    content = original if original is not None else b"preserve external file\n"
                    outside.write_bytes(content)
                    path.symlink_to(outside)
                    before = len(self._read_logs())
                    code, error = self._resume(destination)
                    self.assertEqual(code, 1, error)
                    self.assertIn("output", error)
                    self.assertEqual(outside.read_bytes(), content)
                    self.assertTrue(all(call["tool"] == "git" for call in self._read_logs()[before:]))
                    path.unlink()
                    if original is not None:
                        path.write_bytes(original)

    def test_prerequisite_resume_rejects_redirected_dossier_directory(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        code, error = self._import_prerequisite(result["dossier"], destination)
        self.assertEqual(code, 0, error)
        moved = destination.with_name("moved-prerequisite")
        destination.rename(moved)
        destination.symlink_to(moved, target_is_directory=True)
        before = len(self._read_logs())
        code, error = self._resume(destination)
        self.assertEqual(code, 1, error)
        self.assertIn("symlink", error)
        self.assertTrue(all(call["tool"] == "git" for call in self._read_logs()[before:]))

    def test_prerequisite_resume_revalidates_after_implementation_and_gates(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        code, error = self._import_prerequisite(result["dossier"], destination)
        self.assertEqual(code, 0, error)
        for method, path in [
            ("_run_codex_implementation", result["dossier"] / "execution-evidence/failure.log"),
            ("_run_codex_implementation", destination / "resume.json"),
            ("_run_completion_gates", result["dossier"] / "plan.md"),
        ]:
            with self.subTest(method=method, path=path):
                original_bytes = path.read_bytes()
                original_method = getattr(self.module.VendorUpdater, method)
                def mutate_after(updater, *args, **kwargs):
                    original_method(updater, *args, **kwargs)
                    path.write_bytes(original_bytes + b" ")
                before = len(self._read_logs())
                with mock.patch.object(self.module.VendorUpdater, method, mutate_after):
                    code, error = self._resume(destination)
                self.assertEqual(code, 1, error)
                self.assertIn("changed", error)
                calls = self._read_logs()[before:]
                self.assertFalse(any(call["tool"] == "git" and call["argv"][0] in {"add", "commit"}
                                     for call in calls))
                if method == "_run_codex_implementation":
                    self.assertFalse(any(call["tool"] in {"preflight-gate", "gate-sequence"}
                                         for call in calls))
                self.assertEqual((result["crate"] / "vendor/vendor-lock.toml").read_bytes(),
                                 result["original_lock_bytes"])
                path.write_bytes(original_bytes)

    def test_prerequisite_resume_rejects_changed_source_evidence_inventory(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        code, error = self._import_prerequisite(result["dossier"], destination)
        self.assertEqual(code, 0, error)
        evidence = result["dossier"] / "execution-evidence/failure.log"
        original = evidence.read_bytes()
        for action in ("remove", "add"):
            with self.subTest(action=action):
                changed = evidence if action == "remove" else evidence.with_name("late.log")
                if action == "remove":
                    changed.unlink()
                else:
                    changed.write_text("late evidence\n")
                code, error = self._resume(destination)
                self.assertEqual(code, 1, error)
                self.assertIn("inventory", error)
                if action == "remove":
                    changed.write_bytes(original)
                else:
                    changed.unlink()

    def test_prerequisite_resume_rejects_removed_lineage_before_execution(self) -> None:
        result, destination = self._prerequisite_import_fixture()
        code, error = self._import_prerequisite(result["dossier"], destination)
        self.assertEqual(code, 0, error)
        recovery = destination / "resume.json"
        state = json.loads(recovery.read_text())
        del state["prerequisite_import"]
        recovery.write_text(json.dumps(state))
        before = len(self._read_logs())
        code, error = self._resume(destination)
        self.assertEqual(code, 1, error)
        self.assertIn("lineage", error)
        self.assertTrue(all(call["tool"] == "git" for call in self._read_logs()[before:]))

    def _prerequisite_import_fixture(self, *, external_focus_paths=("Cargo.lock",)):
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 7}, {"exit_code": 0}],
            external_focus_paths=external_focus_paths,
        )
        self.assertEqual(result["exit_code"], 1)
        evidence = result["dossier"] / "execution-evidence/failure.log"
        evidence.parent.mkdir()
        evidence.write_text("retained failed gate evidence\n")
        prerequisite = self.workspace / "app-sdk/src/lib.rs"
        prerequisite.parent.mkdir(parents=True)
        prerequisite.write_text("//! Corrected rustdoc link.\n")
        config = json.loads(self.config_path.read_text())
        config["git"].update(head_output="b" * 40, prerequisite_parents="a" * 40,
                              prerequisite_paths="app-sdk/src/lib.rs\0")
        self._write_tool_config(config)
        return result, result["dossier"].with_name("v2.0.0-prerequisite")

    def _import_prerequisite(self, source, destination, *, paths=None):
        args = ["import-prerequisite", "--dossier", str(source), "--destination", str(destination),
                "--commit", "b" * 40]
        for path in ["app-sdk/src/lib.rs"] if paths is None else paths:
            args.extend(["--path", path])
        stderr = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(stderr):
            try:
                code = self.module.main(args, workspace_root=self.workspace, env=self._env())
            except SystemExit as error:
                code = error.code
        return code, stderr.getvalue()

    def test_interruption_restores_exact_lock_and_retains_both_archives(self) -> None:
        # Inject the interruption at the accepted implementation boundary.
        original_implementation = self.module.VendorUpdater._run_codex_implementation
        def interrupted(updater, *args, **kwargs):
            original_implementation(updater, *args, **kwargs)
            raise KeyboardInterrupt
        with mock.patch.object(self.module.VendorUpdater, "_run_codex_implementation", interrupted):
            with self.assertRaises(KeyboardInterrupt):
                self._run_no_regression_update(
                    completion_policy="strict", gate_sequence=[{"exit_code": 0}],
                )
        dossier = self.workspace / "target/vendor-updates/demo-sdk/demo/v2.0.0"
        original = (dossier / "original-vendor-lock.toml").read_bytes()
        vendor = self.workspace / "demo-sdk/vendor"
        self.assertEqual((vendor / "vendor-lock.toml").read_bytes(), original)
        for name in ["demo-v1.0.0.zip", "demo-v2.0.0.zip", "demo-v1.0.0", "demo-v2.0.0"]:
            self.assertTrue((vendor / name).exists(), name)
        self.assertFalse(any(entry["tool"] == "git" and entry["argv"][0] in {"add", "commit"}
                             for entry in self._read_logs()))

    def test_commit_interruption_preserves_a_successfully_committed_target(self) -> None:
        original_run = self.module.VendorUpdater._run
        def interrupted_after_commit(updater, args, **kwargs):
            result = original_run(updater, args, **kwargs)
            if args[:2] == ["git", "commit"]:
                raise KeyboardInterrupt
            return result
        with mock.patch.object(self.module.VendorUpdater, "_run", interrupted_after_commit):
            with self.assertRaises(KeyboardInterrupt):
                self._run_no_regression_update(
                    completion_policy="strict", gate_sequence=[{"exit_code": 0}],
                )
        vendor = self.workspace / "demo-sdk/vendor"
        lock = tomllib.loads((vendor / "vendor-lock.toml").read_text())
        self.assertEqual(lock["vendors"]["demo"]["tag"], "v2.0.0")
        self.assertFalse((vendor / "demo-v1.0.0.zip").exists())
        self.assertTrue((vendor / "demo-v2.0.0.zip").exists())

    def test_resume_accepts_candidate_lock_and_retains_no_regression_baseline(self) -> None:
        result = self._run_no_regression_update(gate_sequence=[
            {"exit_code": 0}, {"exit_code": 7}, {"exit_code": 0},
        ])
        self.assertEqual(result["exit_code"], 1)
        candidate = (result["dossier"] / "candidate-vendor-lock.toml").read_bytes()
        (result["crate"] / "vendor/vendor-lock.toml").write_bytes(candidate)
        calls_before = len(self._read_logs())
        code, error = self._resume(result["dossier"])
        self.assertEqual(code, 0, error)
        calls = self._read_logs()[calls_before:]
        self.assertEqual(len([call for call in calls if call["tool"] == "gate-sequence"]), 1)
        self.assertEqual(len([call for call in calls if call["tool"] == "preflight-gate"]), 1)
        report = json.loads((result["dossier"] / "completion-gates/report.json").read_text())
        self.assertEqual(report["baseline"][0]["returncode"], 0)
        self.assertEqual(report["post"][0]["returncode"], 0)

    def test_resume_rejects_symlink_escapes_without_modifying_the_target(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 7}],
        )
        with tempfile.TemporaryDirectory() as outside_dir:
            outside = Path(outside_dir) / "outside.rs"
            outside.write_text("// preserve outside file\n")
            other_crate = self.workspace / "storage-sdk"
            other_crate.mkdir()
            other_file = other_crate / "lib.rs"
            other_file.write_text("// preserve outside file\n")
            link = result["crate"] / "src/outside.rs"
            config = json.loads(self.config_path.read_text())
            config["git"]["status_nul"] = "?? demo-sdk/src/outside.rs\0"
            self._write_tool_config(config)
            for destination in (outside, other_file):
                with self.subTest(destination=destination):
                    link.symlink_to(destination)
                    code, error = self._resume(result["dossier"])
                    self.assertEqual(code, 1)
                    self.assertIn("outside", error)
                    self.assertEqual(destination.read_text(), "// preserve outside file\n")
                    link.unlink()

    def test_staging_excludes_unchanged_preexisting_untracked_files(self) -> None:
        note = self.workspace / "notes.txt"
        note.write_text("caller notes\n")
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 0}],
            git_config={"status_nul": "?? notes.txt\0"},
        )
        self.assertEqual(result["exit_code"], 0, result["stderr"])
        stages = [call["argv"] for call in self._read_logs()
                  if call["tool"] == "git" and call["argv"][0] == "add"]
        self.assertEqual(len(stages), 1)
        self.assertNotIn("notes.txt", stages[0])
        self.assertEqual(note.read_text(), "caller notes\n")

    def test_resume_rejects_inconsistent_recorded_target(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 7}],
        )
        resume_file = result["dossier"] / "resume.json"
        original = json.loads(resume_file.read_text())
        for field, value in [("archive", "demo-v1.0.0.zip"), ("tag", "v3.0.0")]:
            state = json.loads(json.dumps(original))
            state["target"][field] = value
            resume_file.write_text(json.dumps(state))
            code, error = self._resume(result["dossier"])
            self.assertEqual(code, 1)
            self.assertIn("target", error)
        resume_file.write_text(json.dumps(original))
        for name, value in [("completion_policy", "skip"), ("git_mode", "new-mode")]:
            state = json.loads(json.dumps(original))
            state[name] = value
            resume_file.write_text(json.dumps(state))
            code, error = self._resume(result["dossier"])
            self.assertEqual(code, 1)
            self.assertIn("invalid resume", error)

    def test_strict_target_preflight_failure_restores_exact_original_lock(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict", preflight_exit_code=9,
            gate_sequence=[{"exit_code": 0}],
        )
        self.assertEqual(result["exit_code"], 1)
        self.assertEqual((result["crate"] / "vendor/vendor-lock.toml").read_bytes(),
                         result["original_lock_bytes"])
        calls = self._read_logs()
        self.assertTrue(any(call["tool"] == "codex" and "workspace-write" in call["argv"]
                            for call in calls))
        self.assertFalse(any(call["tool"] == "gate-sequence" for call in calls))
        self.assertFalse(any(call["tool"] == "git" and call["argv"][0] in {"add", "commit"}
                             for call in calls))

    def test_implementation_timeout_stops_descendants_before_lock_rollback(self) -> None:
        env = self._env()
        env["VENDOR_UPDATE_CODEX_IMPLEMENTATION_TIMEOUT_SECONDS"] = "0.4"
        marker = self.workspace / "late-child-write"
        with mock.patch.object(self, "_env", return_value=env):
            result = self._run_no_regression_update(
                completion_policy="strict", gate_sequence=[{"exit_code": 0}],
                codex_config={"sleep_seconds_by_sandbox": {"workspace-write": 2},
                              "child_marker": str(marker)},
            )
        self.assertEqual(result["exit_code"], 1)
        self.assertIn("implement demo-sdk/demo update timed out", result["stderr"])
        self.assertTrue(marker.with_suffix(".started").exists())
        time.sleep(0.9)
        self.assertFalse(marker.exists(), "timed-out implementation detached a writing child")
        self.assertEqual((result["crate"] / "vendor/vendor-lock.toml").read_bytes(),
                         result["original_lock_bytes"])
        implementation = next(call for call in self._read_logs()
                              if call["tool"] == "codex" and "workspace-write" in call["argv"])
        self.assertEqual(implementation["lock_tag"], "v2.0.0")
        self.assertFalse(any(call["tool"] == "git" and call["argv"][0] in {"add", "commit"}
                             for call in self._read_logs()))

    def _resume(self, dossier: Path) -> tuple[int, str]:
        stderr = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(stderr):
            code = self.module.main(["resume", "--dossier", str(dossier)],
                                    workspace_root=self.workspace, env=self._env())
        return code, stderr.getvalue()

    def test_failed_candidate_gate_restores_exact_lock_and_retains_archives(self) -> None:
        result = self._run_no_regression_update(
            completion_policy="strict", gate_sequence=[{"exit_code": 23}]
        )
        self.assertEqual(result["exit_code"], 1)
        self.assertIn("23", result["stderr"])
        gate = next(entry for entry in self._read_logs() if entry["tool"] == "gate-sequence")
        self.assertEqual(gate["lock_tag"], "v2.0.0")
        self.assertEqual(
            (result["crate"] / "vendor/vendor-lock.toml").read_bytes(), result["original_lock_bytes"]
        )
        for name in ("demo-v1.0.0.zip", "demo-v2.0.0.zip", "demo-v1.0.0", "demo-v2.0.0"):
            self.assertTrue((result["crate"] / "vendor" / name).exists())
        self.assertFalse(any(
            entry["tool"] == "git" and entry["argv"][0] in {"add", "commit", "push"}
            for entry in self._read_logs()
        ))

    def test_interrupted_candidate_gate_restores_lock_without_finalizing(self) -> None:
        run_gates = self.module.VendorUpdater._run_completion_gates
        write_crate = self._write_crate
        unrelated = {
            "tag": "v8.0.0", "archive": "other.zip", "extracted_dir": "other",
            "published_at": "2026-01-01T00:00:00Z", "sha256": "untouched",
        }

        def with_unrelated_lock(**kwargs):
            crate = write_crate(**kwargs)
            with (crate / "vendor/vendor-lock.toml").open("a") as handle:
                handle.write("\n[vendors.unrelated]\n")
                for key, value in unrelated.items():
                    handle.write(f"{key} = {json.dumps(value)}\n")
            return crate

        def interrupt_after_gate(updater, config):
            run_gates(updater, config)
            lock = self.workspace / "demo-sdk/vendor/vendor-lock.toml"
            self.assertEqual(tomllib.loads(lock.read_text())["vendors"]["unrelated"], unrelated)
            raise KeyboardInterrupt("interrupted validation")

        with mock.patch.object(self, "_write_crate", side_effect=with_unrelated_lock), \
             mock.patch.object(self.module.VendorUpdater, "_run_completion_gates", interrupt_after_gate):
            with self.assertRaisesRegex(KeyboardInterrupt, "interrupted validation"):
                self._run_no_regression_update(
                    completion_policy="strict", gate_sequence=[{"exit_code": 0}]
                )
        gate = next(entry for entry in self._read_logs() if entry["tool"] == "gate-sequence")
        self.assertEqual(gate["lock_tag"], "v2.0.0")
        vendor = self.workspace / "demo-sdk/vendor"
        restored = tomllib.loads((vendor / "vendor-lock.toml").read_text())["vendors"]
        self.assertEqual(restored["demo"]["tag"], "v1.0.0")
        self.assertEqual(restored["unrelated"], unrelated)
        self.assertTrue((vendor / "demo-v1.0.0.zip").exists())
        self.assertTrue((vendor / "demo-v2.0.0.zip").exists())
        self.assertFalse(any(
            entry["tool"] == "git" and entry["argv"][0] in {"add", "commit", "push"}
            for entry in self._read_logs()
        ))

    def test_failed_lock_restoration_reports_both_failures(self) -> None:
        write_bytes = Path.write_bytes

        def fail_lock_restore(path, data):
            if (path.name == "vendor-lock.toml"
                and data.startswith(b"# preserve exact original bytes")
                and b'tag = "v2.0.0"' in path.read_bytes()):
                raise OSError("restore denied")
            return write_bytes(path, data)

        with mock.patch.object(Path, "write_bytes", fail_lock_restore):
            result = self._run_no_regression_update(
                completion_policy="strict", gate_sequence=[{"exit_code": 23}]
            )
        self.assertEqual(result["exit_code"], 1)
        self.assertIn("23", result["stderr"])
        self.assertIn("restoring the original vendor lock also failed: restore denied", result["stderr"])
        self.assertTrue((result["crate"] / "vendor/demo-v1.0.0.zip").exists())
        self.assertTrue((result["crate"] / "vendor/demo-v2.0.0.zip").exists())

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["PATH"] = os.pathsep.join([str(self.bin_dir), env.get("PATH", "")])
        env["FAKE_TOOL_LOG"] = str(self.log_path)
        env["FAKE_TOOL_CONFIG"] = str(self.config_path)
        return env

    def _run_no_regression_update(
        self,
        *,
        gate_sequence: list[dict[str, object]],
        completion_policy: str = "no-regression",
        preflight_exit_code: int = 0,
        git_config: dict[str, object] | None = None,
        codex_config: dict[str, object] | None = None,
        external_focus_paths: tuple[str, ...] = (),
    ) -> dict[str, object]:
        crate = self._write_crate(
            vendors_toml="""
            version = 1

            [vendors.demo]
            repo = "acme/demo"
            source_kind = "github-release-source"
            channel = "stable"
            archive_prefix = "demo"
            upstream_focus_paths = ["upstream/api"]
            local_focus_paths = ["demo-sdk/src"]
            plan_prompt_suffix = "Review for protocol drift."
            preflight_gates = [["preflight-gate"]]
            completion_gates = [["gate-sequence"]]
            """.replace('local_focus_paths = ["demo-sdk/src"]',
                        "local_focus_paths = " + json.dumps(["demo-sdk/src", *external_focus_paths]))
        )
        lock_path = crate / "vendor/vendor-lock.toml"
        original_lock_bytes = b"# preserve exact original bytes\r\n" + lock_path.read_bytes().replace(b"\n", b"\r\n")
        lock_path.write_bytes(original_lock_bytes)
        old_archive = crate / "vendor" / "demo-v1.0.0.zip"
        self._write_zip(old_archive, "repo-sha/README.md", "old")
        new_archive_fixture = self.workspace / "download-source.zip"
        self._write_zip(new_archive_fixture, "owner-repo-deadbeef/src/api.txt", "new-api")
        self._write_tool_config(
            {
                "gh": {
                    "latest_release_by_repo": {
                        "acme/demo": {
                            "tag_name": "v2.0.0",
                            "published_at": "2026-03-01T10:00:00Z",
                            "prerelease": False,
                        }
                    },
                    "release_by_repo_and_tag": {
                        "acme/demo": {
                            "v2.0.0": {
                                "tag_name": "v2.0.0",
                                "published_at": "2026-03-01T10:00:00Z",
                                "prerelease": False,
                                "body": "Breaking change",
                                "html_url": "https://example.test/releases/v2.0.0",
                                "zipball_url": "https://example.test/releases/v2.0.0/source.zip",
                            }
                        }
                    },
                    "download_archive": str(new_archive_fixture),
                    "downloaded_name": "downloaded-source.zip",
                },
                "git": {
                    "diff_output": " src/api.txt | 1 +\n 1 file changed, 1 insertion(+)\n",
                    "diff_exit_code": 1,
                    "branch_output": "feature/vendor-campaign\n",
                    **(git_config or {}),
                },
                "codex": codex_config or {},
                "preflight_exit_code": preflight_exit_code,
                "gate_sequence": gate_sequence,
            }
        )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = self.module.main(
                [
                    "update",
                    "--crate",
                    "demo-sdk",
                    "--vendor",
                    "demo",
                    "--git-mode",
                    "current-branch",
                    "--completion-policy",
                    completion_policy,
                ],
                workspace_root=self.workspace,
                env=self._env(),
            )

        dossier = (
            self.workspace
            / "target"
            / "vendor-updates"
            / "demo-sdk"
            / "demo"
            / "v2.0.0"
        )
        return {
            "original_lock_bytes": original_lock_bytes,
            "crate": crate,
            "dossier": dossier,
            "exit_code": exit_code,
            "stdout": stdout.getvalue(),
            "stderr": stderr.getvalue(),
        }

    def _write_crate(
        self,
        *,
        vendors_toml: str | None = None,
        vendor_lock_toml: str | None = None,
    ) -> Path:
        crate = self.workspace / "demo-sdk"
        vendor_dir = crate / "vendor"
        vendor_dir.mkdir(parents=True)
        (self.workspace / ".gitignore").write_text("**/vendor/*/\n")
        (crate / "src").mkdir()
        (crate / "README.md").write_text("demo crate\n")
        (crate / "vendor" / "vendors.toml").write_text(
            textwrap.dedent(
                vendors_toml
                or """
                version = 1

                [vendors.demo]
                repo = "acme/demo"
                source_kind = "github-release-source"
                channel = "stable"
                archive_prefix = "demo"
                upstream_focus_paths = ["upstream/api"]
                local_focus_paths = ["demo-sdk/src"]
                plan_prompt_suffix = "Review for protocol drift."
                completion_gates = [["gate-ok"]]
                """
            ).strip()
            + "\n"
        )
        (crate / "vendor" / "vendor-lock.toml").write_text(
            textwrap.dedent(
                vendor_lock_toml
                or """
                version = 1

                [vendors.demo]
                tag = "v1.0.0"
                archive = "demo-v1.0.0.zip"
                extracted_dir = "demo-v1.0.0"
                published_at = "2026-01-01T00:00:00Z"
                sha256 = "old-demo"
                """
            ).strip()
            + "\n"
        )
        return crate

    def _write_tool_config(self, config: dict) -> None:
        self.config_path.write_text(json.dumps(config))

    def _read_logs(self) -> list[dict]:
        if not self.log_path.exists():
            return []
        return [json.loads(line) for line in self.log_path.read_text().splitlines() if line]

    def _write_zip(self, path: Path, inner_path: str, contents: str) -> None:
        self._write_zip_entries(path, {inner_path: contents})

    def _write_zip_entries(self, path: Path, entries: dict[str, str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(path, "w") as archive:
            for inner_path, contents in entries.items():
                archive.writestr(inner_path, contents)

    def _write_fake_tools(self) -> None:
        self._write_executable(
            "gh",
            r"""#!/usr/bin/env python3
import json
import os
import shutil
import sys
import tomllib
from pathlib import Path

log_path = Path(os.environ["FAKE_TOOL_LOG"])
config = json.loads(Path(os.environ["FAKE_TOOL_CONFIG"]).read_text())
argv = sys.argv[1:]
with log_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "gh", "argv": argv}) + "\n")

def repo_arg(arguments):
    if "-R" in arguments:
        return arguments[arguments.index("-R") + 1]
    return None

if argv[:1] == ["api"]:
    endpoint = argv[1]
    if endpoint.endswith("/releases/latest"):
        repo = endpoint.removeprefix("repos/").removesuffix("/releases/latest")
        payload = config.get("gh", {}).get("latest_release_by_repo", {}).get(repo, {})
        print(json.dumps(payload))
        sys.exit(0)
    if "/releases/tags/" in endpoint:
        remainder = endpoint.removeprefix("repos/")
        repo, tag = remainder.split("/releases/tags/", 1)
        payload = config.get("gh", {}).get("release_by_repo_and_tag", {}).get(repo, {}).get(tag, {})
        print(json.dumps(payload))
        sys.exit(0)
    if endpoint.startswith("repos/") and "/releases?per_page=" in endpoint:
        repo = endpoint.removeprefix("repos/").split("/releases?per_page=", 1)[0]
        payload = config.get("gh", {}).get("release_list_by_repo", {}).get(repo, [])
        print(json.dumps(payload))
        sys.exit(0)
    print(f"unsupported gh api endpoint: {endpoint}", file=sys.stderr)
    sys.exit(2)

if argv[:2] == ["release", "download"]:
    target_dir = Path(argv[argv.index("--dir") + 1])
    target_dir.mkdir(parents=True, exist_ok=True)
    source = Path(config["gh"]["download_archive"])
    name = config["gh"].get("downloaded_name", source.name)
    shutil.copyfile(source, target_dir / name)
    sys.exit(0)

print(f"unsupported gh args: {argv}", file=sys.stderr)
sys.exit(2)
""",
        )
        self._write_executable(
            "git",
            r"""#!/usr/bin/env python3
import json
import os
import sys
import tomllib
from pathlib import Path

log_path = Path(os.environ["FAKE_TOOL_LOG"])
config = json.loads(Path(os.environ["FAKE_TOOL_CONFIG"]).read_text()).get("git", {})
argv = sys.argv[1:]
with log_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "git", "argv": argv}) + "\n")

if argv == ["rev-parse", "HEAD"]:
    print(config.get("head_output", "a" * 40))
    sys.exit(0)

if argv[:3] == ["rev-list", "--parents", "--max-count=1"]:
    print(argv[3] + " " + config.get("prerequisite_parents", ""))
    sys.exit(0)

if argv[:2] == ["diff-tree", "--no-commit-id"]:
    for path in config.get("prerequisite_paths", "").split("\0"):
        if path:
            print(":" + config.get("prerequisite_old_mode", "100644") + " "
                  + config.get("prerequisite_new_mode", "100644") + " "
                  + "a" * 40 + " " + "b" * 40 + " M\0" + path + "\0", end="")
    sys.exit(0)

if argv[:2] == ["check-ignore", "--quiet"]:
    sys.exit(config.get("check_ignore_exit", 0))

if argv[:2] == ["status", "--porcelain"] and "-z" in argv:
    print(config.get("status_nul", ""), end="")
    sys.exit(0)

if argv[:2] == ["status", "--porcelain"] or argv[:3] == ["status", "--porcelain", "--untracked-files=no"]:
    print(config.get("status_output", ""), end="")
    sys.exit(0)

if argv == ["branch", "--show-current"]:
    print(config.get("branch_output", "demo-branch"), end="")
    sys.exit(0)

if argv[:2] == ["switch", "-c"]:
    root_config = json.loads(Path(os.environ["FAKE_TOOL_CONFIG"]).read_text())
    root_config.setdefault("git", {})["branch_output"] = argv[2]
    Path(os.environ["FAKE_TOOL_CONFIG"]).write_text(json.dumps(root_config))
    sys.exit(0)

if argv[:2] == ["diff", "--no-index"]:
    print(config.get("diff_output", ""), end="")
    sys.exit(config.get("diff_exit_code", 1))

if argv == ["write-tree"]:
    print("d" * 40)
    sys.exit(0)

if argv == ["show", "-s", "--format=%P%x00%T%x00%s", "HEAD"]:
    print(config["commit_parent"] + "\0" + "d" * 40 + "\0" + config["commit_message"])
    sys.exit(0)

if argv and argv[0] == "commit":
    root_config = json.loads(Path(os.environ["FAKE_TOOL_CONFIG"]).read_text())
    git = root_config.setdefault("git", {})
    git["commit_parent"] = git.get("head_output", "a" * 40)
    git["head_output"] = "c" * 40
    git["commit_message"] = argv[2]
    Path(os.environ["FAKE_TOOL_CONFIG"]).write_text(json.dumps(root_config))
    sys.exit(0)

if argv and argv[0] in {"add", "push"}:
    sys.exit(0)

print(f"unsupported git args: {argv}", file=sys.stderr)
sys.exit(2)
""",
        )
        self._write_executable(
            "codex",
            r"""#!/usr/bin/env python3
import json
import os
import sys
import time
import subprocess
import tomllib
from pathlib import Path

log_path = Path(os.environ["FAKE_TOOL_LOG"])
config = json.loads(Path(os.environ["FAKE_TOOL_CONFIG"]).read_text()).get("codex", {})
argv = sys.argv[1:]
with log_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "codex", "argv": argv, "lock_tag": tomllib.loads(Path("demo-sdk/vendor/vendor-lock.toml").read_text())["vendors"]["demo"]["tag"]}) + "\n")

output_path = None
if "--output-last-message" in argv:
    output_path = Path(argv[argv.index("--output-last-message") + 1])
elif "-o" in argv:
    output_path = Path(argv[argv.index("-o") + 1])

if output_path is not None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sandbox = argv[argv.index("--sandbox") + 1] if "--sandbox" in argv else "unknown"
    if sandbox == "workspace-write" and config.get("child_marker"):
        marker = config["child_marker"]
        subprocess.Popen([sys.executable, "-c",
                          "from pathlib import Path; import time; "
                          + f"p = Path({marker!r}); p.with_suffix('.started').write_text('ready'); "
                          + "time.sleep(0.8); p.write_text('late write')"])
    sleep_seconds = config.get("sleep_seconds_by_sandbox", {}).get(sandbox, config.get("sleep_seconds", 0))
    if sleep_seconds:
        time.sleep(sleep_seconds)
    output_path.write_text(f"# {sandbox}\n")

sys.exit(0)
""",
        )
        self._write_executable(
            "gate-ok",
            r"""#!/usr/bin/env python3
import json
import os
import sys
import tomllib
from pathlib import Path

log_path = Path(os.environ["FAKE_TOOL_LOG"])
with log_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "gate-ok", "argv": sys.argv[1:]}) + "\n")

for env_name in ["EXPECTED_OLD_ARCHIVE", "EXPECTED_OLD_EXTRACT"]:
    expected = os.environ.get(env_name)
    if expected and not Path(expected).exists():
        print(f"missing expected path during gate: {expected}", file=sys.stderr)
        sys.exit(9)

sys.exit(0)
""",
        )
        self._write_executable(
            "gate-fail",
            r"""#!/usr/bin/env python3
import json
import os
import sys
import tomllib
from pathlib import Path

log_path = Path(os.environ["FAKE_TOOL_LOG"])
with log_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "gate-fail", "argv": sys.argv[1:]}) + "\n")

sys.exit(7)
""",
        )
        self._write_executable(
            "preflight-gate",
            r"""#!/usr/bin/env python3
import json
import os
import sys
import tomllib
from pathlib import Path

log_path = Path(os.environ["FAKE_TOOL_LOG"])
config = json.loads(Path(os.environ["FAKE_TOOL_CONFIG"]).read_text())
with log_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "preflight-gate", "argv": sys.argv[1:], "lock_tag": tomllib.loads(Path("demo-sdk/vendor/vendor-lock.toml").read_text())["vendors"]["demo"]["tag"]}) + "\n")

exit_code = config.get("preflight_exit_code", 0)
if exit_code:
    print("missing required live prerequisite", file=sys.stderr)
sys.exit(exit_code)
""",
        )
        self._write_executable(
            "gate-sequence",
            r"""#!/usr/bin/env python3
import json
import os
import sys
import tomllib
from pathlib import Path

log_path = Path(os.environ["FAKE_TOOL_LOG"])
config = json.loads(Path(os.environ["FAKE_TOOL_CONFIG"]).read_text())
prior_calls = 0
if log_path.exists():
    for line in log_path.read_text().splitlines():
        if line and json.loads(line).get("tool") == "gate-sequence":
            prior_calls += 1

with log_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "gate-sequence", "argv": sys.argv[1:], "lock_tag": tomllib.loads(Path("demo-sdk/vendor/vendor-lock.toml").read_text())["vendors"]["demo"]["tag"]}) + "\n")

sequence = config.get("gate_sequence", [])
if not sequence:
    print("missing gate sequence", file=sys.stderr)
    sys.exit(90)
run = sequence[min(prior_calls, len(sequence) - 1)]
if run.get("stdout"):
    print(run["stdout"], end="")
if run.get("stderr"):
    print(run["stderr"], end="", file=sys.stderr)
sys.exit(run.get("exit_code", 0))
""",
        )

    def _write_executable(self, name: str, contents: str) -> None:
        path = self.bin_dir / name
        path.write_text(contents)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)


if __name__ == "__main__":
    unittest.main()

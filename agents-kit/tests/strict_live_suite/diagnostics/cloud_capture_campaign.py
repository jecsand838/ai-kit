#!/usr/bin/env python3
"""Run one exact cloud case in an immutable, at-most-three-invocation campaign.

This test-only runner never retries, changes model inputs, runs other tests, or
claims qualification. Commit reviewed diagnostics first. Source environment
values and pending response bodies remain private; stdout contains status only.
"""

import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid


class CampaignPolicy:
    CASE = "strict_live_suite::prompt_and_io::opencode_cloud_generation_and_streaming_are_live"
    COMMAND = ["cargo", "test", "--locked", "-p", "agents-kit", "--test",
               "strict_live", "--all-features", CASE, "--", "--exact",
               "--nocapture", "--test-threads=1"]
    LIMIT = 3

    @staticmethod
    def can_continue(report):
        return (report.get("case_outcome") == "passed"
                and report.get("capture_complete") is True
                and report.get("first_divergence") is None
                and all(report.get(key) is True for key in (
                    "cleanup_complete", "provenance_complete", "child_reaped",
                    "process_group_settled"))
                and report.get("primary_error") is None
                and report.get("cleanup_errors") == []
                and report.get("evidence_errors") == [])

    @staticmethod
    def failure(report, phase, error):
        # Never replace the case/launch error with a later cleanup failure.
        failure = {"phase": phase, "type": type(error).__name__, "message": str(error)}
        if phase in ("cleanup", "evidence"):
            report[f"{phase}_errors"].append(failure)
        elif report.get("primary_error") is None:
            report["primary_error"] = failure
        else:
            report["evidence_errors"].append(failure)

    @staticmethod
    def digest(path):
        value = hashlib.sha256()
        before = path.stat()
        if path.is_symlink() or not stat.S_ISREG(before.st_mode):
            raise RuntimeError("Evidence must be a regular, non-symlink file")
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1 << 20), b""):
                value.update(block)
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise RuntimeError("Evidence changed while recording its hash")
        return {"sha256": value.hexdigest(), "bytes": after.st_size}

    @classmethod
    def snapshot(cls, paths):
        result = {}
        for path in paths:
            path = Path(path).absolute()
            if path.is_symlink() or not path.exists():
                raise RuntimeError("Evidence path is missing or a symlink")
            if path.is_dir():
                files = sorted(path.rglob("*"))
                if any(item.is_symlink() for item in files):
                    raise RuntimeError("Evidence inventory contains a symlink")
                files = [item for item in files if item.is_file()]
            else:
                files = [path]
            for item in files:
                result[str(item)] = cls.digest(item)
        return result

    @classmethod
    def verify_snapshot(cls, expected, roots):
        if cls.snapshot(roots) != expected:
            raise RuntimeError("Previously retained evidence changed")

    @staticmethod
    def write_new(path, value):
        content = value if isinstance(value, bytes) else (json.dumps(value, indent=2) + "\n").encode()
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())


class CampaignLedger:
    def __init__(self, root, identity, originals):
        self.root = root
        self.identity = identity
        self.originals = [Path(path).absolute() for path in originals]
        self.attempts = root / "attempts"

    def prepare(self):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.attempts.mkdir(mode=0o700, exist_ok=True)
        manifest_path = self.root / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_bytes())
            if (manifest.get("identity") != self.identity or manifest.get("limit") != CampaignPolicy.LIMIT
                    or manifest.get("original_paths") != [str(path) for path in self.originals]):
                raise RuntimeError("Campaign identity differs from its frozen manifest")
        else:
            if list(self.attempts.iterdir()):
                raise RuntimeError("Attempt inventory lacks its original manifest")
            if not self.originals:
                raise RuntimeError("Original evidence is required before starting a new campaign")
            manifest = {"version": 1, "identity": self.identity, "limit": CampaignPolicy.LIMIT,
                        "case": CampaignPolicy.CASE, "command": CampaignPolicy.COMMAND,
                        "original_paths": [str(path) for path in self.originals],
                        "original_evidence": CampaignPolicy.snapshot(self.originals)}
            CampaignPolicy.write_new(manifest_path, manifest)
        self.validate()

    def validate(self):
        manifest = json.loads((self.root / "manifest.json").read_bytes())
        if (manifest.get("identity") != self.identity or manifest.get("case") != CampaignPolicy.CASE
                or manifest.get("command") != CampaignPolicy.COMMAND
                or manifest.get("limit") != CampaignPolicy.LIMIT):
            raise RuntimeError("Campaign manifest identity changed")
        CampaignPolicy.verify_snapshot(manifest["original_evidence"], self.originals)
        previous = sorted(self.attempts.iterdir())
        if [path.name for path in previous] != [f"attempt-{number:02d}" for number in range(1, len(previous) + 1)]:
            raise RuntimeError("Attempt inventory must be contiguous")
        for number, directory in enumerate(previous, 1):
            receipt_path = directory / "receipt.json"
            seal_path = directory / "seal.json"
            if not receipt_path.is_file() or not seal_path.is_file():
                raise RuntimeError("A reserved attempt is unsettled; the slot cannot be reused")
            seal = json.loads(seal_path.read_bytes())
            files = [item for item in directory.iterdir() if item.name != "seal.json"]
            CampaignPolicy.verify_snapshot(seal["attempt_evidence"], files)
            CampaignPolicy.verify_snapshot(seal["retained_evidence"], [Path(path) for path in seal["retained_roots"]])
            report = json.loads(receipt_path.read_bytes())
            if (report.get("attempt") != number or report.get("case") != CampaignPolicy.CASE
                    or report.get("state") != "settled"):
                raise RuntimeError("Reserved attempt identity is invalid")
            if not CampaignPolicy.can_continue(report):
                raise RuntimeError("Previous attempt requires a campaign stop")
        if len(previous) >= CampaignPolicy.LIMIT:
            raise RuntimeError("The three-invocation campaign budget is exhausted")
        return len(previous) + 1

    def reserve(self):
        number = self.validate()
        previous = list(self.attempts.iterdir())
        directory = self.attempts / f"attempt-{number:02d}"
        directory.mkdir(mode=0o700)
        reservation = {"attempt": number, "campaign_attempt_id": uuid.uuid4().hex,
                       "case": CampaignPolicy.CASE, "command": CampaignPolicy.COMMAND,
                       "source_identity": self.identity,
                       "previous_attempt_roots": [str(path) for path in previous],
                       "previous_attempt_evidence": CampaignPolicy.snapshot(previous),
                       "reserved_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        CampaignPolicy.write_new(directory / "reservation.json", reservation)
        return directory, reservation

    def finish(self, directory, reservation, report, retained_roots):
        report = dict(report, **reservation, state="settled")
        report["continue_allowed"] = CampaignPolicy.can_continue(report) and report["attempt"] < CampaignPolicy.LIMIT
        CampaignPolicy.write_new(directory / "receipt.json", report)
        CampaignPolicy.write_new(directory / "seal.json", {
            "attempt_evidence": CampaignPolicy.snapshot(list(directory.iterdir())),
            "retained_roots": [str(path) for path in retained_roots],
            "retained_evidence": CampaignPolicy.snapshot(retained_roots),
        })


class CaseEvidence:
    """Validate private observations without changing the portable case contract."""

    def __init__(self, root, attempt_id):
        self.root = root
        self.attempt_id = attempt_id
        self.directory = None
        self.pending = []

    @staticmethod
    def read_json(path, bound):
        if path.is_symlink() or path.stat().st_size > bound:
            raise RuntimeError("Evidence JSON is symlinked or exceeds its bound")
        return json.loads(path.read_bytes())

    @staticmethod
    def text_hash(text):
        return hashlib.sha256(text.encode()).hexdigest()

    @staticmethod
    def require(condition, reason):
        if not condition:
            raise RuntimeError(reason)

    @classmethod
    def require_hash(cls, value):
        cls.require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
                    "Fingerprint is missing or malformed")

    @classmethod
    def require_boolean(cls, value):
        cls.require(isinstance(value, bool), "Comparison fact is missing or malformed")

    def inspect(self, log, report):
        markers = re.findall(r"^AGENTS_KIT_CLOUD_OUTPUT_DIAGNOSTIC=(.+)$", log, re.M)
        self.require(len(markers) == 1, "Exactly one case diagnostic marker is required")
        marker = json.loads(markers[0])
        directory = Path(marker["evidence_directory"])
        parent = (self.root / "target/cloud-output-evidence").resolve()
        self.require(directory.is_absolute() and not directory.is_symlink()
                     and directory.resolve().parent == parent, "Unexpected case evidence directory")
        self.directory = directory
        summary = self.read_json(directory / "summary.json", 256 * 1024)
        self.require(summary.get("version") == 3 and summary.get("case") == CampaignPolicy.CASE
                     and summary.get("campaign_attempt_id") == self.attempt_id,
                     "Case summary is stale or has unexpected identity")
        report["case_summary_passed"] = summary.get("case_passed") is True
        report["cleanup_complete"] = summary.get("cleanup_passed") is True
        self.require(summary.get("diagnostic_errors") == [], "Case reported diagnostic errors")
        self.require(summary.get("case_passed") == (report["case_outcome"] == "passed"),
                     "Cargo and case summary outcomes disagree")
        text = self.read_json(directory / "private-pending/case-text.json", 256 * 1024)
        self.require(text.get("version") == 1 and text.get("campaign_attempt_id") == self.attempt_id,
                     "Private case text is stale or invalid")
        self.require(self.text_hash(text["model"]["modelID"]) == report["configured_model_sha256"],
                     "Case model differs from the sourced configured model")
        foreground = self.read_json(directory / "foreground/manifest.json", 64 * 1024)
        self.require(foreground.get("version") == 1 and foreground.get("capture_complete") is True
                     and foreground.get("transport_settled") is True and foreground.get("reasons") == [],
                     "Foreground capture is incomplete")
        native = {}
        for phase in ("unary", "stream"):
            rows = [row for row in foreground["phases"] if row.get("phase") == phase]
            self.require(len(rows) == 1, "Foreground phase is missing or ambiguous")
            native[phase] = self.foreground_phase(rows[0], text["phases"][phase], text["model"])
        self.provider(summary, text, native, report)
        public = summary["public"]
        self.require(public["unary"].get("capture") == "complete"
                     and public["stream"].get("capture") == "complete",
                     "Public prompt observations are incomplete")
        native_readback = public["native"]
        self.require(native_readback.get("capture") == "complete", "Later native readback is incomplete")
        self.require_boolean(native_readback.get("native_equals_portable"))
        if report["case_outcome"] == "passed":
            stream = public["stream"]
            self.require(isinstance(stream.get("deltas"), int) and stream["deltas"] > 0
                         and stream.get("invalid_delta") is False and stream.get("stream_fused") is True
                         and 0 < len(text["phases"]["stream"]["public_deltas"].encode()) <= 64 * 1024,
                         "Passing case lacks its unchanged streaming assertions")
        # Complete, correctly correlated captures may still show mismatched text.
        comparisons = report["comparisons"]
        for phase in ("unary", "stream"):
            observation = text["phases"][phase]
            comparisons[f"{phase}_foreground_to_public"] = native[phase]["text"] == observation["public_terminal"]
            comparisons[f"{phase}_public_to_expected"] = observation["public_terminal"] == observation["expected"]
        comparisons["stream_delta_to_public"] = text["phases"]["stream"]["public_deltas"] == text["phases"]["stream"]["public_terminal"]
        comparisons["stream_readback_to_public"] = native_readback.get("native_equals_portable") is True
        order = (
            "provider_delta_to_done", "provider_parts_delta_equal_done", "provider_part_delta_to_done",
            "provider_parts_done_equal_content", "provider_part_done_to_content",
            "provider_parts_content_equal_item", "provider_part_content_to_item",
            "provider_parts_item_equal_final", "provider_part_item_to_final", "provider_done_to_final",
            "provider_delta_to_native_hook", "native_hook_to_foreground", "native_hook_parts_to_foreground",
            "unary_foreground_to_public", "stream_foreground_to_public", "stream_delta_to_public",
            "stream_readback_to_public", "provider_final_to_expected", "unary_public_to_expected",
            "stream_public_to_expected",
        )
        comparisons = {name: comparisons[name] for name in order}
        report["comparisons"] = comparisons
        report["capture_complete"] = True
        report["first_divergence"] = next((name for name, equal in comparisons.items() if equal is not True), None)
        report["informative_failure"] = report["first_divergence"] is not None
        report["finding_kind"] = ("none" if report["first_divergence"] is None
                                  else "exact_copy_failure" if report["first_divergence"].endswith("_to_expected")
                                  else "observed_boundary_disagreement")
        self.pending = [directory / "private-pending", directory / "foreground"]

    def foreground_phase(self, row, observation, model):
        self.require(all(row.get(key) is True for key in (
            "complete", "request_forwarded", "response_forwarded", "parent_forwarded",
            "correlation_complete", "response_framing_complete", "parent_framing_complete"))
            and 200 <= row.get("response_status", 0) < 300,
            "Foreground phase lacks complete forwarded/correlated bodies")
        bodies = {}
        for kind, bound in (("request", 2 * 1024 * 1024), ("response", 16 * 1024 * 1024), ("parent", 16 * 1024 * 1024)):
            filename = row.get(f"{kind}_file")
            self.require(isinstance(filename, str) and Path(filename).name == filename,
                         "Foreground body filename is invalid")
            path = self.directory / "foreground" / filename
            self.require(path.stat().st_size == row[f"{kind}_body_bytes"], "Foreground byte count differs")
            bodies[kind] = self.read_json(path, bound)
        request, response, parent = (bodies[key] for key in ("request", "response", "parent"))
        info = response["info"]
        parent_info = parent["info"]
        self.require(info.get("role") == "assistant"
                     and isinstance(info.get("time", {}).get("completed"), (int, float))
                     and "error" not in info and parent_info.get("role") == "user"
                     and info.get("parentID") == parent_info.get("id")
                     and info.get("sessionID") == parent_info.get("sessionID")
                     and info.get("providerID") == model["providerID"]
                     and info.get("modelID") == model["modelID"]
                     and request.get("model") == model and parent_info.get("model") == model,
                     "Foreground owner, completion, or model correlation differs")
        request_text = [part.get("text") for part in request["parts"] if part.get("type") == "text"]
        parent_text = [part.get("text") for part in parent["parts"] if part.get("type") == "text" and not part.get("synthetic", False)]
        self.require(request_text == [observation["prompt"]] and parent_text == request_text,
                     "Foreground request and parent do not contain the exact prompt")
        parts = response["parts"]
        self.require(all(part.get("sessionID") == info["sessionID"] and part.get("messageID") == info["id"] for part in parts),
                     "Foreground part ownership differs")
        parts = sorted((part for part in parts if part.get("type") == "text" and not part.get("synthetic", False)), key=lambda part: part["id"])
        self.require(parts and len({part["id"] for part in parts}) == len(parts)
                     and all(isinstance(part.get("text"), str) for part in parts), "Foreground text parts are invalid")
        return {"text": "".join(part["text"] for part in parts), "parts": parts, "info": info}

    def provider(self, summary, text, native, report):
        rows = summary["provider"]["receipts"]
        comparisons = [row for row in rows if row.get("event") == "fresh_comparison"]
        self.require(comparisons, "No correlated provider/native comparison was captured")
        comparison = comparisons[-1]
        self.require(comparison.get("capture") == "complete"
                     and comparison.get("receipt_persistence_complete") is True
                     and comparison.get("matching_provider_flows") == 1
                     and comparison.get("matching_assistants") == 1
                     and all(comparison.get(key) is True for key in (
                         "provider_delta_complete", "provider_done_complete", "provider_final_complete"))
                     and comparison.get("provider_reported_model_matches_requested") is True,
                     "Provider/native capture or configured model attribution is incomplete")
        roots = [row for row in rows if row.get("event") == "fresh_user"]
        requests = [row for row in rows if row.get("event") == "fresh_request"]
        outputs = [row for row in rows if row.get("event") == "fresh_provider_text"]
        self.require(len(roots) == len(requests) == len(outputs) == 1
                     and roots[0].get("prompt_matches_expected") is True
                     and all(requests[0].get(key) is True for key in (
                         "model_matches_configured", "session_matches_fresh_user"))
                     and requests[0].get("exact_prompt_parts") == 1
                     and requests[0].get("other_user_text_parts") == 0
                     and outputs[0].get("flow") == requests[0].get("flow"),
                     "Provider request correlation is missing or ambiguous")
        matched_flow = requests[0]["flow"]
        # Unary is intentionally outside the provider fresh-stream observer.
        # Its protocol end can be incomplete without invalidating this flow.
        self.require(not any(row.get("event") in ("fresh_incomplete", "observer_incomplete", "anomaly", "unavailable")
                             or (row.get("capture") == "incomplete" and (
                                 row.get("flow") == matched_flow or row.get("event", "").startswith("fresh_")))
                             for row in rows), "Provider observer reported incomplete matched capture")
        parts = [row for row in rows if row.get("event") == "fresh_provider_part"]
        self.require(len(parts) == comparison.get("provider_text_parts") and bool(parts)
                     and all(part.get("flow") == requests[0]["flow"] for part in parts)
                     and len({(part["output_index"], part["content_index"]) for part in parts}) == len(parts),
                     "Provider per-part capture is missing or ambiguous")
        capture_id = comparison.get("capture_id")
        self.require(isinstance(capture_id, str) and 0 < len(capture_id) <= 128
                     and all(row.get("capture_id") == capture_id for row in roots + requests + outputs + comparisons + parts),
                     "Provider receipt capture identity is missing or inconsistent")
        manifests = sorted((self.directory / "private-pending").glob("*.manifest.json"))
        self.require(len(manifests) == 1, "Provider raw capture manifest is missing or ambiguous")
        for path in manifests:
            manifest = self.read_json(path, 32 * 1024)
            self.require(manifest.get("capture_id") == capture_id, "Provider body capture identity differs from receipts")
            self.require(manifest.get("version") == 1 and manifest.get("protocol") == "openai_responses"
                         and manifest.get("flow") == requests[0]["flow"]
                         and all(manifest.get(key) is True for key in ("eof", "body_capture_complete", "provider_correlation_complete", "receipt_persistence_complete"))
                         and manifest.get("response_bound_exceeded") is False
                         and 200 <= manifest.get("status", 0) < 300,
                         "Provider raw body capture is incomplete")
            for kind, bound in (("request", 2 * 1024 * 1024), ("response", 8 * 1024 * 1024)):
                name = manifest[f"{kind}_file"]
                self.require(Path(name).name == name, "Provider capture filename is invalid")
                body = path.parent / name
                self.require(not body.is_symlink() and body.stat().st_size == manifest[f"{kind}_bytes"]
                             and body.stat().st_size <= bound, "Provider body length or bound differs")
        stream = native["stream"]
        for key in ("native_session_sha256", "native_user_sha256", "native_assistant_sha256",
                    "native_text_sha256", "provider_delta_sha256", "provider_done_sha256", "provider_final_sha256"):
            self.require_hash(comparison.get(key))
        self.require(isinstance(comparison.get("native_parts"), list) and bool(comparison["native_parts"]),
                     "Native hook part facts are missing")
        for part in comparison["native_parts"]:
            self.require_hash(part.get("part_sha256"))
            self.require_hash(part.get("text_sha256"))
            self.require(isinstance(part.get("text_bytes"), int) and part["text_bytes"] >= 0,
                         "Native hook part byte count is missing or malformed")
        self.require(all(comparison[f"native_{owner}_sha256"] == self.text_hash(stream["info"][field]) for owner, field in (
            ("session", "sessionID"), ("user", "parentID"), ("assistant", "id"))), "Provider hook and foreground native identities differ")
        expected_parts = [{"part_sha256": self.text_hash(part["id"]), "text_sha256": self.text_hash(part["text"]),
                           "text_bytes": len(part["text"].encode())} for part in stream["parts"]]
        report["comparisons"] = {
            "provider_delta_to_done": comparison["provider_delta_sha256"] == comparison["provider_done_sha256"],
            "provider_done_to_final": comparison["provider_done_sha256"] == comparison["provider_final_sha256"],
            "provider_delta_to_native_hook": comparison["provider_delta_sha256"] == comparison["native_text_sha256"],
            "native_hook_to_foreground": comparison["native_text_sha256"] == self.text_hash(stream["text"]),
            "native_hook_parts_to_foreground": sorted(comparison["native_parts"], key=lambda part: part["part_sha256"]) == sorted(expected_parts, key=lambda part: part["part_sha256"]),
            "provider_final_to_expected": comparison["provider_final_sha256"] == self.text_hash(text["phases"]["stream"]["expected"]),
        }
        for name in ("provider_parts_delta_equal_done", "provider_parts_done_equal_content", "provider_parts_content_equal_item", "provider_parts_item_equal_final"):
            self.require_boolean(comparison.get(name))
            report["comparisons"][name] = comparison.get(name) is True
        for before, after in (("delta", "done"), ("done", "content"), ("content", "item"), ("item", "final")):
            equalities = []
            for part in parts:
                self.require_hash(part.get(f"{before}_sha256"))
                self.require_hash(part.get(f"{after}_sha256"))
                self.require_boolean(part.get(f"{before}_equals_{after}"))
                equal = part[f"{before}_sha256"] == part[f"{after}_sha256"]
                self.require(equal == part[f"{before}_equals_{after}"],
                             "Provider part hashes contradict comparison facts")
                equalities.append(equal)
            self.require(all(equalities) == comparison[f"provider_parts_{before}_equal_{after}"],
                         "Provider part inventory contradicts aggregate part facts")
            report["comparisons"][f"provider_part_{before}_to_{after}"] = all(equalities)

    def remove_successful_pending(self):
        for path in self.pending:
            self.require(path.is_dir() and not path.is_symlink(), "Pending artifact directory is invalid")
            shutil.rmtree(path)


class CaptureCampaign:
    OUTER_SECONDS = 7200
    FLAGS = ("OPENCODE_PURE", "OPENCODE_DISABLE_DEFAULT_PLUGINS", "OPENCODE_EXPERIMENTAL_NATIVE_LLM", "OPENCODE_EXPERIMENTAL_WEBSOCKETS")

    def __init__(self, arguments):
        self.root = Path(__file__).resolve().parents[4]
        self.campaign = Path(arguments.campaign).absolute()
        self.environment_path = Path(arguments.environment).resolve(strict=True)
        self.originals = [Path(path).absolute() for path in arguments.prior_evidence]
        if not self.originals:
            self.originals = [self.root / "target/cloud-output-investigation"]

    def command_output(self, command):
        return subprocess.check_output(command, cwd=self.root, env=self.environment, stderr=subprocess.DEVNULL, timeout=15)

    def source_identity(self):
        status = self.command_output(["git", "status", "--porcelain=v1", "--untracked-files=all"])
        if status:
            raise RuntimeError("Commit reviewed changes first; the campaign requires a clean source checkout")
        return {"revision": self.command_output(["git", "rev-parse", "HEAD"]).decode().strip(),
                "branch": self.command_output(["git", "branch", "--show-current"]).decode().strip(),
                "cargo_lock": CampaignPolicy.digest(self.root / "Cargo.lock"),
                "runner": CampaignPolicy.digest(Path(__file__).resolve())}

    def runtime_identity(self):
        executable = shutil.which("opencode", path=self.environment.get("PATH"))
        if executable is None:
            raise RuntimeError("OpenCode executable is absent from the sourced environment PATH")
        native = Path(executable).resolve(strict=True)
        specifications = (self.root / "agents-kit/specs/SPECIFICATIONS.md").read_text()
        pins = re.findall(r"^- OpenCode Server: `v([^`]+)`$", specifications, re.M)
        version = self.command_output([str(native), "--version"]).decode().strip()
        if len(pins) != 1 or version.removeprefix("v") != pins[0]:
            raise RuntimeError("Native OpenCode version does not match the selected specification pin")
        return {"native_path": executable, "native_realpath": str(native),
                "native_binary": CampaignPolicy.digest(native),
                "native_version": version, "selected_native_version": pins[0],
                "rustc": self.command_output(["rustc", "-vV"]).decode(),
                "cargo": self.command_output(["cargo", "-vV"]).decode(),
                "environment_path": str(self.environment_path),
                "environment_file": CampaignPolicy.digest(self.environment_path),
                "runtime_flags": {name: {"present": name in self.environment,
                    "sha256": hashlib.sha256(self.environment.get(name, "").encode()).hexdigest()} for name in self.FLAGS},
                "configured_model_sha256": hashlib.sha256(self.environment["TEST_OPENCODE_CLOUD_MODEL"].encode()).hexdigest(),
                "additional_native_probes": False}

    def preflight(self):
        source_script = 'set +x; set -a; source "$1" >/dev/null 2>/dev/null || exit $?; exec "$2" -c \'import os,sys; sys.stdout.buffer.write(b"\\0".join((k+"="+v).encode() for k,v in os.environ.items()))\''
        result = subprocess.run(["bash", "--noprofile", "--norc", "-c", source_script,
                                 "cloud-capture", str(self.environment_path), sys.executable],
                                cwd=self.root, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=15)
        if result.returncode:
            raise RuntimeError("Private environment sourcing failed; values were not exported")
        self.environment = dict(item.decode().split("=", 1) for item in result.stdout.split(b"\0") if item)
        for name in ("TEST_OPENCODE_CLOUD_MODEL", "TEST_OPENAI_API_KEY", "TEST_OPENAI_BASE_URL"):
            value = self.environment.get(name)
            if not value or value.strip() != value:
                raise RuntimeError(f"Required private environment variable is missing/invalid: {name}")
        self.environment.update(CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1")
        target = Path(self.environment.get("CARGO_TARGET_DIR", self.root / "target"))
        self.target = (self.root / target).resolve() if not target.is_absolute() else target.resolve()
        self.environment["CARGO_TARGET_DIR"] = str(self.target)
        if self.campaign.is_symlink() or any(self.campaign == path or self.campaign.is_relative_to(path) for path in self.originals):
            raise RuntimeError("New campaign must be separate from all original evidence")
        self.require_serial_lane()
        source = self.source_identity()
        runtime = self.runtime_identity()
        return {"source": source, "runtime": runtime, "cwd": str(self.root),
                "cargo_environment": {key: self.environment[key] for key in ("CARGO_INCREMENTAL", "CARGO_BUILD_JOBS", "CARGO_TARGET_DIR")}}

    @staticmethod
    def processes():
        output = subprocess.check_output(["ps", "-axo", "pid=,pgid=,command="], text=True, timeout=5)
        rows = []
        for line in output.splitlines():
            fields = line.split(None, 2)
            if len(fields) != 3:
                raise RuntimeError("Process inventory is incomplete")
            rows.append((int(fields[0]), int(fields[1]), fields[2]))
        return rows

    def require_serial_lane(self):
        for pid, _group, command in self.processes():
            if pid == os.getpid():
                continue
            words = command.split()
            program = Path(words[0]).name
            script = Path(words[1]).name if program.lower().startswith("python") and len(words) > 1 else ""
            if (program in ("cargo", "rustc", "rustdoc", "cargo-clippy")
                    or re.fullmatch(r"strict_live-[0-9a-f]+", program)
                    or script in ("run_completion_gates.py", "run_case.py", "cloud_capture_campaign.py")):
                raise RuntimeError(f"Serial Cargo lane is occupied by process {pid}")

    def group_members(self, group):
        return [pid for pid, owner, _command in self.processes() if owner == group]

    def settle(self, child, report):
        try:
            for chosen in (signal.SIGTERM, signal.SIGKILL):
                child.poll()
                if not self.group_members(child.pid):
                    break
                try:
                    os.killpg(child.pid, chosen)
                except ProcessLookupError:
                    pass
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    child.poll()
                    if not self.group_members(child.pid):
                        break
                    time.sleep(0.1)
            report["process_group_settled"] = not self.group_members(child.pid)
            if not report["process_group_settled"]:
                raise RuntimeError("Owned process group remains unsettled")
        except BaseException as error:
            CampaignPolicy.failure(report, "cleanup", error)
        finally:
            # This fallback does not depend on ps, and never reuses a naked PID.
            for chosen in (signal.SIGTERM, signal.SIGKILL):
                try:
                    if child.poll() is None:
                        child.send_signal(chosen)
                    report["exit_code"] = child.wait(timeout=10)
                    report["child_reaped"] = True
                    break
                except subprocess.TimeoutExpired:
                    continue
                except BaseException as error:
                    CampaignPolicy.failure(report, "cleanup", error)
            if not report["child_reaped"]:
                CampaignPolicy.failure(report, "cleanup", RuntimeError("Direct child reap is unconfirmed"))

    def inspect_log(self, path, report):
        log = path.read_text(errors="replace")
        started = re.findall(r"^test (\S+) \.\.\.", log, re.M)
        results = re.findall(r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;", log, re.M)
        artifacts = re.findall(r"^\s+Running tests/strict_live\.rs \(([^)\r\n]+)\)$", log, re.M)
        if started != [CampaignPolicy.CASE] or len(results) != 1 or len(artifacts) != 1:
            raise RuntimeError("Cargo did not record exactly the requested case and executable")
        binary = Path(artifacts[0])
        binary = binary if binary.is_absolute() else self.root / binary
        if binary.is_symlink() or binary.resolve().parent != (self.target / "debug/deps").resolve() or not re.fullmatch(r"strict_live-[0-9a-f]+", binary.name):
            raise RuntimeError("Executed Cargo artifact has unexpected ownership")
        report["executed_binary"] = dict(path=str(binary.resolve()), observed_after_execution=True, **CampaignPolicy.digest(binary))
        result = results[0]
        if result[:5] == ("ok", "1", "0", "0", "0") and report.get("exit_code") == 0:
            report["case_outcome"] = "passed"
        elif result[:5] == ("FAILED", "0", "1", "0", "0"):
            report["case_outcome"] = "failed"
        else:
            raise RuntimeError("Exact case inventory or result is invalid")
        report["filtered_test_count"] = int(result[5])
        return log

    def run(self):
        identity = self.preflight()  # No reservation or paid model call on preflight failure.
        self.campaign.mkdir(mode=0o700, parents=True, exist_ok=True)
        with (self.campaign / ".lock").open("a+b") as lock:
            os.chmod(lock.name, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ledger = CampaignLedger(self.campaign, identity, self.originals)
            ledger.prepare()
            directory, reservation = ledger.reserve()
            report = {"case_outcome": "not_started", "capture_complete": False, "first_divergence": None,
                      "cleanup_complete": False, "provenance_complete": False, "child_reaped": False,
                      "process_group_settled": False, "primary_error": None, "cleanup_errors": [],
                      "evidence_errors": [], "outer_wait_seconds": self.OUTER_SECONDS,
                      "scope": "one exact case; no retries, fix phase, or full qualification",
                      "raw_body_disposition": "retained_pending_or_incomplete"}
            report["configured_model_sha256"] = identity["runtime"]["configured_model_sha256"]
            child = None
            evidence = CaseEvidence(self.root, reservation["campaign_attempt_id"])
            log_path = directory / "execution.log"
            began = time.monotonic()
            print(json.dumps({"state": "reserved", "attempt": reservation["attempt"], "directory": str(directory)}), flush=True)
            try:
                CampaignPolicy.write_new(directory / "executed-runner.py", Path(__file__).read_bytes())
                self.environment["AI_KIT_CLOUD_CAPTURE_ATTEMPT_ID"] = reservation["campaign_attempt_id"]
                with os.fdopen(os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
                    child = subprocess.Popen(CampaignPolicy.COMMAND, cwd=self.root, env=self.environment,
                                             stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                                             start_new_session=True)
                    report["child_pid"] = child.pid
                    CampaignPolicy.write_new(directory / "launched.json", {
                        "campaign_attempt_id": reservation["campaign_attempt_id"],
                        "child_pid": child.pid, "owned_process_group": child.pid,
                        "command": CampaignPolicy.COMMAND, "cwd": str(self.root),
                    })
                    report["exit_code"] = child.wait(timeout=self.OUTER_SECONDS)
                    report["child_reaped"] = True
                    if report["exit_code"]:
                        report["primary_error"] = {"phase": "case", "type": "NonzeroExit", "exit_code": report["exit_code"]}
                    if self.group_members(child.pid):
                        CampaignPolicy.failure(report, "cleanup", RuntimeError("Owned descendants survived Cargo exit"))
            except BaseException as error:
                CampaignPolicy.failure(report, "execution", error)
            finally:
                for chosen in (signal.SIGINT, signal.SIGTERM):
                    signal.signal(chosen, signal.SIG_IGN)
                if child is not None:
                    self.settle(child, report)
                try:
                    log = self.inspect_log(log_path, report)
                    evidence.inspect(log, report)
                except BaseException as error:
                    CampaignPolicy.failure(report, "evidence", error)
                try:
                    if self.source_identity() != identity["source"] or self.runtime_identity() != identity["runtime"]:
                        raise RuntimeError("Source or runtime identity changed during invocation")
                    manifest = json.loads((self.campaign / "manifest.json").read_bytes())
                    CampaignPolicy.verify_snapshot(manifest["original_evidence"], self.originals)
                    CampaignPolicy.verify_snapshot(reservation["previous_attempt_evidence"],
                                                   [Path(path) for path in reservation["previous_attempt_roots"]])
                    self.require_serial_lane()
                    report["provenance_complete"] = True
                except BaseException as error:
                    CampaignPolicy.failure(report, "evidence", error)
                if CampaignPolicy.can_continue(report):
                    try:
                        report["removed_private_artifact_hashes"] = CampaignPolicy.snapshot(evidence.pending)
                        evidence.remove_successful_pending()
                        report["raw_body_disposition"] = "removed_after_complete_success"
                    except BaseException as error:
                        CampaignPolicy.failure(report, "evidence", error)
                report["seconds"] = round(time.monotonic() - began, 3)
                report["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                retained = [evidence.directory] if evidence.directory is not None else []
                ledger.finish(directory, reservation, report, retained)
            status = {key: report.get(key) for key in ("case_outcome", "capture_complete", "first_divergence", "cleanup_complete", "provenance_complete", "child_reaped", "process_group_settled", "seconds")}
            status["continue_allowed"] = CampaignPolicy.can_continue(report) and reservation["attempt"] < CampaignPolicy.LIMIT
            print(json.dumps(status), flush=True)
            return 0 if CampaignPolicy.can_continue(report) else 1

    @staticmethod
    def interrupted(signum, _frame):
        raise InterruptedError(f"Capture interrupted by {signal.Signals(signum).name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True, help="New private ignored evidence directory")
    parser.add_argument("--environment", required=True, help="Existing private shell environment file")
    parser.add_argument("--prior-evidence", action="append", default=[], help="Immutable original file/tree; repeat to include all retained evidence")
    arguments = parser.parse_args()
    os.umask(0o077)
    for chosen in (signal.SIGINT, signal.SIGTERM):
        signal.signal(chosen, CaptureCampaign.interrupted)
    try:
        return CaptureCampaign(arguments).run()
    except BaseException as error:
        print(json.dumps({"state": "blocked", "error_type": type(error).__name__, "reason": str(error)}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

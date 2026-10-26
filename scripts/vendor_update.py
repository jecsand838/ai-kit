#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import textwrap
import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
PRERELEASE_PAGE_SIZE = 20
STABLE_RELEASE_PAGE_SIZE = 100
MAX_RELEASE_LIST_PAGES = 5
DEFAULT_CODEX_PLANNING_TIMEOUT_SECONDS = 300.0
DEFAULT_CODEX_IMPLEMENTATION_TIMEOUT_SECONDS = 900.0
COMPLETION_POLICIES = ("strict", "no-regression")
COORDINATION_SCOPE = (
    "scripts/vendor_update.py",
    "scripts/tests/test_vendor_campaign.py",
    ".agents/skills/vendor-refresh-campaign/references/workflow.md",
)
PREREQUISITE_INPUTS = (
    "resume.json", "metadata.json", "dossier.md", "plan.md", "release.json",
    "original-vendor-lock.toml", "candidate-vendor-lock.toml",
    "upstream_diff_stat.txt", "upstream_changed_files.txt",
)
SEMVER_TAG_RE = re.compile(
    r"^(?:v)?(?P<core>(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*))"
    r"(?P<prerelease>-[0-9A-Za-z.-]+)?"
    r"(?P<build>\+[0-9A-Za-z.-]+)?$"
)
ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
FORMAT_DIFF_RE = re.compile(r"^Diff in (?P<path>.+?):\d+:$")
FAILED_TEST_RE = re.compile(r"^test (?P<name>\S+) \.\.\. FAILED$")
TEST_FAILURE_HEADER_RE = re.compile(r"^---- (?P<name>.+?) stdout ----$")
PANIC_HEADER_RE = re.compile(
    r"^thread '(?P<name>[^']+)'(?: \(\d+\))? panicked at .+?:\d+:\d+:$"
)
ERROR_LINE_RE = re.compile(r"^error(?:\[[^]]+\])?: (?P<message>.+)$", re.IGNORECASE)
SOURCE_LOCATION_RE = re.compile(r"^\s*-->\s+(?P<path>.+?):\d+:\d+$")
SHELL_FAILURE_RE = re.compile(
    r"(missing required|connection refused|failed to connect|could not connect|timed out|command not found)",
    re.IGNORECASE,
)


class VendorUpdateError(RuntimeError):
    pass


@dataclass(frozen=True)
class VendorConfig:
    crate: str
    name: str
    repo: str
    source_kind: str
    channel: str
    release_tag_prefix: str | None
    archive_prefix: str
    upstream_focus_paths: tuple[str, ...]
    local_focus_paths: tuple[str, ...]
    plan_prompt_suffix: str
    preflight_gates: tuple[tuple[str, ...], ...]
    completion_gates: tuple[tuple[str, ...], ...]

    def archive_version(self, tag: str) -> str:
        if self.release_tag_prefix is not None and tag.startswith(self.release_tag_prefix):
            return tag[len(self.release_tag_prefix) :]
        return tag


@dataclass(frozen=True)
class VendorLock:
    tag: str
    archive: str
    extracted_dir: str
    published_at: str
    sha256: str


@dataclass(frozen=True)
class ReleaseInfo:
    tag: str
    published_at: str
    prerelease: bool


@dataclass(frozen=True)
class CrateState:
    crate: str
    crate_root: Path
    vendor_root: Path
    configs: dict[str, VendorConfig]
    locks: dict[str, VendorLock]


@dataclass(frozen=True)
class CompletionGateRun:
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    signatures: tuple[str, ...]


class VendorUpdater:
    def __init__(
        self,
        workspace_root: Path,
        *,
        env: Mapping[str, str] | None = None,
        stdout: Any = None,
        stderr: Any = None,
    ) -> None:
        self.workspace_root = workspace_root.resolve()
        self.env = dict(env or os.environ)
        self.stdout = stdout or sys.stdout
        self.stderr = stderr or sys.stderr
        self.suppress_progress = False
        self.codex_planning_timeout_seconds = self._timeout_from_env(
            "VENDOR_UPDATE_CODEX_PLANNING_TIMEOUT_SECONDS",
            DEFAULT_CODEX_PLANNING_TIMEOUT_SECONDS,
        )
        self.codex_implementation_timeout_seconds = self._timeout_from_env(
            "VENDOR_UPDATE_CODEX_IMPLEMENTATION_TIMEOUT_SECONDS",
            DEFAULT_CODEX_IMPLEMENTATION_TIMEOUT_SECONDS,
        )

    def check(self, crate_name: str | None, *, json_output: bool = False) -> int:
        crate_states = self._load_crate_states(crate_name)
        statuses: list[dict[str, Any]] = []
        previous_suppress_progress = self.suppress_progress
        self.suppress_progress = json_output
        try:
            for crate_state in crate_states:
                for vendor_name, config in crate_state.configs.items():
                    lock = crate_state.locks[vendor_name]
                    latest = self._resolve_latest_release(config)
                    status = "up-to-date" if latest.tag == lock.tag else "update-available"
                    entry = {
                        "crate": crate_state.crate,
                        "vendor": vendor_name,
                        "channel": config.channel,
                        "status": status,
                        "current": {
                            "tag": lock.tag,
                            "published_at": lock.published_at,
                        },
                        "latest": {
                            "tag": latest.tag,
                            "published_at": latest.published_at,
                        },
                    }
                    if json_output:
                        statuses.append(entry)
                        continue
                    self._print(
                        (
                            f"{entry['crate']}/{entry['vendor']}: {entry['status']} "
                            f"current={lock.tag} ({lock.published_at}) "
                            f"latest={latest.tag} ({latest.published_at})"
                        )
                    )
        finally:
            self.suppress_progress = previous_suppress_progress
        if json_output:
            print(json.dumps(statuses, indent=2), file=self.stdout)
        return 0

    def update(
        self,
        crate_name: str,
        vendor_name: str,
        *,
        git_mode: str = "managed",
        completion_policy: str = "strict",
    ) -> int:
        crate_state = self._load_crate_state(crate_name)
        if vendor_name not in crate_state.configs:
            raise VendorUpdateError(
                f"crate `{crate_name}` does not define vendor `{vendor_name}`"
            )

        config = crate_state.configs[vendor_name]
        current_lock = crate_state.locks[vendor_name]
        if completion_policy not in COMPLETION_POLICIES:
            raise VendorUpdateError(
                f"unsupported completion policy `{completion_policy}`"
            )

        self._ensure_clean_tracked_worktree()
        latest = self._resolve_latest_release(config)
        if latest.tag == current_lock.tag:
            self._print(
                f"{crate_name}/{vendor_name} already tracks {latest.tag} ({latest.published_at})"
            )
            return 0

        initial_dirty = self._dirty_path_fingerprints()
        if git_mode == "managed":
            branch_name = self._branch_name(crate_name, vendor_name, latest.tag)
            self._run(
                ["git", "switch", "-c", branch_name],
                cwd=self.workspace_root,
                description=f"create branch {branch_name}",
            )
            landing_branch = branch_name
        elif git_mode == "current-branch":
            landing_branch = self._current_branch_name()
        else:
            raise VendorUpdateError(f"unsupported git mode `{git_mode}`")

        release_details = self._fetch_release_details(config, latest.tag)

        current_archive_path = crate_state.vendor_root / current_lock.archive
        if not current_archive_path.is_file():
            raise VendorUpdateError(
                f"missing current archive `{current_archive_path}` for {crate_name}/{vendor_name}"
            )

        current_extract_path = crate_state.vendor_root / current_lock.extracted_dir
        self._materialize_extract_if_missing(current_archive_path, current_extract_path)

        target_version = config.archive_version(latest.tag)
        target_archive_name = f"{config.archive_prefix}-{target_version}.zip"
        target_extract_name = f"{config.archive_prefix}-{target_version}"
        target_archive_path = crate_state.vendor_root / target_archive_name
        target_extract_path = crate_state.vendor_root / target_extract_name
        if target_archive_path.exists() and not target_archive_path.is_file():
            raise VendorUpdateError(
                f"target archive path `{target_archive_path}` exists but is not a file"
            )
        if target_extract_path.exists() and not target_extract_path.is_dir():
            raise VendorUpdateError(
                f"target extracted path `{target_extract_path}` exists but is not a directory"
            )

        if target_archive_path.exists():
            new_sha256 = self._sha256(target_archive_path)
        else:
            downloaded_archive = self._download_release_archive(config.repo, latest.tag)
            shutil.move(str(downloaded_archive), target_archive_path)
            new_sha256 = self._sha256(target_archive_path)
        if not target_extract_path.exists():
            self._extract_archive_to_path(target_archive_path, target_extract_path)

        dossier_dir = self._create_dossier(
            crate_state=crate_state,
            config=config,
            current_lock=current_lock,
            latest=latest,
            release_details=release_details,
            current_extract_path=current_extract_path,
            target_archive_path=target_archive_path,
            target_extract_path=target_extract_path,
            new_sha256=new_sha256,
            completion_policy=completion_policy,
        )

        baseline_gate_runs: tuple[CompletionGateRun, ...] = ()
        if completion_policy == "no-regression":
            self._run_preflight_gates(config)
            baseline_gate_runs = self._observe_completion_gates(
                config,
                dossier_dir=dossier_dir,
                stage="baseline",
            )
            self._write_completion_gate_report(
                dossier_dir=dossier_dir,
                baseline=baseline_gate_runs,
                post=(),
                comparisons=(),
                regressions=(),
            )
            self._reject_baseline_prerequisite_failures(baseline_gate_runs)

        state = self._prepare_resume_state(
            crate_state, config, dossier_dir, current_lock, latest,
            release_details, new_sha256, landing_branch, git_mode,
            completion_policy, initial_dirty, baseline_gate_runs,
        )
        plan_path = dossier_dir / "plan.md"
        self._run_codex_planning(
            crate_state, config, dossier_dir, plan_path,
            completion_policy=completion_policy,
        )
        return self._execute_prepared_update(crate_state, config, dossier_dir, state)

    def resume(self, dossier_path: Path, *, implementation_only: bool = False,
               validation_only: bool = False) -> int:
        dossier_dir = dossier_path.resolve()
        if dossier_dir.name == "dossier.md":
            dossier_dir = dossier_dir.parent
        if not dossier_dir.is_relative_to(self.workspace_root / "target/vendor-updates"):
            raise VendorUpdateError("resume dossier must belong to this workspace's target/vendor-updates")
        try:
            state = json.loads((dossier_dir / "resume.json").read_text())
            if "prerequisite_import" in state:
                self._prerequisite_dossier_path(
                    dossier_path.parent if dossier_path.name == "dossier.md" else dossier_path)
            if state["version"] == 2:
                if implementation_only == validation_only:
                    raise VendorUpdateError("coordinated resume requires exactly one of --implementation-only or --validation-only")
                crate_state = self._load_crate_state("agent-sdk")
                self._validate_coordinated_update(crate_state, dossier_dir, state)
                return self._execute_prepared_update(
                    crate_state, crate_state.configs["codex"], dossier_dir, state,
                    implementation_only=implementation_only, validation_only=validation_only)
            if implementation_only or validation_only:
                raise VendorUpdateError("split execution is only supported for a coordinated dossier")
            if state["version"] != 1 or state["workspace"] != str(self.workspace_root):
                raise VendorUpdateError("resume dossier belongs to a different workspace or format")
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", state["crate"]):
                raise VendorUpdateError("invalid resume crate path")
            crate_state = self._load_crate_state(state["crate"])
            config = crate_state.configs[state["vendor"]]
            self._validate_prepared_update(crate_state, config, dossier_dir, state)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise VendorUpdateError(f"invalid resume dossier: {exc}") from exc
        if ((dossier_dir / "plan.md").is_symlink()
            or not (dossier_dir / "plan.md").is_file()
            or not (dossier_dir / "plan.md").read_text().strip()):
            raise VendorUpdateError("resume requires the saved nonempty plan.md")
        return self._execute_prepared_update(crate_state, config, dossier_dir, state)

    def import_prerequisite(
        self, dossier_path: Path, destination_path: Path, commit: str, paths: list[str],
    ) -> Path:
        """Prepare the same frozen leaf after one explicitly scoped prerequisite commit."""
        try:
            source = self._prerequisite_dossier_path(dossier_path)
            destination = self._prerequisite_dossier_path(destination_path)
            if (not source.is_dir() or destination.exists()
                or destination.is_relative_to(source) or source.is_relative_to(destination)):
                raise VendorUpdateError("prerequisite destination must be a new, separate dossier")
            ignored = self._run(
                ["git", "check-ignore", "--quiet", "--", str(destination.relative_to(self.workspace_root))],
                cwd=self.workspace_root, capture_output=True, accepted_exit_codes=(0, 1),
                description="verify prerequisite dossier is ignored",
            )
            if ignored.returncode != 0:
                raise VendorUpdateError("prerequisite destination must be ignored")
            files = self._prerequisite_source_files(source)
            state = json.loads((source / "resume.json").read_text())
            self._require_prerequisite_source(state)
            crate = self._load_crate_state(state["crate"])
            config = crate.configs[state["vendor"]]
            self._validate_prerequisite_commit(state, commit, paths)
            self._validate_prepared_update(crate, config, source, state, expected_head=commit)
            if self._sha256(crate.vendor_root / "vendor-lock.toml") != state["original_lock_sha256"]:
                raise VendorUpdateError("prerequisite import requires the restored original lock")
            if not (source / "plan.md").read_text().strip():
                raise VendorUpdateError("prerequisite import requires a saved nonempty plan")
            imported = dict(state)
            imported["head"] = commit
            imported["prerequisite_import"] = {
                "source": str(source), "files": files, "commit": commit,
                "parent": state["head"], "paths": sorted(paths),
            }
            destination.mkdir(parents=True)
            for filename in PREREQUISITE_INPUTS:
                copied_name = "source-resume.json" if filename == "resume.json" else filename
                (destination / copied_name).write_bytes((source / filename).read_bytes())
            (destination / "resume.json").write_text(json.dumps(imported, indent=2) + "\n")
            self._validate_prepared_update(crate, config, destination, imported)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise VendorUpdateError(f"invalid prerequisite import: {exc}") from exc
        self._print(f"prepared strict prerequisite dossier: {destination}")
        return destination

    def _prerequisite_dossier_path(self, path: Path) -> Path:
        absolute = path.absolute()
        resolved = absolute.resolve()
        if (not resolved.is_relative_to(self.workspace_root / "target/vendor-updates")
            or resolved == self.workspace_root / "target/vendor-updates"):
            raise VendorUpdateError("prerequisite dossier path escapes its owner or uses a symlink")
        for component in (absolute, *absolute.parents):
            if component.resolve() == self.workspace_root:
                break
            if component.is_symlink():
                raise VendorUpdateError("prerequisite dossier path uses a symlink")
        return resolved

    def _prerequisite_source_files(self, source: Path) -> dict[str, str]:
        files = {}
        for path in sorted(source.rglob("*")):
            if path.is_symlink() or (not path.is_file() and not path.is_dir()):
                raise VendorUpdateError("prerequisite source input is not a regular file or directory")
            if path.is_file():
                files[path.relative_to(source).as_posix()] = self._sha256(path)
        if not set(PREREQUISITE_INPUTS).issubset(files):
            raise VendorUpdateError("prerequisite source is missing frozen inputs")
        return files

    def _require_prerequisite_source(self, state: dict[str, Any]) -> None:
        if (state["version"] != 1 or state["workspace"] != str(self.workspace_root)
            or state["git_mode"] != "current-branch" or state["completion_policy"] != "strict"
            or "prerequisite_import" in state
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", state["crate"])):
            raise VendorUpdateError("prerequisite import requires an original strict current-branch v1 leaf")

    def _validate_prerequisite_commit(
        self, original: dict[str, Any], commit: str, paths: list[str],
    ) -> None:
        if not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", commit) or self._current_head() != commit:
            raise VendorUpdateError("prerequisite commit must be the exact full current HEAD")
        parents = self._run(
            ["git", "rev-list", "--parents", "--max-count=1", commit],
            cwd=self.workspace_root, capture_output=True, description="verify prerequisite ancestry",
        ).stdout.split()
        if parents != [commit, original["head"]]:
            raise VendorUpdateError("prerequisite commit must have the original HEAD as its sole parent")
        if not paths or len(set(paths)) != len(paths):
            raise VendorUpdateError("prerequisite paths must be a nonempty exact allowlist without duplicates")
        for path in paths:
            candidate = PurePosixPath(path)
            if (candidate.is_absolute() or not candidate.parts or ".." in candidate.parts
                or candidate.parts[0] == "tmp" or candidate.as_posix() != path or "\\" in path
                or not (self.workspace_root / path).resolve().is_relative_to(self.workspace_root)):
                raise VendorUpdateError(f"invalid prerequisite path: {path}")
            if (path == "Cargo.lock" or any(path == scope or path.startswith(scope.rstrip("/") + "/")
                    or (self.workspace_root / path).resolve().is_relative_to(self.workspace_root / scope)
                    for scope in original["permitted_scope"])):
                raise VendorUpdateError(f"prerequisite path touches frozen vendor scope: {path}")
            if "vendor" in candidate.parts:
                raise VendorUpdateError(f"prerequisite path touches vendor artifacts: {path}")
        changed = self._run(
            ["git", "diff-tree", "--no-commit-id", "--raw", "--no-abbrev", "--no-renames", "-r", "-z", commit, "--"],
            cwd=self.workspace_root, capture_output=True, description="verify exact prerequisite paths",
        ).stdout.split("\0")
        if changed[-1] != "" or len(changed) % 2 != 1:
            raise VendorUpdateError("invalid prerequisite committed path evidence")
        for header in changed[:-1:2]:
            fields = header.split()
            if (len(fields) != 5 or not fields[0].startswith(":")
                or fields[0][1:] not in {"000000", "100644", "100755"}
                or fields[1] not in {"000000", "100644", "100755"}):
                raise VendorUpdateError("prerequisite commit must change regular files only")
        if sorted(changed[1::2]) != sorted(paths):
            raise VendorUpdateError("prerequisite committed paths differ from the exact allowlist")

    def _validate_prerequisite_import(
        self, crate: CrateState, config: VendorConfig, dossier: Path, state: dict[str, Any],
    ) -> None:
        lineage = state["prerequisite_import"]
        if set(lineage) != {"source", "files", "commit", "parent", "paths"}:
            raise VendorUpdateError("invalid prerequisite lineage")
        source = self._prerequisite_dossier_path(Path(lineage["source"]))
        destination = self._prerequisite_dossier_path(dossier)
        if source.is_relative_to(destination) or destination.is_relative_to(source):
            raise VendorUpdateError("prerequisite source and destination must remain separate")
        if self._prerequisite_source_files(source) != lineage["files"]:
            raise VendorUpdateError("prerequisite source input inventory or hashes changed")
        for filename in PREREQUISITE_INPUTS:
            copied_name = "source-resume.json" if filename == "resume.json" else filename
            path = destination / copied_name
            if (path.is_symlink() or not path.is_file()
                or path.resolve().parent != destination or self._sha256(path) != lineage["files"][filename]):
                raise VendorUpdateError(f"prerequisite saved input changed: {path.name}")
        original = json.loads((source / "resume.json").read_text())
        self._require_prerequisite_source(original)
        expected = dict(original)
        expected["head"] = lineage["commit"]
        expected["prerequisite_import"] = lineage
        if state != expected or lineage["parent"] != original["head"]:
            raise VendorUpdateError("prerequisite recovery differs from the imported original")
        self._validate_prerequisite_commit(original, lineage["commit"], lineage["paths"])
        self._validate_prepared_update(crate, config, source, original, expected_head=lineage["commit"])
        for filename in ("resume.json", "implementation.md", "original-archive.zip"):
            output = destination / filename
            if output.is_symlink() or (output.exists() and not output.is_file()):
                raise VendorUpdateError(f"prerequisite updater output escapes its owner: {filename}")

    def coordinate(
        self, dossier_path: Path, vendor_name: str, target_tag: str, plan_path: Path,
    ) -> Path:
        """Explicitly import a failed Codex leaf into a strict two-vendor campaign.

        Preparation never runs implementation, publishes a lock, or commits.
        The old leaf remains immutable; the new dossier owns the combined lock.
        """
        source = dossier_path.resolve()
        if not source.is_relative_to(self.workspace_root / "target/vendor-updates"):
            raise VendorUpdateError("source dossier must belong to this workspace")
        state = json.loads((source / "resume.json").read_text())
        if (state["version"] != 1 or state["workspace"] != str(self.workspace_root)
            or state["crate"] != "agent-sdk" or state["vendor"] != "codex"
            or vendor_name != "opencode" or state["git_mode"] != "current-branch"
            or state["completion_policy"] != "strict" or "prerequisite_import" in state):
            raise VendorUpdateError("coordination requires an original strict current-branch Agent Codex dossier")
        crate = self._load_crate_state("agent-sdk")
        self._validate_prepared_update(crate, crate.configs["codex"], source, state,
                                       coordination=True)
        if self._sha256(crate.vendor_root / "vendor-lock.toml") != state["original_lock_sha256"]:
            raise VendorUpdateError("coordination requires the restored original lock")
        if list(crate.configs) != ["codex", "opencode"]:
            raise VendorUpdateError("coordination requires the Agent manifest vendor order")
        self._require_common_gates(crate)
        for plan in (source / "plan.md", plan_path):
            if plan.is_symlink() or not plan.is_file() or not plan.read_text().strip():
                raise VendorUpdateError("coordination requires saved nonempty plans")
        # A new campaign explicitly checks releases; a subsequent resume never does.
        for name, tag in (("codex", state["target"]["tag"]), (vendor_name, target_tag)):
            latest = self._resolve_latest_release(crate.configs[name])
            if latest.tag != tag:
                raise VendorUpdateError(f"{name} stable target advanced to {latest.tag}; refresh research first")
        config = crate.configs[vendor_name]
        details = self._fetch_release_details(config, target_tag)
        latest = ReleaseInfo(target_tag, details["publishedAt"], False)
        original = self._parse_vendor_locks(source / "original-vendor-lock.toml",
                                            self._read_toml(source / "original-vendor-lock.toml"))
        candidate = self._parse_vendor_locks(source / "candidate-vendor-lock.toml",
                                             self._read_toml(source / "candidate-vendor-lock.toml"))
        target_name = f"{config.archive_prefix}-{config.archive_version(target_tag)}"
        destination = self.workspace_root / "target/vendor-updates/agent-sdk/coordinated" / (
            state["target"]["tag"] + "_" + target_tag)
        target_leaf = self.workspace_root / "target/vendor-updates/agent-sdk/opencode" / target_tag
        if destination.exists() or target_leaf.exists():
            raise VendorUpdateError("coordinated or target dossier already exists; inspect and resume it")
        archive = crate.vendor_root / f"{target_name}.zip"
        extract = crate.vendor_root / target_name
        if archive.is_symlink() or extract.is_symlink():
            raise VendorUpdateError("target paths must not be symlinks")
        downloaded = self._download_release_archive(config.repo, target_tag)
        digest = self._sha256(downloaded)
        if archive.exists() and self._sha256(archive) != digest:
            raise VendorUpdateError("existing target archive differs from official release download")
        if not archive.exists():
            shutil.copyfile(downloaded, archive)
        self._materialize_extract_if_missing(archive, extract)
        old = original[vendor_name]
        self._materialize_extract_if_missing(crate.vendor_root / old.archive,
                                             crate.vendor_root / old.extracted_dir)
        self._create_dossier(crate_state=crate, config=config, current_lock=old,
            latest=latest, release_details=details,
            current_extract_path=crate.vendor_root / old.extracted_dir,
            target_archive_path=archive, target_extract_path=extract,
            new_sha256=digest, completion_policy="strict")
        (target_leaf / "plan.md").write_bytes(plan_path.read_bytes())
        candidate[vendor_name] = VendorLock(target_tag, archive.name, extract.name,
                                             details["publishedAt"], digest)
        destination.mkdir(parents=True)
        (destination / "original-vendor-lock.toml").write_bytes(
            (source / "original-vendor-lock.toml").read_bytes())
        self._write_vendor_lock(destination / "candidate-vendor-lock.toml", candidate)
        imports = {}
        vendors = {}
        for name, leaf in (("codex", source), ("opencode", target_leaf)):
            copied = destination / name
            copied.mkdir()
            files = {}
            for filename in ("resume.json", "metadata.json", "dossier.md", "plan.md",
                             "original-vendor-lock.toml", "candidate-vendor-lock.toml",
                             "release.json", "upstream_diff_stat.txt", "upstream_changed_files.txt"):
                path = leaf / filename
                if path.is_symlink():
                    raise VendorUpdateError("imported dossier files must not be symlinks")
                if path.is_file():
                    shutil.copyfile(path, copied / filename)
                    files[filename] = self._sha256(path)
            imports[name] = {"path": str(leaf), "files": files}
            vendors[name] = {"current": vars(original[name]), "target": vars(candidate[name])}
        combined = {
            "version": 2, "workspace": str(self.workspace_root), "crate": "agent-sdk",
            "branch": state["branch"], "head": state["head"],
            "git_mode": "current-branch", "completion_policy": "strict",
            "initial_dirty": state["initial_dirty"], "vendors": vendors, "imports": imports,
            "manifest_sha256": state["manifest_sha256"],
            "original_lock_sha256": self._sha256(destination / "original-vendor-lock.toml"),
            "candidate_lock_sha256": self._sha256(destination / "candidate-vendor-lock.toml"),
            "permitted_scope": self._coordinated_scope(crate),
        }
        (destination / "resume.json").write_text(json.dumps(combined, indent=2) + "\n")
        self._validate_coordinated_update(crate, destination, combined)
        self._print(f"prepared strict coordinated dossier: {destination}")
        return destination

    def _require_common_gates(self, crate: CrateState) -> None:
        codex, opencode = crate.configs["codex"], crate.configs["opencode"]
        if (not codex.preflight_gates or not codex.completion_gates
            or codex.preflight_gates != opencode.preflight_gates
            or codex.completion_gates != opencode.completion_gates):
            raise VendorUpdateError("coordinated vendors must share nonempty strict gates")

    def _coordinated_scope(self, crate: CrateState) -> list[str]:
        return [crate.crate, *sorted({path for config in crate.configs.values()
            for path in config.local_focus_paths if not path.startswith(f"{crate.crate}/")}),
            *COORDINATION_SCOPE]

    def _validate_coordinated_update(
        self, crate: CrateState, dossier: Path, state: dict[str, Any],
        *, expected_resume: bytes | None = None,
    ) -> list[str]:
        if (state["version"] != 2 or state["workspace"] != str(self.workspace_root)
            or state["crate"] != "agent-sdk" or state["git_mode"] != "current-branch"
            or state["completion_policy"] != "strict"
            or list(state["vendors"]) != ["codex", "opencode"]
            or set(state["imports"]) != {"codex", "opencode"}
            or state["permitted_scope"] != self._coordinated_scope(crate)):
            raise VendorUpdateError("invalid coordinated campaign contract")
        recovery = dossier / "resume.json"
        if (recovery.is_symlink() or not recovery.is_file()
            or (expected_resume is not None and recovery.read_bytes() != expected_resume)):
            raise VendorUpdateError("coordinated recovery metadata changed")
        self._require_common_gates(crate)
        if self._current_branch_name() != state["branch"] or self._current_head() != state["head"]:
            raise VendorUpdateError("coordinated campaign branch or HEAD changed")
        if crate.vendor_root.is_symlink() or not crate.vendor_root.resolve().is_relative_to(self.workspace_root):
            raise VendorUpdateError("vendor directory escapes workspace")
        for path, digest in (
            (crate.vendor_root / "vendors.toml", state["manifest_sha256"]),
            (dossier / "original-vendor-lock.toml", state["original_lock_sha256"]),
            (dossier / "candidate-vendor-lock.toml", state["candidate_lock_sha256"]),
        ):
            if path.is_symlink() or self._sha256(path) != digest:
                raise VendorUpdateError(f"coordinated input changed: {path.name}")
        original = self._parse_vendor_locks(dossier / "original-vendor-lock.toml",
                                            self._read_toml(dossier / "original-vendor-lock.toml"))
        candidate = self._parse_vendor_locks(dossier / "candidate-vendor-lock.toml",
                                             self._read_toml(dossier / "candidate-vendor-lock.toml"))
        if set(original) != set(candidate) or set(original) != set(state["vendors"]):
            raise VendorUpdateError("coordinated inventory changed")
        vendor_paths = {"agent-sdk/vendor/vendor-lock.toml"}
        for name, vendor in state["vendors"].items():
            imported = state["imports"][name]
            source = Path(imported["path"])
            copied = dossier / name
            if (source.is_symlink() or not source.is_dir()
                or not source.resolve().is_relative_to(self.workspace_root / "target/vendor-updates")
                or source.resolve().is_relative_to(dossier.resolve())):
                raise VendorUpdateError("coordinated source dossier escapes workspace")
            if (copied.is_symlink() or not copied.is_dir()
                or copied.resolve() != dossier.resolve() / name):
                raise VendorUpdateError("coordinated copied dossier escapes its owner")
            output = copied / "implementation.md"
            if (output.is_symlink() or (output.exists() and not output.is_file())
                or output.resolve().parent != copied.resolve()):
                raise VendorUpdateError("coordinated implementation output escapes its owner")
            if not {"plan.md", "dossier.md", "metadata.json"}.issubset(imported["files"]):
                raise VendorUpdateError("coordinated dossier missing saved inputs")
            for filename, digest in imported["files"].items():
                if PurePosixPath(filename).name != filename or filename in {".", ".."}:
                    raise VendorUpdateError("invalid coordinated input path")
                for owner in (source, copied):
                    path = owner / filename
                    if (path.is_symlink() or path.resolve().parent != owner.resolve()
                        or self._sha256(path) != digest):
                        raise VendorUpdateError(f"coordinated saved input changed: {path.name}")
            for phase, locks in (("current", original), ("target", candidate)):
                locked = locks[name]
                if vendor[phase] != vars(locked):
                    raise VendorUpdateError("coordinated target differs from saved lock")
                for field in ("archive", "extracted_dir"):
                    component = getattr(locked, field)
                    path = crate.vendor_root / component
                    if (PurePosixPath(component).name != component or component in {"", ".", ".."}
                        or "\\" in component or path.is_symlink()
                        or not path.resolve().is_relative_to(crate.vendor_root.resolve())):
                        raise VendorUpdateError("invalid coordinated archive path")
                archive = crate.vendor_root / locked.archive
                if not archive.is_file() or self._sha256(archive) != locked.sha256:
                    raise VendorUpdateError("coordinated archive changed or missing")
                vendor_paths.add(f"agent-sdk/vendor/{locked.archive}")
        lock = crate.vendor_root / "vendor-lock.toml"
        if lock.is_symlink() or self._sha256(lock) not in {
            state["original_lock_sha256"], state["candidate_lock_sha256"]}:
            raise VendorUpdateError("coordinated lock is neither the original nor combined candidate")
        dirty = self._dirty_path_fingerprints()
        changed = {path for path in set(dirty) | set(state["initial_dirty"])
                   if dirty.get(path) != state["initial_dirty"].get(path)}
        for path in changed:
            if (not any(path == scope or path.startswith(scope + "/") for scope in state["permitted_scope"])
                or not any((self.workspace_root / path).resolve().is_relative_to(self.workspace_root / scope)
                           for scope in state["permitted_scope"])
                or (path.startswith("agent-sdk/vendor/") and path not in vendor_paths)):
                raise VendorUpdateError(f"dirty path outside coordinated scope: {path}")
        return sorted(changed)

    def _prepare_resume_state(
        self, crate_state: CrateState, config: VendorConfig, dossier_dir: Path,
        current_lock: VendorLock, latest: ReleaseInfo, release_details: dict[str, Any],
        new_sha256: str, landing_branch: str, git_mode: str, completion_policy: str,
        initial_dirty: dict[str, str], baseline: tuple[CompletionGateRun, ...],
    ) -> dict[str, Any]:
        lock_path = crate_state.vendor_root / "vendor-lock.toml"
        original_path = dossier_dir / "original-vendor-lock.toml"
        candidate_path = dossier_dir / "candidate-vendor-lock.toml"
        original_path.write_bytes(lock_path.read_bytes())
        updated_locks = dict(crate_state.locks)
        target = VendorLock(
            tag=latest.tag,
            archive=f"{config.archive_prefix}-{config.archive_version(latest.tag)}.zip",
            extracted_dir=f"{config.archive_prefix}-{config.archive_version(latest.tag)}",
            published_at=release_details["publishedAt"],
            sha256=new_sha256,
        )
        updated_locks[config.name] = target
        self._write_vendor_lock(candidate_path, updated_locks)
        external_scope = [path for path in config.local_focus_paths
                          if not path.startswith(f"{crate_state.crate}/")]
        state = {
            "version": 1,
            "workspace": str(self.workspace_root),
            "crate": crate_state.crate,
            "vendor": config.name,
            "branch": landing_branch,
            "head": self._current_head(),
            "git_mode": git_mode,
            "completion_policy": completion_policy,
            "permitted_scope": [crate_state.crate, *external_scope],
            "initial_dirty": initial_dirty,
            "manifest_sha256": self._sha256(crate_state.vendor_root / "vendors.toml"),
            "metadata_sha256": self._sha256(dossier_dir / "metadata.json"),
            "original_lock_sha256": self._sha256(original_path),
            "candidate_lock_sha256": self._sha256(candidate_path),
            "current": {"archive": current_lock.archive,
                        "extracted_dir": current_lock.extracted_dir,
                        "sha256": self._sha256(crate_state.vendor_root / current_lock.archive)},
            "target": {"tag": target.tag, "archive": target.archive,
                       "extracted_dir": target.extracted_dir, "sha256": new_sha256},
            "baseline": [dict(command=list(run.command), returncode=run.returncode,
                              stdout=run.stdout, stderr=run.stderr,
                              signatures=list(run.signatures)) for run in baseline],
        }
        (dossier_dir / "resume.json").write_text(json.dumps(state, indent=2) + "\n")
        return state

    def _validate_prepared_update(
        self, crate_state: CrateState, config: VendorConfig, dossier_dir: Path,
        state: dict[str, Any],
        *, coordination: bool = False, expected_head: str | None = None,
    ) -> list[str]:
        saved_source = dossier_dir / "source-resume.json"
        if ("prerequisite_import" not in state
            and (saved_source.exists() or saved_source.is_symlink())):
            raise VendorUpdateError("prerequisite lineage is missing from imported recovery metadata")
        if "prerequisite_import" in state:
            self._validate_prerequisite_import(crate_state, config, dossier_dir, state)
        if state["completion_policy"] not in COMPLETION_POLICIES:
            raise VendorUpdateError("invalid resume completion policy")
        if state["git_mode"] not in {"managed", "current-branch"}:
            raise VendorUpdateError("invalid resume git mode")
        expected_scope = [crate_state.crate, *[
            path for path in config.local_focus_paths
            if not path.startswith(f"{crate_state.crate}/")
        ]]
        if state["permitted_scope"] != expected_scope:
            raise VendorUpdateError("resume permitted scope differs from the recorded manifest")
        if (not crate_state.vendor_root.resolve().is_relative_to(self.workspace_root)
            or crate_state.vendor_root.is_symlink()):
            raise VendorUpdateError("vendor directory escapes the workspace")
        for name in ("current", "target"):
            for field in ("archive", "extracted_dir"):
                component = state[name][field]
                if (not isinstance(component, str) or component in {"", ".", ".."}
                    or PurePosixPath(component).name != component
                    or "\\" in component):
                    raise VendorUpdateError("invalid resume archive or extraction path")
                path = crate_state.vendor_root / component
                if path.is_symlink() or not path.resolve().is_relative_to(crate_state.vendor_root.resolve()):
                    raise VendorUpdateError("resume archive or extraction path is a symlink escape")
        if self._current_branch_name() != state["branch"]:
            raise VendorUpdateError("resume branch differs from the recorded starting branch")
        if self._current_head() != (state["head"] if expected_head is None else expected_head):
            raise VendorUpdateError("resume HEAD differs from the recorded starting HEAD")
        for path, key in [
            (crate_state.vendor_root / "vendors.toml", "manifest_sha256"),
            (dossier_dir / "metadata.json", "metadata_sha256"),
            (dossier_dir / "original-vendor-lock.toml", "original_lock_sha256"),
            (dossier_dir / "candidate-vendor-lock.toml", "candidate_lock_sha256"),
        ]:
            if path.is_symlink() or self._sha256(path) != state[key]:
                raise VendorUpdateError(f"resume input changed: {path.name}")
        original_locks = self._parse_vendor_locks(
            dossier_dir / "original-vendor-lock.toml",
            self._read_toml(dossier_dir / "original-vendor-lock.toml"),
        )
        candidate_locks = self._parse_vendor_locks(
            dossier_dir / "candidate-vendor-lock.toml",
            self._read_toml(dossier_dir / "candidate-vendor-lock.toml"),
        )
        metadata = json.loads((dossier_dir / "metadata.json").read_text())
        for name, locks in (("current", original_locks), ("target", candidate_locks)):
            locked = locks[config.name]
            for field in ("archive", "extracted_dir"):
                if state[name][field] != getattr(locked, field) or state[name][field] != metadata[name][field]:
                    raise VendorUpdateError(f"resume {name} {field} differs from its locked metadata")
        if (state["target"]["tag"] != candidate_locks[config.name].tag
            or state["target"]["sha256"] != candidate_locks[config.name].sha256
            or metadata["target"]["tag"] != state["target"]["tag"]
            or metadata["target"]["sha256"] != state["target"]["sha256"]):
            raise VendorUpdateError("resume target differs from its candidate lock")
        for name, locked in original_locks.items():
            if name != config.name and candidate_locks.get(name) != locked:
                raise VendorUpdateError("candidate lock changed another vendor")
        if set(original_locks) != set(candidate_locks):
            raise VendorUpdateError("candidate lock changed the vendor inventory")
        if state["completion_policy"] == "no-regression":
            if [run["command"] for run in state["baseline"]] != [list(gate) for gate in config.completion_gates]:
                raise VendorUpdateError("resume baseline does not match the configured gates")
        elif state["baseline"]:
            raise VendorUpdateError("strict resume unexpectedly contains a baseline")
        if (crate_state.vendor_root / "vendor-lock.toml").is_symlink():
            raise VendorUpdateError("vendor lock must not be a symlink")
        lock_digest = self._sha256(crate_state.vendor_root / "vendor-lock.toml")
        if lock_digest not in {state["original_lock_sha256"], state["candidate_lock_sha256"]}:
            raise VendorUpdateError("vendor lock is neither the recorded original nor candidate")
        for name in ("current", "target"):
            archive = crate_state.vendor_root / state[name]["archive"]
            if not archive.is_file() or self._sha256(archive) != state[name]["sha256"]:
                raise VendorUpdateError(f"resume archive changed or missing: {archive}")
        dirty = self._dirty_path_fingerprints()
        changed = [path for path, digest in dirty.items()
                   if state["initial_dirty"].get(path) != digest]
        changed.extend(path for path in state["initial_dirty"] if path not in dirty)
        vendor_prefix = f"{crate_state.crate}/vendor/"
        allowed_vendor = {vendor_prefix + "vendor-lock.toml",
                          vendor_prefix + state["current"]["archive"],
                          vendor_prefix + state["target"]["archive"]}
        permitted_scope = [*state["permitted_scope"], *(COORDINATION_SCOPE if coordination else ())]
        for path in changed:
            candidate = PurePosixPath(path)
            if (candidate.is_absolute() or ".." in candidate.parts
                or not candidate.parts or candidate.parts[0] == "tmp"
                or not any((self.workspace_root / path).resolve().is_relative_to(self.workspace_root / scope)
                           for scope in permitted_scope)
                or not any(path == scope or path.startswith(scope.rstrip("/") + "/")
                           for scope in permitted_scope)
                or (path.startswith(vendor_prefix) and path not in allowed_vendor)):
                raise VendorUpdateError(f"dirty path outside recorded vendor scope: {path}")
        return sorted(set(changed))

    def _execute_prepared_update(
        self, crate_state: CrateState, config: VendorConfig, dossier_dir: Path,
        state: dict[str, Any],
        *, implementation_only: bool = False, validation_only: bool = False,
    ) -> int:
        coordinated = state["version"] == 2
        if coordinated:
            self._validate_coordinated_update(crate_state, dossier_dir, state)
            recovery_bytes = (dossier_dir / "resume.json").read_bytes()
            vendors = state["vendors"]
        else:
            self._validate_prepared_update(crate_state, config, dossier_dir, state)
            vendors = {config.name: state}
            recovery_bytes = (dossier_dir / "resume.json").read_bytes()
        for vendor in vendors.values():
            for name in ("current", "target"):
                self._materialize_extract_if_missing(
                    crate_state.vendor_root / vendor[name]["archive"],
                    crate_state.vendor_root / vendor[name]["extracted_dir"],
                )
        lock_path = crate_state.vendor_root / "vendor-lock.toml"
        original = (dossier_dir / "original-vendor-lock.toml").read_bytes()
        candidate = (dossier_dir / "candidate-vendor-lock.toml").read_bytes()
        completion_policy = state["completion_policy"]
        # Retain a recovery copy until the local commit succeeds.
        retained = []
        for name, vendor in vendors.items():
            current_archive = crate_state.vendor_root / vendor["current"]["archive"]
            current_extract = crate_state.vendor_root / vendor["current"]["extracted_dir"]
            archive_backup = dossier_dir / (f"{name}-original-archive.zip" if coordinated else "original-archive.zip")
            shutil.copyfile(current_archive, archive_backup)
            retained.append((current_archive, current_extract, archive_backup))
        committed = False
        primary_error = None
        commit_tree = None
        commit_message = f"{crate_state.crate}: update " + ", ".join(
            f"{name} vendor to {vendor['target']['tag']}" for name, vendor in vendors.items())
        try:
            lock_path.write_bytes(candidate)
            for name in (() if validation_only else vendors):
                leaf = dossier_dir / name if coordinated else dossier_dir
                self._run_codex_implementation(
                    crate_state, crate_state.configs[name], leaf, leaf / "plan.md",
                    completion_policy=completion_policy,
                    coordinated_source=Path(state["imports"][name]["path"]) if coordinated else None,
                    prerequisite_source=(Path(state["prerequisite_import"]["source"])
                                         if "prerequisite_import" in state else None),
                )
                if lock_path.read_bytes() != candidate:
                    raise VendorUpdateError("implementation changed the updater-owned candidate lock")
                if coordinated:
                    self._validate_coordinated_update(
                        crate_state, dossier_dir, state, expected_resume=recovery_bytes)
                elif "prerequisite_import" in state:
                    if (dossier_dir / "resume.json").read_bytes() != recovery_bytes:
                        raise VendorUpdateError("prerequisite recovery metadata changed")
                    self._validate_prepared_update(crate_state, config, dossier_dir, state)
            if lock_path.read_bytes() != candidate:
                raise VendorUpdateError("implementation changed the updater-owned candidate lock")
            if implementation_only:
                self._print("adaptation complete; original lock restored for review; no acceptance gates or commit run")
                return 0
            # Target specifications must be adapted before protocol-aware preflight.
            self._run_preflight_gates(config)
            if completion_policy == "strict":
                self._run_completion_gates(config)
            else:
                baseline = tuple(CompletionGateRun(
                    command=tuple(run["command"]), returncode=run["returncode"],
                    stdout=run["stdout"], stderr=run["stderr"],
                    signatures=tuple(run["signatures"]),
                ) for run in state["baseline"])
                post = self._observe_completion_gates(config, dossier_dir=dossier_dir, stage="post")
                comparisons, regressions = self._compare_completion_gate_runs(baseline, post)
                self._write_completion_gate_report(
                    dossier_dir=dossier_dir, baseline=baseline, post=post,
                    comparisons=comparisons, regressions=regressions,
                )
                if regressions:
                    raise VendorUpdateError(
                        f"completion gate regressions were detected: {', '.join(regressions)}"
                    )
            if lock_path.read_bytes() != candidate:
                raise VendorUpdateError("completion gates changed the updater-owned candidate lock")
            if ("prerequisite_import" in state
                and (dossier_dir / "resume.json").read_bytes() != recovery_bytes):
                raise VendorUpdateError("prerequisite recovery metadata changed")
            changed = (self._validate_coordinated_update(
                           crate_state, dossier_dir, state, expected_resume=recovery_bytes)
                       if coordinated else self._validate_prepared_update(crate_state, config, dossier_dir, state))
            forced_paths = [lock_path]
            for name, vendor in vendors.items():
                current_archive = crate_state.vendor_root / vendor["current"]["archive"]
                current_extract = crate_state.vendor_root / vendor["current"]["extracted_dir"]
                target_archive = crate_state.vendor_root / vendor["target"]["archive"]
                target_extract = crate_state.vendor_root / vendor["target"]["extracted_dir"]
                if current_archive != target_archive:
                    current_archive.unlink()
                if current_extract != target_extract and current_extract.exists():
                    shutil.rmtree(current_extract)
                forced_paths.extend([current_archive, target_archive])
            stage_paths = sorted(set(changed) | {
                str(path.relative_to(self.workspace_root)) for path in forced_paths
            })
            if coordinated:
                staged = self._run(["git", "diff", "--cached", "--name-only", "-z"],
                    cwd=self.workspace_root, capture_output=True,
                    description="check existing staged ownership").stdout.split("\0")
                if any(path and path not in stage_paths for path in staged):
                    raise VendorUpdateError("index contains unrelated staged changes")
            self._run(["git", "add", "-A", "--", *stage_paths], cwd=self.workspace_root,
                      description="stage vendor-owned changes")
            commit_tree = self._run(["git", "write-tree"], cwd=self.workspace_root,
                                    capture_output=True, description="record vendor commit tree").stdout.strip()
            self._run(["git", "commit", "-m", commit_message], cwd=self.workspace_root,
                      description="create commit")
            committed = True
        except BaseException as error:
            primary_error = error
            raise
        finally:
            if not committed and commit_tree is not None:
                current_head = self._current_head()
                if current_head != state["head"]:
                    committed = True
                    identity = self._run(
                        ["git", "show", "-s", "--format=%P%x00%T%x00%s", "HEAD"],
                        cwd=self.workspace_root, capture_output=True,
                        description="resolve interrupted commit outcome",
                    ).stdout.rstrip("\n").split("\0")
                    if identity == [state["head"], commit_tree, commit_message]:
                        self._print(f"vendor commit completed before interruption: {current_head}")
                    else:
                        self._print("HEAD changed during commit; preserving checkout for inspection")
            if not committed:
                restoration_errors = []
                try:
                    lock_path.write_bytes(original)
                except OSError as error:
                    restoration_errors.append(f"restoring the original vendor lock also failed: {error}")
                for current_archive, current_extract, archive_backup in retained:
                    try:
                        if not current_archive.exists():
                            shutil.copyfile(archive_backup, current_archive)
                        self._materialize_extract_if_missing(current_archive, current_extract)
                    except (OSError, VendorUpdateError, zipfile.BadZipFile) as error:
                        restoration_errors.append(
                            f"restoring {current_archive.name} also failed: {error}")
                if restoration_errors:
                    if primary_error is None:
                        raise VendorUpdateError("; ".join(restoration_errors))
                    for error in restoration_errors:
                        print(f"error: {error}", file=self.stderr)
        if state["git_mode"] == "managed":
            self._run(["git", "push", "-u", "origin", state["branch"]],
                      cwd=self.workspace_root, description="push branch")
        if coordinated:
            self._print(f"{commit_message} on {state['branch']}")
        else:
            self._print(f"updated {crate_state.crate}/{config.name} to {state['target']['tag']} on {state['branch']}")
        return 0

    def _current_head(self) -> str:
        return self._run(["git", "rev-parse", "HEAD"], cwd=self.workspace_root,
                         capture_output=True, description="record starting HEAD").stdout.strip()

    def _dirty_path_fingerprints(self) -> dict[str, str]:
        result = self._run(["git", "status", "--porcelain", "-z", "--untracked-files=all"],
                           cwd=self.workspace_root, capture_output=True,
                           description="inspect vendor change scope")
        entries = iter(result.stdout.split("\0"))
        paths = {}
        for entry in entries:
            if not entry:
                continue
            status, path = entry[:2], entry[3:]
            paths[path] = self._path_fingerprint(self.workspace_root / path, status)
            if "R" in status or "C" in status:
                previous = next(entries)
                paths[previous] = self._path_fingerprint(self.workspace_root / previous, status)
        return paths

    def _path_fingerprint(self, path: Path, status: str) -> str:
        if path.is_symlink():
            return status + ":symlink:" + os.readlink(path)
        if path.is_file():
            return status + ":" + self._sha256(path)
        return status + ":missing"

    def _load_crate_states(self, crate_name: str | None) -> list[CrateState]:
        if crate_name is not None:
            return [self._load_crate_state(crate_name)]

        manifests = sorted(self.workspace_root.glob("*/vendor/vendors.toml"))
        if not manifests:
            raise VendorUpdateError("no crate-local vendor manifests were found")
        return [self._parse_crate_state(manifest.parent.parent) for manifest in manifests]

    def _load_crate_state(self, crate_name: str) -> CrateState:
        crate_root = self.workspace_root / crate_name
        if not crate_root.is_dir():
            raise VendorUpdateError(f"crate `{crate_name}` does not exist under {self.workspace_root}")
        return self._parse_crate_state(crate_root)

    def _parse_crate_state(self, crate_root: Path) -> CrateState:
        vendor_root = crate_root / "vendor"
        manifest_path = vendor_root / "vendors.toml"
        lock_path = vendor_root / "vendor-lock.toml"
        if not manifest_path.is_file():
            raise VendorUpdateError(f"missing vendor manifest `{manifest_path}`")
        if not lock_path.is_file():
            raise VendorUpdateError(f"missing vendor lock `{lock_path}`")

        manifest = self._read_toml(manifest_path)
        lock = self._read_toml(lock_path)
        configs = self._parse_vendor_configs(crate_root.name, manifest_path, manifest)
        locks = self._parse_vendor_locks(lock_path, lock)

        missing_locks = sorted(set(configs) - set(locks))
        if missing_locks:
            joined = ", ".join(missing_locks)
            raise VendorUpdateError(f"vendor lock `{lock_path}` is missing entries for {joined}")

        return CrateState(
            crate=crate_root.name,
            crate_root=crate_root,
            vendor_root=vendor_root,
            configs=configs,
            locks=locks,
        )

    def _parse_vendor_configs(
        self, crate_name: str, manifest_path: Path, manifest: dict[str, Any]
    ) -> dict[str, VendorConfig]:
        version = manifest.get("version")
        if version != 1:
            raise VendorUpdateError(f"unsupported vendor manifest version `{version}` in {manifest_path}")
        vendors = manifest.get("vendors")
        if not isinstance(vendors, dict) or not vendors:
            raise VendorUpdateError(f"vendor manifest `{manifest_path}` must define [vendors.*] tables")

        configs: dict[str, VendorConfig] = {}
        for name, raw in vendors.items():
            if not isinstance(raw, dict):
                raise VendorUpdateError(f"vendor `{name}` in {manifest_path} must be a table")
            raw_gates = self._require_list(
                raw.get("completion_gates"),
                f"completion_gates for {name} in {manifest_path}",
            )
            completion_gates = tuple(
                tuple(
                    self._require_string(
                        item,
                        f"completion gate entry for {name} in {manifest_path}",
                    )
                    for item in self._require_list(
                        gate,
                        f"completion gate for {name} in {manifest_path}",
                    )
                )
                for gate in raw_gates
            )
            raw_preflight_gates = self._require_list(
                raw.get("preflight_gates", []),
                f"preflight_gates for {name} in {manifest_path}",
            )
            preflight_gates = tuple(
                tuple(
                    self._require_string(
                        item,
                        f"preflight gate entry for {name} in {manifest_path}",
                    )
                    for item in self._require_list(
                        gate,
                        f"preflight gate for {name} in {manifest_path}",
                    )
                )
                for gate in raw_preflight_gates
            )
            configs[name] = VendorConfig(
                crate=crate_name,
                name=name,
                repo=self._require_string(raw.get("repo"), f"repo for {name} in {manifest_path}"),
                source_kind=self._require_string(
                    raw.get("source_kind"), f"source_kind for {name} in {manifest_path}"
                ),
                channel=self._require_string(raw.get("channel"), f"channel for {name} in {manifest_path}"),
                release_tag_prefix=self._optional_string(
                    raw.get("release_tag_prefix"), f"release_tag_prefix for {name} in {manifest_path}"
                ),
                archive_prefix=self._require_string(
                    raw.get("archive_prefix"), f"archive_prefix for {name} in {manifest_path}"
                ),
                upstream_focus_paths=tuple(
                    self._require_string(item, f"upstream focus path for {name} in {manifest_path}")
                    for item in self._require_list(
                        raw.get("upstream_focus_paths"),
                        f"upstream_focus_paths for {name} in {manifest_path}",
                    )
                ),
                local_focus_paths=tuple(
                    self._require_string(item, f"local focus path for {name} in {manifest_path}")
                    for item in self._require_list(
                        raw.get("local_focus_paths"),
                        f"local_focus_paths for {name} in {manifest_path}",
                    )
                ),
                plan_prompt_suffix=self._require_string(
                    raw.get("plan_prompt_suffix"),
                    f"plan_prompt_suffix for {name} in {manifest_path}",
                ),
                preflight_gates=preflight_gates,
                completion_gates=completion_gates,
            )

        return configs

    def _parse_vendor_locks(
        self, lock_path: Path, lock: dict[str, Any]
    ) -> dict[str, VendorLock]:
        version = lock.get("version")
        if version != 1:
            raise VendorUpdateError(f"unsupported vendor lock version `{version}` in {lock_path}")
        vendors = lock.get("vendors")
        if not isinstance(vendors, dict) or not vendors:
            raise VendorUpdateError(f"vendor lock `{lock_path}` must define [vendors.*] tables")

        parsed: dict[str, VendorLock] = {}
        for name, raw in vendors.items():
            if not isinstance(raw, dict):
                raise VendorUpdateError(f"vendor `{name}` in {lock_path} must be a table")
            parsed[name] = VendorLock(
                tag=self._require_string(raw.get("tag"), f"tag for {name} in {lock_path}"),
                archive=self._require_string(raw.get("archive"), f"archive for {name} in {lock_path}"),
                extracted_dir=self._require_string(
                    raw.get("extracted_dir"), f"extracted_dir for {name} in {lock_path}"
                ),
                published_at=self._require_string(
                    raw.get("published_at"), f"published_at for {name} in {lock_path}"
                ),
                sha256=self._require_string(raw.get("sha256"), f"sha256 for {name} in {lock_path}"),
            )
        return parsed

    def _resolve_latest_release(self, config: VendorConfig) -> ReleaseInfo:
        if config.source_kind != "github-release-source":
            raise VendorUpdateError(
                f"vendor `{config.name}` uses unsupported source_kind `{config.source_kind}`"
            )
        if config.channel == "stable":
            latest = self._github_api_release(
                f"repos/{config.repo}/releases/latest",
                description=f"resolve latest release for {config.repo}",
            )
            if not self._is_acceptable_stable_release(config, latest):
                latest = self._find_release_from_list(
                    config,
                    per_page=STABLE_RELEASE_PAGE_SIZE,
                    description=f"resolve latest stable release for {config.repo}",
                    predicate=lambda release: self._is_acceptable_stable_release(config, release),
                )
        elif config.channel == "prerelease":
            latest = self._find_release_from_list(
                config,
                per_page=PRERELEASE_PAGE_SIZE,
                description=f"resolve latest release for {config.repo}",
                predicate=lambda _release: True,
            )
        else:
            raise VendorUpdateError(
                f"vendor `{config.name}` uses unsupported channel `{config.channel}`"
            )
        return ReleaseInfo(
            tag=self._require_string(
                latest.get("tagName"), f"tagName from release payload for {config.repo}"
            ),
            published_at=self._require_string(
                latest.get("publishedAt"), f"publishedAt from release payload for {config.repo}"
            ),
            prerelease=bool(latest.get("isPrerelease")),
        )

    def _find_release_from_list(
        self,
        config: VendorConfig,
        *,
        per_page: int,
        description: str,
        predicate: Callable[[Mapping[str, Any]], bool],
    ) -> dict[str, Any]:
        for page in range(1, MAX_RELEASE_LIST_PAGES + 1):
            payload = self._github_api(
                f"repos/{config.repo}/releases?per_page={per_page}&page={page}",
                description=description,
            )
            if not isinstance(payload, list):
                raise VendorUpdateError(f"{description} returned an unexpected JSON payload")
            if not payload:
                break
            for raw_release in payload:
                if not isinstance(raw_release, dict) or bool(raw_release.get("draft")):
                    continue
                release = self._normalize_release_payload(
                    raw_release,
                    context=f"release list for {config.repo}",
                )
                if predicate(release):
                    return release
            if len(payload) < per_page:
                break
        raise VendorUpdateError(f"no matching releases were returned for {config.repo}")

    def _is_acceptable_stable_release(
        self, config: VendorConfig, release: Mapping[str, Any]
    ) -> bool:
        tag = release.get("tagName")
        if not isinstance(tag, str) or not tag:
            return False
        if bool(release.get("isPrerelease")):
            return False
        return self._is_stable_semver_tag(tag, prefix=config.release_tag_prefix)

    def _is_stable_semver_tag(self, tag: str, *, prefix: str | None) -> bool:
        candidate = tag
        if prefix is not None:
            if not candidate.startswith(prefix):
                return False
            candidate = candidate[len(prefix) :]

        match = SEMVER_TAG_RE.fullmatch(candidate)
        return match is not None and match.group("prerelease") is None

    def _fetch_release_details(self, config: VendorConfig, tag: str) -> dict[str, Any]:
        payload = self._github_api_release(
            f"repos/{config.repo}/releases/tags/{tag}",
            description=f"fetch release metadata for {config.repo}@{tag}",
        )
        self._require_string(
            payload.get("publishedAt"),
            f"publishedAt from release metadata for {config.repo}@{tag}",
        )
        return payload

    def _github_api_release(self, endpoint: str, *, description: str) -> dict[str, Any]:
        payload = self._github_api(endpoint, description=description)
        if not isinstance(payload, dict):
            raise VendorUpdateError(f"{description} returned an unexpected JSON payload")
        return self._normalize_release_payload(payload, context=description)

    def _github_api(self, endpoint: str, *, description: str) -> Any:
        result = self._run(
            ["gh", "api", endpoint],
            cwd=self.workspace_root,
            capture_output=True,
            description=description,
        )
        return json.loads(result.stdout)

    def _normalize_release_payload(self, payload: Mapping[str, Any], *, context: str) -> dict[str, Any]:
        release = {
            "tagName": self._require_string(
                payload.get("tag_name"),
                f"tag_name from GitHub API payload for {context}",
            ),
            "publishedAt": self._require_string(
                payload.get("published_at"),
                f"published_at from GitHub API payload for {context}",
            ),
            "isPrerelease": bool(payload.get("prerelease")),
            "body": payload.get("body") if isinstance(payload.get("body"), str) else "",
            "url": payload.get("html_url") if isinstance(payload.get("html_url"), str) else "",
            "zipballUrl": (
                payload.get("zipball_url") if isinstance(payload.get("zipball_url"), str) else ""
            ),
        }
        return release

    def _ensure_clean_tracked_worktree(self) -> None:
        result = self._run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=self.workspace_root,
            capture_output=True,
            description="check tracked worktree cleanliness",
        )
        if result.stdout.strip():
            raise VendorUpdateError("refusing to update vendors with a dirty tracked worktree")

    def _download_release_archive(self, repo: str, tag: str) -> Path:
        target_root = self.workspace_root / "target"
        target_root.mkdir(parents=True, exist_ok=True)
        temp_dir = Path(tempfile.mkdtemp(prefix="vendor-download-", dir=target_root))
        before = set(temp_dir.iterdir())
        self._run(
            [
                "gh",
                "release",
                "download",
                tag,
                "-R",
                repo,
                "--archive=zip",
                "--dir",
                str(temp_dir),
            ],
            cwd=self.workspace_root,
            description=f"download source archive for {repo}@{tag}",
        )
        created = sorted(path for path in temp_dir.iterdir() if path not in before)
        if len(created) != 1:
            raise VendorUpdateError(
                f"expected a single downloaded archive for {repo}@{tag}, found {len(created)} in {temp_dir}"
            )
        return created[0]

    def _materialize_extract_if_missing(self, archive_path: Path, extract_path: Path) -> None:
        if extract_path.exists():
            return
        self._print(f"materializing {extract_path} from {archive_path.name}")
        self._extract_archive_to_path(archive_path, extract_path)

    def _extract_archive_to_path(self, archive_path: Path, extract_path: Path) -> None:
        if extract_path.exists():
            raise VendorUpdateError(f"refusing to overwrite existing extracted directory `{extract_path}`")
        temp_root = Path(tempfile.mkdtemp(prefix="vendor-extract-", dir=extract_path.parent))
        staging_root = temp_root / "staging"
        staging_root.mkdir()
        try:
            self._safe_extract_zip(archive_path, staging_root)
            extracted_root = self._collapse_archive_root(staging_root)
            shutil.move(str(extracted_root), extract_path)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    def _safe_extract_zip(self, archive_path: Path, destination: Path) -> None:
        with zipfile.ZipFile(archive_path) as archive:
            for member in archive.infolist():
                member_path = PurePosixPath(member.filename)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise VendorUpdateError(
                        f"archive `{archive_path}` contains unsafe path `{member.filename}`"
                    )
                if self._is_archive_metadata_path(member_path):
                    continue
                target_path = destination.joinpath(*member_path.parts)
                if member.is_dir():
                    target_path.mkdir(parents=True, exist_ok=True)
                    continue
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, target_path.open("wb") as sink:
                    shutil.copyfileobj(source, sink)

    def _is_archive_metadata_path(self, path: PurePosixPath) -> bool:
        return "__MACOSX" in path.parts or any(part.startswith("._") for part in path.parts)

    def _collapse_archive_root(self, staging_root: Path) -> Path:
        top_level = list(staging_root.iterdir())
        if len(top_level) == 1 and top_level[0].is_dir():
            return top_level[0]

        collapsed = staging_root / "__collapsed__"
        collapsed.mkdir()
        for path in top_level:
            shutil.move(str(path), collapsed / path.name)
        return collapsed

    def _create_dossier(
        self,
        *,
        crate_state: CrateState,
        config: VendorConfig,
        current_lock: VendorLock,
        latest: ReleaseInfo,
        release_details: dict[str, Any],
        current_extract_path: Path,
        target_archive_path: Path,
        target_extract_path: Path,
        new_sha256: str,
        completion_policy: str,
    ) -> Path:
        dossier_dir = (
            self.workspace_root
            / "target"
            / "vendor-updates"
            / crate_state.crate
            / config.name
            / latest.tag
        )
        dossier_dir.mkdir(parents=True, exist_ok=True)

        diff_stat = self._run(
            [
                "git",
                "diff",
                "--no-index",
                "--stat",
                "--",
                str(current_extract_path),
                str(target_extract_path),
            ],
            cwd=self.workspace_root,
            capture_output=True,
            description=f"generate diff stat for {config.name}",
            accepted_exit_codes=(0, 1),
        ).stdout
        diff_names = self._run(
            [
                "git",
                "diff",
                "--no-index",
                "--name-status",
                "--",
                str(current_extract_path),
                str(target_extract_path),
            ],
            cwd=self.workspace_root,
            capture_output=True,
            description=f"generate changed-file list for {config.name}",
            accepted_exit_codes=(0, 1),
        ).stdout

        metadata = {
            "crate": crate_state.crate,
            "vendor": config.name,
            "repo": config.repo,
            "source_kind": config.source_kind,
            "channel": config.channel,
            "current": {
                "tag": current_lock.tag,
                "archive": current_lock.archive,
                "extracted_dir": current_lock.extracted_dir,
                "published_at": current_lock.published_at,
                "sha256": current_lock.sha256,
            },
            "target": {
                "tag": latest.tag,
                "archive": target_archive_path.name,
                "extracted_dir": target_extract_path.name,
                "published_at": release_details["publishedAt"],
                "sha256": new_sha256,
            },
            "upstream_focus_paths": list(config.upstream_focus_paths),
            "local_focus_paths": list(config.local_focus_paths),
            "completion_policy": completion_policy,
            "preflight_gates": [list(gate) for gate in config.preflight_gates],
            "completion_gates": [list(gate) for gate in config.completion_gates],
        }
        (dossier_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        (dossier_dir / "release.json").write_text(json.dumps(release_details, indent=2) + "\n")
        (dossier_dir / "upstream_diff_stat.txt").write_text(diff_stat)
        (dossier_dir / "upstream_changed_files.txt").write_text(diff_names)

        dossier_md = textwrap.dedent(
            f"""
            # Vendor Update Dossier

            - Crate: `{crate_state.crate}`
            - Vendor: `{config.name}`
            - Repo: `{config.repo}`
            - Current tag: `{current_lock.tag}` (`{current_lock.published_at}`)
            - Target tag: `{latest.tag}` (`{release_details["publishedAt"]}`)
            - Current archive: `{current_lock.archive}`
            - Target archive: `{target_archive_path.name}`
            - Current extracted dir: `{current_lock.extracted_dir}`
            - Target extracted dir: `{target_extract_path.name}`
            - Target sha256: `{new_sha256}`
            - Completion policy: `{completion_policy}`

            ## Upstream Focus Paths

            {self._bullet_list(config.upstream_focus_paths)}

            ## Local Focus Paths

            {self._bullet_list(config.local_focus_paths)}

            ## Completion Gates

            {self._bullet_list(" ".join(gate) for gate in config.completion_gates)}

            ## Preflight Gates

            {self._bullet_list(" ".join(gate) for gate in config.preflight_gates)}

            ## Prompt Suffix

            {config.plan_prompt_suffix}

            ## Upstream Diff Stat

            ```text
            {diff_stat.strip()}
            ```

            ## Upstream Changed Files

            ```text
            {diff_names.strip()}
            ```

            ## Release Metadata

            See `release.json` and `metadata.json` in this directory.
            """
        ).strip()
        (dossier_dir / "dossier.md").write_text(dossier_md + "\n")
        return dossier_dir

    def _run_codex_planning(
        self,
        crate_state: CrateState,
        config: VendorConfig,
        dossier_dir: Path,
        plan_path: Path,
        *,
        completion_policy: str,
    ) -> None:
        vendor_specific_focus = config.plan_prompt_suffix.strip()
        policy_guidance = self._completion_policy_guidance(
            completion_policy,
            dossier_dir,
        )
        prompt = textwrap.dedent(
            f"""
            Use the `vendor-release-update` skill.

            Read `{dossier_dir / "dossier.md"}` and produce a decision-complete markdown update plan
            for adapting `{crate_state.crate}` to the new `{config.name}` vendor release.
            Save the final markdown response to `{plan_path}`.
            - Do not modify files.
            - Follow planning mode from the skill and the repository or crate AGENTS guidance it references.
            - Do not use collaboration sub-agents or delegate work; complete the dossier analysis in this Codex execution.
            {policy_guidance}
            - Apply this vendor-specific focus: {vendor_specific_focus}
            """
        ).strip()
        self._run(
            [
                "codex",
                "exec",
                "--cd",
                str(self.workspace_root),
                "--sandbox",
                "read-only",
                "--output-last-message",
                str(plan_path),
                prompt,
            ],
            cwd=self.workspace_root,
            description=f"plan {crate_state.crate}/{config.name} update",
            timeout_seconds=self.codex_planning_timeout_seconds,
        )

    def _run_codex_implementation(
        self,
        crate_state: CrateState,
        config: VendorConfig,
        dossier_dir: Path,
        plan_path: Path,
        *,
        completion_policy: str,
        coordinated_source: Path | None = None,
        prerequisite_source: Path | None = None,
    ) -> None:
        output_path = dossier_dir / "implementation.md"
        vendor_specific_focus = config.plan_prompt_suffix.strip()
        policy_guidance = self._completion_policy_guidance(
            completion_policy,
            dossier_dir,
        )
        if coordinated_source is not None:
            policy_guidance += (
                "\nExplicitly authorized coordinated Agent campaign: both targets are selected in the lock. "
                "Adapt only this vendor and run focused checks. Do not run the full strict-live or "
                "workspace completion gates inside this implementation: the parent must review both "
                "vendor diffs before invoking the common strict gates with --validation-only. "
                "This sequencing amendment supersedes single-vendor advancement instructions in retained plans. "
                f"The current coordinated authority is `{dossier_dir.parent / 'resume.json'}` and "
                f"`{self.workspace_root / '.agents/skills/vendor-refresh-campaign/references/workflow.md'}`. "
                f"Resolve relative historical references missing from the copied dossier against "
                f"the original imported dossier `{coordinated_source}` as read-only evidence. "
                "Historical results and service-state assumptions are not current acceptance or runtime authority. "
                "Do not write to the original imported dossier or resume its single-vendor workflow. "
                "Keep all frozen inputs unchanged, including copied/original plans, metadata, and lock snapshots. "
                f"New evidence belongs only under `{dossier_dir}`; implementation edits must stay within "
                "the coordinated permitted scope and this vendor's adaptation. "
                "Do not weaken assertions, substitute models, enlarge deadlines, normalize outputs, or retry until pass."
            )
        if prerequisite_source is not None:
            policy_guidance += (
                "\nThis leaf imports unchanged frozen inputs after one scoped prerequisite commit. "
                f"The current recovery authority is `{dossier_dir / 'resume.json'}`. "
                f"Resolve historical references against `{prerequisite_source}` as read-only evidence. "
                "Do not write to the original imported dossier or resume its workflow. "
                "Keep original and copied plans, metadata, release inputs, and lock snapshots unchanged. "
                f"New evidence belongs only under `{dossier_dir}`; retain the original vendor change scope. "
                "Historical passes do not replace current strict preflight and completion gates."
            )
        prompt = textwrap.dedent(
            f"""
            Use the `vendor-release-update` skill.

            Read `{dossier_dir / "dossier.md"}` and `{plan_path}`, then execute the plan for
            `{crate_state.crate}` and the new `{config.name}` vendor release.
            - Follow execution mode from the skill and the repository or crate AGENTS guidance it references.
            - Do not clean old vendor artifacts; the updater will handle cleanup after the gates pass.
            - Do not create commits or push branches; the updater will do that after validation.
            - Leave the worktree ready for the completion gates listed in the dossier.
            - Do not use collaboration sub-agents or delegate work; complete the dossier implementation in this Codex execution.
            {policy_guidance}
            - Apply this vendor-specific focus: {vendor_specific_focus}
            """
        ).strip()
        self._run(
            [
                "codex",
                "exec",
                "--cd",
                str(self.workspace_root),
                "--sandbox",
                "workspace-write",
                "--output-last-message",
                str(output_path),
                prompt,
            ],
            cwd=self.workspace_root,
            description=f"implement {crate_state.crate}/{config.name} update",
            timeout_seconds=self.codex_implementation_timeout_seconds,
        )

    def _run_completion_gates(self, config: VendorConfig) -> None:
        for gate in config.completion_gates:
            self._run(
                list(gate),
                cwd=self.workspace_root,
                description=f"run gate: {' '.join(gate)}",
            )

    def _run_preflight_gates(self, config: VendorConfig) -> None:
        for gate in config.preflight_gates:
            self._run(
                list(gate),
                cwd=self.workspace_root,
                description=f"run preflight gate: {' '.join(gate)}",
            )

    def _observe_completion_gates(
        self,
        config: VendorConfig,
        *,
        dossier_dir: Path,
        stage: str,
    ) -> tuple[CompletionGateRun, ...]:
        runs: list[CompletionGateRun] = []
        for index, gate in enumerate(config.completion_gates, start=1):
            result = self._run(
                list(gate),
                cwd=self.workspace_root,
                capture_output=True,
                description=f"run {stage} gate: {' '.join(gate)}",
                accepted_exit_codes=None,
            )
            run = CompletionGateRun(
                command=gate,
                returncode=result.returncode,
                stdout=result.stdout or "",
                stderr=result.stderr or "",
                signatures=self._completion_gate_signatures(result),
            )
            runs.append(run)
            self._write_completion_gate_logs(
                dossier_dir=dossier_dir,
                stage=stage,
                index=index,
                run=run,
            )
        return tuple(runs)

    def _completion_gate_signatures(
        self,
        result: subprocess.CompletedProcess[str],
    ) -> tuple[str, ...]:
        if result.returncode == 0:
            return ()

        combined = ANSI_ESCAPE_RE.sub("", f"{result.stdout or ''}\n{result.stderr or ''}")
        lines = combined.splitlines()
        signatures = set(self._test_failure_signatures(lines))
        for index, raw_line in enumerate(lines):
            line = raw_line.strip()
            format_match = FORMAT_DIFF_RE.match(line)
            if format_match is not None:
                signatures.add(
                    f"format:{self._normalized_gate_path(format_match.group('path'))}"
                )
                continue

            failed_test_match = FAILED_TEST_RE.match(line)
            if failed_test_match is not None:
                signatures.add(f"test:{failed_test_match.group('name')}")
                continue

            error_match = ERROR_LINE_RE.match(line)
            if error_match is not None:
                message = self._normalized_gate_text(error_match.group("message"))
                source_path = None
                for candidate in lines[index + 1 : index + 8]:
                    source_match = SOURCE_LOCATION_RE.match(candidate)
                    if source_match is not None:
                        source_path = self._normalized_gate_path(source_match.group("path"))
                        break
                if source_path is None:
                    signatures.add(f"error:{message}")
                else:
                    signatures.add(f"rust:{source_path}:{message}")
                continue

            if SHELL_FAILURE_RE.search(line):
                signatures.add(f"shell:{self._normalized_gate_text(line)}")

        if signatures:
            return tuple(sorted(signatures))

        normalized_output = self._normalized_gate_output(combined)
        digest = hashlib.sha256(normalized_output.encode("utf-8")).hexdigest()
        return (f"opaque:{digest}",)

    def _test_failure_signatures(self, lines: list[str]) -> tuple[str, ...]:
        signatures: set[str] = set()
        for index, raw_line in enumerate(lines):
            panic_match = PANIC_HEADER_RE.match(raw_line.strip())
            if panic_match is None:
                continue
            message = next(
                (
                    candidate.strip()
                    for candidate in lines[index + 1 :]
                    if candidate.strip()
                    and not candidate.strip().startswith("note: run with")
                ),
                None,
            )
            if message is not None:
                signatures.add(
                    "test-error:"
                    f"{panic_match.group('name')}:"
                    f"{self._normalized_gate_text(message)}"
                )

        for index, raw_line in enumerate(lines):
            header_match = TEST_FAILURE_HEADER_RE.match(raw_line.strip())
            if header_match is None:
                continue

            section: list[str] = []
            for candidate in lines[index + 1 :]:
                stripped = candidate.strip()
                if TEST_FAILURE_HEADER_RE.match(stripped) is not None:
                    break
                if stripped in {"failures:", "test result:"} or stripped.startswith(
                    "test result:"
                ):
                    break
                section.append(stripped)

            message = None
            for section_index, candidate in enumerate(section):
                if " panicked at " not in candidate:
                    continue
                for following in section[section_index + 1 :]:
                    if following and not following.startswith("note: run with"):
                        message = following
                        break
                break
            if message is None:
                message = next(
                    (
                        candidate
                        for candidate in section
                        if candidate.lower().startswith("error:")
                    ),
                    None,
                )
            if message is not None:
                signatures.add(
                    "test-error:"
                    f"{header_match.group('name')}:"
                    f"{self._normalized_gate_text(message)}"
                )
        return tuple(sorted(signatures))

    def _reject_baseline_prerequisite_failures(
        self,
        baseline: tuple[CompletionGateRun, ...],
    ) -> None:
        failures = [
            f"gate-{index:02d}:{signature}"
            for index, run in enumerate(baseline, start=1)
            for signature in run.signatures
            if signature.startswith("shell:")
        ]
        if failures:
            raise VendorUpdateError(
                "baseline completion gates reported unavailable prerequisites: "
                + ", ".join(failures)
            )

    def _normalized_gate_path(self, value: str) -> str:
        normalized = value.strip().replace(str(self.workspace_root), "<workspace>")
        if normalized.startswith("<workspace>/"):
            return normalized.removeprefix("<workspace>/")
        return normalized

    def _normalized_gate_text(self, value: str) -> str:
        normalized = value.replace(str(self.workspace_root), "<workspace>")
        normalized = re.sub(r":\d+(?::\d+)?\b", ":<location>", normalized)
        normalized = re.sub(r"\b\d+(?:\.\d+)?s\b", "<duration>", normalized)
        normalized = re.sub(
            r"\b(?P<prefix>aborting|failed|due to) \d+ previous errors?\b",
            lambda match: f"{match.group('prefix')} <count> previous errors",
            normalized,
        )
        normalized = re.sub(
            r"\b\d+ warnings? emitted\b",
            "<count> warnings emitted",
            normalized,
        )
        return " ".join(normalized.split())

    def _normalized_gate_output(self, value: str) -> str:
        stable_lines: list[str] = []
        for raw_line in value.splitlines():
            line = self._normalized_gate_text(raw_line)
            if not line:
                continue
            if line.startswith(("Checking ", "Compiling ", "Finished ", "Running ")):
                continue
            stable_lines.append(line)
        return "\n".join(stable_lines)

    def _compare_completion_gate_runs(
        self,
        baseline: tuple[CompletionGateRun, ...],
        post: tuple[CompletionGateRun, ...],
    ) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...]]:
        if len(baseline) != len(post):
            raise VendorUpdateError("baseline and post completion gate counts differ")

        comparisons: list[dict[str, Any]] = []
        regressions: list[str] = []
        for index, (baseline_run, post_run) in enumerate(zip(baseline, post), start=1):
            if baseline_run.command != post_run.command:
                raise VendorUpdateError(
                    f"baseline and post commands differ for completion gate {index}"
                )

            new_signatures: tuple[str, ...] = ()
            if baseline_run.returncode == 0 and post_run.returncode == 0:
                outcome = "unchanged-pass"
            elif baseline_run.returncode != 0 and post_run.returncode == 0:
                outcome = "improved"
            elif baseline_run.returncode == 0:
                outcome = "regressed"
                regressions.append(f"gate-{index:02d}:pass-to-fail")
            else:
                baseline_signatures = set(baseline_run.signatures)
                new_signatures = tuple(
                    sorted(set(post_run.signatures) - baseline_signatures)
                )
                if new_signatures:
                    outcome = "regressed"
                    regressions.extend(new_signatures)
                else:
                    outcome = "pre-existing-failure"

            comparisons.append(
                {
                    "gate": index,
                    "command": list(baseline_run.command),
                    "baseline_returncode": baseline_run.returncode,
                    "post_returncode": post_run.returncode,
                    "outcome": outcome,
                    "new_signatures": list(new_signatures),
                }
            )
        return tuple(comparisons), tuple(regressions)

    def _write_completion_gate_logs(
        self,
        *,
        dossier_dir: Path,
        stage: str,
        index: int,
        run: CompletionGateRun,
    ) -> None:
        report_dir = dossier_dir / "completion-gates"
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / f"{stage}-{index:02d}.stdout.log").write_text(run.stdout)
        (report_dir / f"{stage}-{index:02d}.stderr.log").write_text(run.stderr)

    def _write_completion_gate_report(
        self,
        *,
        dossier_dir: Path,
        baseline: tuple[CompletionGateRun, ...],
        post: tuple[CompletionGateRun, ...],
        comparisons: tuple[dict[str, Any], ...],
        regressions: tuple[str, ...],
    ) -> None:
        report_dir = dossier_dir / "completion-gates"
        report_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "policy": "no-regression",
            "baseline": [self._completion_gate_run_payload(run) for run in baseline],
            "post": [self._completion_gate_run_payload(run) for run in post],
            "comparisons": list(comparisons),
            "regressions": list(regressions),
        }
        (report_dir / "report.json").write_text(json.dumps(payload, indent=2) + "\n")

    def _completion_gate_run_payload(self, run: CompletionGateRun) -> dict[str, Any]:
        return {
            "command": list(run.command),
            "returncode": run.returncode,
            "signatures": list(run.signatures),
            "stdout_sha256": hashlib.sha256(run.stdout.encode("utf-8")).hexdigest(),
            "stderr_sha256": hashlib.sha256(run.stderr.encode("utf-8")).hexdigest(),
        }

    def _completion_policy_guidance(
        self,
        completion_policy: str,
        dossier_dir: Path,
    ) -> str:
        if completion_policy == "strict":
            return "- Treat every configured completion gate as blocking."
        return (
            f"- Read `{dossier_dir / 'completion-gates' / 'report.json'}` before deciding scope. "
            "Baseline failures are not an implementation queue. Limit changes to confirmed vendor "
            "release drift and mechanical cleanup in touched paths; do not audit or implement "
            "unrelated specification backlog, add public API, or change unrelated backends/crates."
        )

    def _write_vendor_lock(self, path: Path, locks: Mapping[str, VendorLock]) -> None:
        lines = ["version = 1", ""]
        for name in sorted(locks):
            lock = locks[name]
            lines.extend(
                [
                    f"[vendors.{name}]",
                    f'tag = "{lock.tag}"',
                    f'archive = "{lock.archive}"',
                    f'extracted_dir = "{lock.extracted_dir}"',
                    f'published_at = "{lock.published_at}"',
                    f'sha256 = "{lock.sha256}"',
                    "",
                ]
            )
        path.write_text("\n".join(lines).rstrip() + "\n")

    def _branch_name(self, crate_name: str, vendor_name: str, tag: str) -> str:
        return f"codex/vendor-{self._slug(crate_name)}-{self._slug(vendor_name)}-{self._slug(tag)}"

    def _current_branch_name(self) -> str:
        result = self._run(
            ["git", "branch", "--show-current"],
            cwd=self.workspace_root,
            capture_output=True,
            description="detect current branch",
        )
        branch_name = result.stdout.strip()
        if not branch_name:
            raise VendorUpdateError("refusing current-branch vendor update from a detached HEAD")
        return branch_name

    def _slug(self, value: str) -> str:
        return "".join(character if character.isalnum() or character in "._-" else "-" for character in value)

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _read_toml(self, path: Path) -> dict[str, Any]:
        with path.open("rb") as handle:
            return tomllib.load(handle)

    def _require_list(self, value: Any, context: str) -> list[Any]:
        if not isinstance(value, list):
            raise VendorUpdateError(f"expected a list for {context}")
        return value

    def _require_string(self, value: Any, context: str) -> str:
        if not isinstance(value, str) or not value:
            raise VendorUpdateError(f"expected a non-empty string for {context}")
        return value

    def _optional_string(self, value: Any, context: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise VendorUpdateError(f"expected a non-empty string for {context}")
        return value

    def _bullet_list(self, items: Sequence[str] | Any) -> str:
        lines = [f"- `{item}`" for item in items]
        return "\n".join(lines) if lines else "- none"

    def _timeout_from_env(self, name: str, default: float) -> float:
        raw = self.env.get(name)
        if raw is None:
            return default
        try:
            timeout = float(raw)
        except ValueError as exc:
            raise VendorUpdateError(f"invalid timeout `{raw}` in {name}") from exc
        if timeout <= 0:
            raise VendorUpdateError(f"timeout `{name}` must be greater than zero")
        return timeout

    def _run(
        self,
        args: Sequence[str],
        *,
        cwd: Path,
        capture_output: bool = False,
        description: str,
        accepted_exit_codes: tuple[int, ...] | None = (0,),
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self._print(f"==> {description}")
        process = subprocess.Popen(
            list(args), cwd=cwd, env=self.env, text=True,
            stdout=subprocess.PIPE if capture_output else None,
            stderr=subprocess.PIPE if capture_output else None,
            start_new_session=os.name == "posix",
        )
        try:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
        except BaseException as exc:
            try:
                self._stop_process(process)
            except BaseException as cleanup_error:
                note = (
                    f"Owned process cleanup failed ({type(cleanup_error).__name__}); "
                    "descendant termination is unverified."
                )
                if isinstance(exc, subprocess.TimeoutExpired):
                    raise VendorUpdateError(
                        f"{description} timed out after {timeout_seconds:g}s; {note}"
                    ) from cleanup_error
                exc.add_note(note)
                raise exc from cleanup_error
            if isinstance(exc, subprocess.TimeoutExpired):
                raise VendorUpdateError(
                    f"{description} timed out after {timeout_seconds:g}s"
                ) from exc
            raise
        result = subprocess.CompletedProcess(list(args), process.returncode, stdout, stderr)
        if accepted_exit_codes is not None and result.returncode not in accepted_exit_codes:
            stderr = result.stderr.strip() if result.stderr else ""
            stdout = result.stdout.strip() if result.stdout else ""
            detail = stderr or stdout or f"exit status {result.returncode}"
            raise VendorUpdateError(f"{description} failed: {detail}")
        return result

    def _stop_process(self, process: subprocess.Popen[str]) -> None:
        # Codex can own children that also write to the checkout. Reap its whole
        # process group before restoring the selected lock after interruption.
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        elif process.poll() is None:
            process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        finally:
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif process.poll() is None:
                process.kill()
            process.communicate()

    def _interrupt(self, signum: int, frame: Any) -> None:
        raise KeyboardInterrupt(f"vendor update interrupted by signal {signum}")

    def _print(self, message: str) -> None:
        if self.suppress_progress:
            return
        print(message, file=self.stdout)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check and update vendored upstream release archives.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="check for available vendor updates")
    check_parser.add_argument("--crate", dest="crate_name")
    check_parser.add_argument("--json", dest="json_output", action="store_true")

    update_parser = subparsers.add_parser("update", help="update a single configured vendor")
    update_parser.add_argument("--crate", dest="crate_name", required=True)
    update_parser.add_argument("--vendor", dest="vendor_name", required=True)
    update_parser.add_argument(
        "--git-mode",
        dest="git_mode",
        choices=("managed", "current-branch"),
        default="managed",
    )
    update_parser.add_argument(
        "--completion-policy",
        choices=COMPLETION_POLICIES,
        default="strict",
    )
    resume_parser = subparsers.add_parser("resume", help="resume a prepared vendor dossier")
    resume_parser.add_argument("--dossier", type=Path, required=True)
    phase = resume_parser.add_mutually_exclusive_group()
    phase.add_argument("--implementation-only", action="store_true")
    phase.add_argument("--validation-only", action="store_true")
    coordinate_parser = subparsers.add_parser("coordinate", help="prepare an explicitly authorized strict Agent vendor pair")
    coordinate_parser.add_argument("--dossier", type=Path, required=True)
    coordinate_parser.add_argument("--vendor", dest="vendor_name", choices=("opencode",), required=True)
    coordinate_parser.add_argument("--target-tag", required=True)
    coordinate_parser.add_argument("--plan", type=Path, required=True)
    prerequisite_parser = subparsers.add_parser(
        "import-prerequisite", help="prepare a frozen strict leaf after one scoped prerequisite commit")
    prerequisite_parser.add_argument("--dossier", type=Path, required=True)
    prerequisite_parser.add_argument("--destination", type=Path, required=True)
    prerequisite_parser.add_argument("--commit", required=True)
    prerequisite_parser.add_argument("--path", dest="paths", action="append", required=True)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    workspace_root: Path | None = None,
    env: Mapping[str, str] | None = None,
    stdout: Any = None,
    stderr: Any = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    updater = VendorUpdater(
        workspace_root or WORKSPACE_ROOT,
        env=env,
        stdout=stdout,
        stderr=stderr,
    )

    previous_termination_handler = signal.signal(signal.SIGTERM, updater._interrupt)
    try:
        if args.command == "check":
            return updater.check(args.crate_name, json_output=args.json_output)
        if args.command == "update":
            return updater.update(
                args.crate_name,
                args.vendor_name,
                git_mode=args.git_mode,
                completion_policy=args.completion_policy,
            )
        if args.command == "resume":
            return updater.resume(args.dossier, implementation_only=args.implementation_only,
                                  validation_only=args.validation_only)
        if args.command == "coordinate":
            updater.coordinate(args.dossier, args.vendor_name, args.target_tag, args.plan)
            return 0
        if args.command == "import-prerequisite":
            updater.import_prerequisite(args.dossier, args.destination, args.commit, args.paths)
            return 0
        parser.error(f"unknown command `{args.command}`")
        return 2
    except VendorUpdateError as exc:
        print(f"error: {exc}", file=stderr or sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_termination_handler)


if __name__ == "__main__":
    raise SystemExit(main())

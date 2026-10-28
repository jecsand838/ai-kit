#!/usr/bin/env python3
"""Explicit, source-bound continuation receipts for canonical release stages.

The owning shell scripts remain the gate inventory. An unknown stage executes;
only a checked, immutable receipt can supply REUSED evidence. No receipt changes
a command's acceptance rules or authorizes a failed/incomplete stage to pass.
"""

import argparse
from collections import Counter
import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time
import tomllib
import uuid

if __package__:
    from .security_dependencies import SecurityPolicy
else:
    from security_dependencies import SecurityPolicy


CRATES = ("providers-kit", "storage-kit", "tools-kit", "agents-kit", "app-kit")
SUMMARY = re.compile(r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out", re.M)
SECRET_NAME = re.compile(r"KEY|TOKEN|PASSWORD|SECRET|CREDENTIAL|AUTH|ENV_JSON|ENVIRONMENT", re.I)
BUILD_NAMES = ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTDOCFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS")
COMMAND_ENV_NAMES = (*BUILD_NAMES, "CARGO_TARGET_DIR")


class EvidenceError(ValueError):
    """A receipt or invocation cannot establish the declared stage."""


class JsonFile:
    @staticmethod
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise EvidenceError("evidence JSON contains a duplicate field")
            result[key] = value
        return result

    @staticmethod
    def read(path):
        with Path(path).open() as source:
            result = json.load(source, object_pairs_hook=JsonFile.unique_pairs)
        if not isinstance(result, dict):
            raise EvidenceError("receipt must be a JSON object")
        return result

    @staticmethod
    def write(path, value):
        path = Path(path)
        temporary = path.with_name(path.name + ".tmp")
        with temporary.open("x") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
        os.replace(temporary, path)

    @staticmethod
    def digest(path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    @classmethod
    def verified(cls, reference):
        if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
            raise EvidenceError("artifact reference requires exactly path and sha256")
        if not isinstance(reference["path"], str) or not isinstance(reference["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", reference["sha256"]):
            raise EvidenceError("artifact reference has an invalid path or hash")
        path = Path(reference["path"])
        if not path.is_absolute() or not path.is_file():
            raise EvidenceError("referenced evidence file is missing or not absolute")
        if cls.digest(path) != reference["sha256"]:
            raise EvidenceError("referenced evidence hash differs")
        return path


class Compiler:
    @staticmethod
    def read(root):
        result = subprocess.run(["rustc", "--version", "--verbose"], cwd=root,
                                capture_output=True, text=True, timeout=30, check=True)
        if not result.stdout.strip():
            raise EvidenceError("compiler identity is empty")
        return result.stdout.strip()


class Command:
    @staticmethod
    def unwrap(arguments):
        if not isinstance(arguments, list) or not arguments or any(not isinstance(arg, str) or not arg or "\0" in arg for arg in arguments):
            raise EvidenceError("command must be a nonempty argument array")
        if Path(arguments[0]).name != "env":
            return arguments, {}
        assignments = {}
        index = 1
        while index < len(arguments) and "=" in arguments[index]:
            name, value = arguments[index].split("=", 1)
            if name not in COMMAND_ENV_NAMES or name in assignments:
                raise EvidenceError("env prefix requires unique, explicitly allowed build settings")
            if name in ("CARGO_INCREMENTAL", "CARGO_BUILD_JOBS") and value != {"CARGO_INCREMENTAL": "0", "CARGO_BUILD_JOBS": "1"}[name]:
                raise EvidenceError("env prefix cannot change the sequential release build settings")
            assignments[name] = value
            index += 1
        if index == 1 or index == len(arguments) or arguments[index].startswith("-"):
            raise EvidenceError("env prefix requires assignments followed by a command")
        return arguments[index:], assignments

    @staticmethod
    def environment(arguments):
        _, assignments = Command.unwrap(arguments)
        return dict(os.environ, CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1") | assignments

    @staticmethod
    def doctest(arguments):
        inner, _ = Command.unwrap(arguments)
        before = inner[:inner.index("--")] if "--" in inner else inner
        return len(before) > 1 and Path(before[0]).name == "cargo" and before[1] == "test" and "--doc" in before

    @staticmethod
    def workspace_doctest(arguments):
        if not Command.doctest(arguments):
            return False
        inner, _ = Command.unwrap(arguments)
        before = inner[:inner.index("--")] if "--" in inner else inner
        if any(item == "--manifest-path" or item.startswith("--manifest-path=") for item in before):
            return False
        packages = Command.values(before, "-p", "--package")
        packages.extend(item[2:].removeprefix("=") for item in before if item.startswith("-p") and item != "-p")
        return set(packages) <= set(CRATES)

    @staticmethod
    def identity_rules(arguments):
        inner, _ = Command.unwrap(arguments)
        rules = ["cargo-option-order"] if Path(inner[0]).name == "cargo" else ["exact-argv"]
        if Command.workspace_doctest(arguments):
            rules.append("workspace-doctest-build-output-directory")
        return rules

    @staticmethod
    def canonical(arguments):
        inner, assignments = Command.unwrap(arguments)
        if assignments:
            if Command.workspace_doctest(inner):
                assignments = {name: value for name, value in assignments.items() if name != "CARGO_TARGET_DIR"}
            prefix = [arguments[0], *[f"{name}={value}" for name, value in sorted(assignments.items())]] if assignments else []
            return [*prefix, *Command.canonical(inner)]
        if Path(arguments[0]).name != "cargo" or len(arguments) < 2:
            return arguments
        before, separator, after = arguments, [], []
        if "--" in arguments:
            index = arguments.index("--")
            before, separator, after = arguments[:index], ["--"], arguments[index + 1:]
        # Canonicalize Cargo option ordering, not behavior or feature selection.
        valued = {"-p": "--package", "--package": "--package", "--test": "--test", "--example": "--example",
                  "--bin": "--bin", "--bench": "--bench", "--exclude": "--exclude", "--features": "--features",
                  "--profile": "--profile", "--target": "--target", "--target-dir": "--target-dir", "--manifest-path": "--manifest-path"}
        options, positional = [], []
        index = 2
        while index < len(before):
            argument = before[index]
            name, equal, value = argument.partition("=")
            if name in valued:
                if not equal:
                    index += 1
                    if index >= len(before):
                        raise EvidenceError("Cargo option is missing its value")
                    value = before[index]
                if name == "--features":
                    value = ",".join(sorted(filter(None, re.split(r"[, ]+", value))))
                # Workspace doctests do not observe Cargo build-output paths.
                # Keep publication builds and ordinary integration targets exact.
                if name != "--target-dir" or not Command.workspace_doctest(arguments):
                    options.append((valued[name], value))
            elif argument.startswith("-"):
                options.append((argument,))
            else:
                positional.append(argument)
            index += 1
        return [arguments[0], arguments[1], *[part for option in sorted(options) for part in option], *positional, *separator, *after]

    @staticmethod
    def values(arguments, *names):
        arguments, _ = Command.unwrap(arguments)
        result = []
        for index, argument in enumerate(arguments):
            if argument in names and index + 1 < len(arguments):
                result.append(arguments[index + 1])
            elif any(argument.startswith(name + "=") for name in names):
                result.append(argument.partition("=")[2])
        return result

    @staticmethod
    def kind(arguments):
        arguments, _ = Command.unwrap(arguments)
        if Path(arguments[0]).name == "cargo" and len(arguments) > 1:
            if arguments[1] == "test" and "--no-run" not in arguments:
                return "inventory" if "--list" in arguments else "tests"
            return "command"
        if ("unittest" in arguments or any("contract.py" in item for item in arguments)) and Path(arguments[0]).name.startswith("python"):
            return "python-tests"
        return "command"

    @staticmethod
    def build_names(arguments):
        inner, _ = Command.unwrap(arguments)
        if Path(inner[0]).name == "cargo" and len(inner) > 1:
            if inner[1] == "fmt":
                return ()
            explicit_targets = any(item in inner for item in ("--test", "--lib", "--bin", "--bins", "--tests", "--example", "--examples", "--bench", "--benches", "--all-targets", "--no-run"))
            possible_doctests = inner[1] == "test" and not explicit_targets
            if inner[1] != "doc" and "--doc" not in inner and not possible_doctests:
                return tuple(name for name in BUILD_NAMES if name != "RUSTDOCFLAGS")
        return BUILD_NAMES

    @staticmethod
    def fresh(stage_id, arguments):
        inner, _ = Command.unwrap(arguments)
        return (stage_id in ("agent.precheck", "app.precheck")
                or any(Path(item).name == "precheck_strict_live.sh" for item in inner)
                or stage_id.startswith("app.prepare.")
                or "clean-checkout" in stage_id or "clean_checkout" in stage_id
                or "prepare_contract" in inner
                or Path(inner[0]).name == "cargo" and len(inner) > 1 and inner[1] == "doc")

    @staticmethod
    def allow_empty(arguments):
        inner, _ = Command.unwrap(arguments)
        return ("--lib" in inner or "--doc" in inner
                or Command.values(inner, "-p", "--package") == ["app-kit"]
                and Command.values(inner, "--test") == ["compile_contract"])

    @staticmethod
    def supervised_parent(arguments):
        packages, targets = Command.values(arguments, "-p", "--package"), Command.values(arguments, "--test")
        if packages == ["app-kit"] and targets == ["prepare_contract"]:
            return "app-prepare"
        if packages == ["storage-kit"] and targets == ["lancedb_state_store"]:
            return "storage-lance"
        return "integration" if len(targets) == 1 else None

    @staticmethod
    def live(stage_id, arguments):
        return ".live." in stage_id or any(target in ("strict_live", "workflow_live") for target in Command.values(arguments, "--test"))

    @staticmethod
    def timeout(stage_id, arguments):
        # Provider's authored batch cases can span several consecutive 24h jobs.
        return 10 * 24 * 3600 if stage_id.startswith("provider.live.") else 24 * 3600

    @staticmethod
    def listing(arguments):
        inner, assignments = Command.unwrap(arguments)
        if assignments:
            return [arguments[0], *[f"{name}={value}" for name, value in assignments.items()], *Command.listing(inner)]
        before = arguments[:arguments.index("--")] if "--" in arguments else arguments
        after = arguments[arguments.index("--") + 1:] if "--" in arguments else []
        selection = []
        index = 0
        while index < len(after):
            argument = after[index]
            if argument in ("--nocapture", "--show-output", "--quiet") or argument.startswith(("--test-threads=", "--color=", "--format=")):
                pass
            elif argument in ("--test-threads", "--color", "--format"):
                index += 1
            else:
                selection.append(argument)
            index += 1
        return [*before, "--", *selection, "--list", "--format", "terse"]


class SourceIdentity:
    @staticmethod
    def security_inputs(root):
        try:
            return set(SecurityPolicy(root).inputs())
        except (OSError, ValueError) as error:
            raise EvidenceError(f"security exception policy: {error}") from error

    @staticmethod
    def doctest_output_independence(root, paths):
        # Admission for the narrow current-workspace directory equivalence. A
        # future doctest observing its executable/build environment needs a new
        # review; it cannot silently inherit the present source audit.
        observation = re.compile(r"CARGO_TARGET|target/(?:debug|release)|target[-_]dir|current_exe|(?:std::)?env::(?:var|vars|args)|/proc/self/exe|(?:option_)?env!|use\s+std::env(?:\s|;|::\{)")
        for relative in paths:
            if (relative.endswith(".rs") and any(relative.startswith(crate + "/src/") for crate in CRATES)
                    and observation.search((root / relative).read_text())):
                raise EvidenceError("workspace doctest build-output equivalence needs renewed source review: " + relative)

    @staticmethod
    def _relative(root, path):
        path = Path(path)
        absolute = path if path.is_absolute() else root / path
        try:
            relative = absolute.relative_to(root)
            absolute.resolve().relative_to(root.resolve())
        except ValueError as error:
            raise EvidenceError("source scope escapes the workspace") from error
        if not relative.parts or relative.parts[0] in ("tmp", "target", ".git"):
            raise EvidenceError("historical, generated, or Git metadata is not a source input")
        if absolute.is_symlink():
            raise EvidenceError("source symlinks require an explicit resolved preparation proof")
        return relative.as_posix()

    @classmethod
    def _tree(cls, root, directory):
        directory = root / directory
        if not directory.is_dir():
            return set()
        result = set()
        for current, directories, files in os.walk(directory):
            directories[:] = sorted(name for name in directories if name not in ("target", "tmp", ".git", "__pycache__", "wip", "node_modules"))
            for name in files:
                path = Path(current) / name
                if name.endswith((".pyc", ".pyo")) or name == "strict-tests.env":
                    continue
                result.add(cls._relative(root, path))
        return result

    @classmethod
    def _vendor(cls, root, directory):
        """Pin research archives/locks without traversing ignored extractions.

        These repository vendor directories are research snapshots, not Cargo
        path dependencies. Their immutable archives and manifests are inputs;
        ignored unpacked upstream test fixtures can contain unrelated symlinks.
        A newly introduced local dependency is rejected below until scoped.
        """
        if not (root / directory).is_dir():
            return set()
        result = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", directory],
                                cwd=root, capture_output=True, check=False, timeout=30)
        if result.returncode:
            raise EvidenceError("vendor input selection requires the repository index")
        return {cls._relative(root, os.fsdecode(path)) for path in result.stdout.split(b"\0") if path}

    @classmethod
    def paths(cls, root, arguments):
        root = Path(root).resolve()
        arguments, _ = Command.unwrap(arguments)
        result = {name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rustfmt.toml", ".rustfmt.toml") if (root / name).is_file()}
        result |= cls._tree(root, ".cargo")
        result |= cls.security_inputs(root)
        cargo = Path(arguments[0]).name == "cargo"
        owners = set(Command.values(arguments, "-p", "--package")) if cargo else set()
        if cargo and not owners:
            owners = set(CRATES) - set(Command.values(arguments, "--exclude"))
        if not cargo:
            owners = {name for name in CRATES if any(item.startswith(name + "/") for item in arguments)}
            if not owners:
                owners = set(CRATES)
        if not owners <= set(CRATES):
            raise EvidenceError("unknown workspace package in source scope")
        workspace = {}
        manifest = root / "Cargo.toml"
        if manifest.is_file():
            workspace = tomllib.loads(manifest.read_text()).get("workspace", {}).get("dependencies", {})
        dependencies, pending = set(), list(owners)
        while pending:
            owner = pending.pop()
            manifest = root / owner / "Cargo.toml"
            if not manifest.is_file():
                continue
            document = tomllib.loads(manifest.read_text())
            tables = [document, *document.get("target", {}).values()]
            for table in tables:
                for group in ("dependencies", "dev-dependencies", "build-dependencies"):
                    for key, value in table.get(group, {}).items():
                        settings = workspace.get(key, {}) if isinstance(value, dict) and value.get("workspace") else value
                        if isinstance(settings, dict):
                            package = settings.get("package", key)
                            if "path" in settings and package not in CRATES:
                                raise EvidenceError("new local Cargo dependency needs an explicit source-scope implementation")
                        else:
                            package = key
                        if package in CRATES and package not in owners | dependencies:
                            dependencies.add(package)
                            pending.append(package)
        for owner in owners | dependencies:
            for name in ("Cargo.toml", "build.rs"):
                if (root / owner / name).is_file():
                    result.add(f"{owner}/{name}")
            result |= cls._tree(root, f"{owner}/src")
            result |= cls._vendor(root, f"{owner}/vendor")
            if owner in owners:
                result |= cls._tree(root, f"{owner}/tests")
                result |= cls._tree(root, f"{owner}/examples")
        contract = (not cargo or any(word in item for item in arguments for word in ("source_contract", "final_audit", "public_surface", "strict_env_contract")))
        documentation = cargo and ("--doc" in arguments or len(arguments) > 1 and arguments[1] == "doc")
        if contract:
            result |= cls._tree(root, "scripts")
            result |= cls._tree(root, "docs")
            for path in root.glob("*.md"):
                result.add(cls._relative(root, path))
            for owner in CRATES:
                result |= cls._tree(root, f"{owner}/scripts")
        if contract or documentation:
            for owner in CRATES if contract else owners:
                for path in (root / owner).glob("*.md"):
                    result.add(cls._relative(root, path))
                result |= cls._tree(root, f"{owner}/docs")
                result |= cls._tree(root, f"{owner}/specs")
                template = root / owner / "strict-tests.env.example"
                if template.is_file():
                    result.add(cls._relative(root, template))
        result |= cls._vendor(root, "vendor")
        for item in arguments:
            path = root / item
            if path.is_file() and path.suffix in (".py", ".sh", ".toml"):
                result.add(cls._relative(root, path))
        if not result:
            raise EvidenceError("source scope is empty")
        return sorted(result)

    @classmethod
    def from_paths(cls, root, paths):
        root = Path(root).resolve()
        files = {}
        for path in sorted(set(paths)):
            relative = cls._relative(root, path)
            source = root / relative
            if not source.is_file():
                raise EvidenceError(f"source input is missing: {relative}")
            files[relative] = {"sha256": JsonFile.digest(source), "mode": stat.S_IMODE(source.stat().st_mode) & 0o111}
        if not files:
            raise EvidenceError("source scope is empty")
        encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        return {"sha256": hashlib.sha256(encoded).hexdigest(), "files": files}

    @classmethod
    def capture(cls, root, command, cwd=".", scope_override=None):
        root = Path(root).resolve()
        if Path(cwd).is_absolute() or (root / cwd).resolve() != root:
            raise EvidenceError("release stages must execute from the workspace root")
        paths = cls.paths(root, command)
        if Command.workspace_doctest(command):
            cls.doctest_output_independence(root, paths)
        if scope_override is not None:
            if not isinstance(scope_override, dict) or set(scope_override) != {"paths", "audit"}:
                raise EvidenceError("source scope override requires exactly paths and audit")
            audit_path = JsonFile.verified(scope_override["audit"])
            audit = JsonFile.read(audit_path)
            selected = scope_override.get("paths")
            if (audit.get("schema") != 1 or audit.get("status") != "APPROVED"
                    or not audit.get("reviewer") or not audit.get("reason")
                    or audit.get("command") != Command.canonical(command)
                    or audit.get("paths") != selected or not isinstance(selected, list)):
                raise EvidenceError("scope override lacks its matching independent review")
            excluded = set(paths) - set(selected)
            explanations = audit.get("excluded_paths", {})
            if set(explanations) != excluded or any(not isinstance(reason, str) or not reason.strip() for reason in explanations.values()):
                raise EvidenceError("scope review must explain every omitted input")
            # Root build inputs can never be removed by a scoped source audit.
            mandatory = {name for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml") if (root / name).is_file()}
            mandatory |= cls.security_inputs(root)
            if not mandatory <= set(selected):
                raise EvidenceError("scope override omits root build authority")
            paths = selected
        return cls.from_paths(root, paths)


class RuntimeIdentity:
    @staticmethod
    def capture(names, environment=None):
        if not isinstance(names, (list, tuple)) or any(not re.fullmatch(r"[A-Z][A-Z0-9_]*", name) or SECRET_NAME.search(name) for name in names):
            raise EvidenceError("runtime identity names must be explicitly nonsecret variables")
        environment = dict(os.environ, CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1") if environment is None else environment
        return {name: hashlib.sha256(("present\0" + environment[name] if name in environment else "absent").encode()).hexdigest() for name in sorted(set(names))}


class Outcome:
    @staticmethod
    def test_name(name):
        # Rustdoc adds execution-mode labels to outcome rows but omits those
        # labels from --list. Retain the exact source location and test name.
        name = name.removesuffix(" - should panic")
        rustdoc = re.fullmatch(r"(.+\.(?:rs|md) - .*?\(line [1-9][0-9]*\)) - (?:compile|compile fail)", name)
        return rustdoc.group(1) if rustdoc else name

    @staticmethod
    def inventory(output):
        return [line[:-6] for line in output.splitlines() if line.endswith(": test")]

    @staticmethod
    def harnesses(output):
        """Retain top-level harnesses; supervised child output is not a second gate.

        A child process can print its own libtest inventory and summary while an
        owning test is running. Its parent result is authoritative. Sequential
        top-level rustdoc groups, in contrast, are all required.
        """
        stack, completed = [], []
        for line in output.splitlines():
            start = re.search(r"(?:^|\.\.\.\s*)running (\d+) tests?$", line)
            if start:
                prefix = re.match(r"^test (.+?) \.\.\.", line)
                if prefix and stack:
                    if stack[-1]["pending"] is not None:
                        raise EvidenceError("ambiguous parent outcome before child harness")
                    stack[-1]["pending"] = Outcome.test_name(prefix.group(1))
                stack.append({"declared": int(start.group(1)), "rows": [], "pending": None})
                continue
            summary = SUMMARY.match(line)
            if summary:
                if not stack:
                    raise EvidenceError("test summary lacks its owning harness")
                current = stack.pop()
                current["summary"] = summary.groups()
                if current["pending"] is not None:
                    raise EvidenceError("unfinished test outcome row")
                if not stack:
                    completed.append(current)
                continue
            if not stack:
                continue
            current = stack[-1]
            match = re.match(r"^test (.+?) \.\.\. ?(.*)$", line)
            if match:
                name, rest = match.groups()
                name = Outcome.test_name(name)
                terminal = re.match(r"^(ok|FAILED|ignored)(?:$|\s)", rest)
                if terminal:
                    owner = current
                    if (len(stack) > 1 and current["declared"] == 1
                            and current["pending"] is not None and current["pending"] != name):
                        # A one-case child has already named its pending case.
                        # Other named completions belong to concurrently running
                        # parent tests, whose full inventory is checked below.
                        owner = stack[0]
                    owner["rows"].append((name, terminal.group(1)))
                else:
                    if current["pending"] is not None:
                        raise EvidenceError("ambiguous interleaved test outcome rows")
                    current["pending"] = name
            elif current["pending"] is not None and re.fullmatch(r"(ok|FAILED|ignored)(?:\s+.*)?", line):
                current["rows"].append((current["pending"], line.split()[0]))
                current["pending"] = None
        if stack:
            raise EvidenceError("test output contains an unfinished harness")
        return completed

    @classmethod
    def rows(cls, output):
        return [row for harness in cls.harnesses(output) for row in harness["rows"]]

    @staticmethod
    def supervised_harness(output, inventory, owner):
        """Retain the terminal of a known native crash-test parent.

        Integration process tests can deliberately terminate supervised children
        before libtest reports a summary. Completed child harnesses are removed
        first. For a deliberately unfinished child, the
        pending parent's final token may follow the child's partial prefix.
        The entire parent inventory and final summary must still agree.
        """
        lines = []
        for line in output.splitlines():
            joined = re.match(r"^(test .+? \.\.\.)\s*(running \d+ tests?)$", line)
            lines.extend(joined.groups() if joined else [line])
        starts = [(index, int(match.group(1))) for index, line in enumerate(lines)
                  if (match := re.fullmatch(r"running (\d+) tests?", line))]
        summaries = SUMMARY.findall(output)
        if not starts or not summaries or not inventory or starts[0][1] != len(inventory):
            raise EvidenceError("supervised native tests require their complete parent harness")
        if owner in (True, "app-prepare") and len(inventory) != 4:
            raise EvidenceError("App prepare requires its complete four-case parent harness")
        child_stack, omitted = [], set()
        for index, line in enumerate(lines[starts[0][0] + 1:], starts[0][0] + 1):
            start = re.fullmatch(r"running (\d+) tests?", line)
            if start:
                child_stack.append((index, int(start.group(1))))
            summary = SUMMARY.match(line)
            if summary and child_stack and sum(int(value) for value in summary.groups()[1:5]) == child_stack[-1][1]:
                first, _ = child_stack.pop()
                omitted.update(range(first, index + 1))
        rows, pending, child_active = [], None, False
        for index, line in enumerate(lines):
            if index in omitted or index <= starts[0][0]:
                continue
            if re.fullmatch(r"running \d+ tests?", line):
                if pending is None:
                    raise EvidenceError("supervised child lacks its named pending parent")
                child_active = True
                continue
            match = re.match(r"^test (.+?) \.\.\. ?(.*)$", line)
            if match:
                name, rest = match.groups()
                terminal = re.match(r"^(ok|FAILED|ignored)(?:$|\s)", rest)
                if name not in inventory:
                    raise EvidenceError("supervised native tests emitted an unexpected case")
                if terminal:
                    if not child_active and pending is not None and pending != name:
                        raise EvidenceError("supervised parent case never reached its terminal")
                    rows.append((pending if child_active else name, terminal.group(1)))
                    pending = None
                    child_active = False
                else:
                    if child_active:
                        continue
                    if pending is not None and pending != name:
                        raise EvidenceError("supervised parent case never reached its terminal")
                    pending = name
            elif pending is not None and re.fullmatch(r"(ok|FAILED|ignored)(?:\s+.*)?", line):
                rows.append((pending, line.split()[0]))
                pending = None
                child_active = False
        if pending is not None:
            raise EvidenceError("supervised parent outcome is unfinished")
        return [{"declared": len(inventory), "rows": rows, "summary": summaries[-1]}]

    @classmethod
    def validate(cls, kind, output, inventory=None, expected_filtered=0, allow_empty=False, supervised_parent=False):
        if kind == "tests":
            try:
                harnesses = cls.harnesses(output)
            except EvidenceError:
                if not supervised_parent:
                    raise
                harnesses = cls.supervised_harness(output, inventory, supervised_parent)
            if not harnesses:
                raise EvidenceError("test output has no final summary")
            summaries = [harness["summary"] for harness in harnesses]
            if any(row[0] != "ok" for row in summaries):
                raise EvidenceError("test output contains a failed summary")
            totals = {key: sum(int(row[index]) for row in summaries) for index, key in enumerate(("passed", "failed", "ignored", "measured", "filtered"), 1)}
            if totals["failed"] or totals["ignored"] or totals["measured"] or totals["filtered"] != expected_filtered:
                raise EvidenceError("test summary contains failed, ignored, measured, or unexpected filtered cases")
            for harness in harnesses:
                summary = harness["summary"]
                if (harness["declared"] != sum(int(value) for value in summary[1:5])
                        or len(harness["rows"]) != harness["declared"]):
                    raise EvidenceError("test harness inventory disagrees with its summary")
                if len({name for name, _ in harness["rows"]}) != len(harness["rows"]):
                    raise EvidenceError("duplicate outcomes within one test harness")
            rows = [row for harness in harnesses for row in harness["rows"]]
            names = [name for name, _ in rows]
            if any(result != "ok" for _, result in rows) or len(names) != totals["passed"]:
                raise EvidenceError("test outcome rows disagree with summaries")
            if inventory is None or Counter(names) != Counter(inventory):
                raise EvidenceError("test execution differs from its complete declared inventory")
            if not inventory and not allow_empty:
                raise EvidenceError("behavioral target inventory is empty")
            return totals
        if kind == "inventory":
            names = cls.inventory(output)
            if not names or len(names) != len(set(names)):
                raise EvidenceError("inventory is empty or duplicated")
            if inventory is not None and sorted(names) != sorted(inventory):
                raise EvidenceError("inventory output changed")
            return {"listed": len(names)}
        if kind == "python-tests":
            rows = re.findall(r"^Ran (\d+) tests? in .+$", output, re.M)
            if len(rows) != 1 or int(rows[0]) == 0 or not re.search(r"^OK$", output, re.M):
                raise EvidenceError("Python test execution lacks a complete successful summary")
            if re.search(r"^(FAILED|OK \(|.*\.\.\. skipped)", output, re.M):
                raise EvidenceError("Python test execution was failed or skipped")
            return {"passed": int(rows[0]), "failed": 0, "ignored": 0, "filtered": 0}
        if kind != "command":
            raise EvidenceError("unknown stage outcome kind")
        return None


class OwnedProcess:
    @staticmethod
    def run(arguments, root, output, timeout, cleanup_timeout=5, stream=True, stdout_output=None):
        process = None
        interrupted = []
        parent = os.environ.get("AI_KIT_RELEASE_PARENT_RECEIPT")
        marker = None
        if parent:
            parent_path = Path(parent)
            if not parent_path.is_absolute() or not parent_path.is_file():
                raise EvidenceError("parent process receipt is missing or not absolute")
            marker = parent_path.with_name(parent_path.name + ".release-" + uuid.uuid4().hex)
            JsonFile.write(marker, {"schema": 1, "status": "ADMITTING", "owner_pid": os.getpid()})
        def interrupt(number, _frame):
            interrupted.append(number)
            if process is not None:
                # selectors retries InterruptedError internally. Use our own
                # exception so cancellation always enters the cleanup owner.
                raise KeyboardInterrupt("release stage was interrupted")
        previous = {number: signal.signal(number, interrupt) for number in (signal.SIGTERM, signal.SIGINT)}
        selector = selectors.DefaultSelector()
        settled = False
        started = time.monotonic()
        try:
            process = subprocess.Popen(arguments, cwd=root, env=Command.environment(arguments),
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE if stdout_output is not None else subprocess.STDOUT,
                                       start_new_session=True)
            if marker:
                JsonFile.write(marker, {"schema": 1, "status": "ACTIVE", "owner_pid": os.getpid(), "pgid": process.pid})
            if interrupted:
                raise InterruptedError("release stage was interrupted during admission")
            selector.register(process.stdout, selectors.EVENT_READ)
            if process.stderr is not None:
                selector.register(process.stderr, selectors.EVENT_READ)
            while selector.get_map():
                if time.monotonic() - started >= timeout:
                    raise subprocess.TimeoutExpired(arguments, timeout)
                for key, _ in selector.select(min(1, max(0.01, timeout - (time.monotonic() - started)))):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    if output.write(chunk) != len(chunk):
                        raise EvidenceError("stage log capture wrote an incomplete chunk")
                    output.flush()
                    if stdout_output is not None and key.fileobj is process.stdout:
                        if stdout_output.write(chunk) != len(chunk):
                            raise EvidenceError("stage stdout capture wrote an incomplete chunk")
                        stdout_output.flush()
                    if stream:
                        sys.stdout.buffer.write(chunk)
                        sys.stdout.buffer.flush()
            code = process.wait(timeout=max(0.01, timeout - (time.monotonic() - started)))
            settled = OwnedProcess.settle(process, cleanup_timeout)
            if not settled:
                raise EvidenceError("owned command process group did not settle")
            return code
        finally:
            selector.close()
            try:
                if process is not None and not settled:
                    signal.signal(signal.SIGTERM, signal.SIG_IGN)
                    signal.signal(signal.SIGINT, signal.SIG_IGN)
                    settled = OwnedProcess.settle(process, cleanup_timeout)
                    if not settled:
                        raise EvidenceError("owned command cleanup could not establish settlement")
                if marker and (process is None or settled):
                    marker.unlink()
            finally:
                if process is not None:
                    for pipe in (process.stdout, process.stderr):
                        if pipe is not None:
                            pipe.close()
                for number, handler in previous.items():
                    signal.signal(number, handler)

    @staticmethod
    def settle(process, timeout):
        for number in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, number)
            except ProcessLookupError:
                pass
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                process.poll()
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    process.wait()
                    return True
                except PermissionError:
                    pass
                time.sleep(0.02)
        return False


class Stage:
    def __init__(self, directory, stage_id, arguments, compiler=None, execute=None):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", stage_id):
            raise EvidenceError("invalid stage identity")
        self.directory = Path(directory).resolve()
        self.manifest = JsonFile.read(self.directory / "manifest.json")
        if self.manifest.get("schema") != 1 or not isinstance(self.manifest.get("root"), str) or not isinstance(self.manifest.get("stages"), dict):
            raise EvidenceError("unsupported release manifest schema")
        self.root = Path(self.manifest["root"]).resolve()
        if Path.cwd().resolve() != self.root:
            raise EvidenceError("release stage cwd differs from the manifest workspace")
        self.stage_id, self.arguments = stage_id, arguments
        self.canonical = Command.canonical(arguments)
        self.definition = self.manifest.get("stages", {}).get(stage_id, {})
        if not isinstance(self.definition, dict):
            raise EvidenceError("stage definition must be an object")
        allowed = {"command", "cwd", "kind", "inventory", "expected_filtered", "runtime", "reuse", "scope_override", "timeout_seconds", "fresh"}
        if set(self.definition) - allowed:
            raise EvidenceError("stage definition contains unknown fields")
        if not isinstance(self.definition.get("runtime", {}), dict):
            raise EvidenceError("stage runtime identity must be an object")
        if "inventory" in self.definition and (not isinstance(self.definition["inventory"], list) or any(not isinstance(name, str) or not name for name in self.definition["inventory"])):
            raise EvidenceError("stage inventory must contain nonempty test names")
        if self.definition and Command.canonical(self.definition.get("command", [])) != self.canonical:
            raise EvidenceError("stage command differs from its declared receipt binding")
        self.cwd = self.definition.get("cwd", ".")
        self.compiler = (compiler if compiler is not None else Compiler.read(self.root)).strip()
        if self.compiler.strip() != self.manifest.get("compiler", "").strip():
            raise EvidenceError("compiler identity changed")
        self.kind = self.definition.get("kind", Command.kind(arguments))
        if self.kind != Command.kind(arguments):
            raise EvidenceError("stage kind cannot weaken the command's inferred checks")
        self.override = self.definition.get("scope_override")
        self.source = SourceIdentity.capture(self.root, arguments, self.cwd, self.override)
        self.runtime = RuntimeIdentity.capture(list(self.definition.get("runtime", {})))
        if self.runtime != self.definition.get("runtime", {}):
            raise EvidenceError("runtime configuration differs from its receipt binding")
        self.build_environment = RuntimeIdentity.capture(Command.build_names(arguments), Command.environment(arguments))
        self.fresh = bool(self.definition.get("fresh") or Command.fresh(stage_id, arguments) or os.environ.get("AI_KIT_RELEASE_STAGE_FRESH") == "1")
        self.execute = execute or OwnedProcess.run
        self.allow_empty = Command.allow_empty(arguments)
        self.filtered = self.definition.get("expected_filtered", 0)
        if type(self.filtered) is not int or self.filtered < 0:
            raise EvidenceError("invalid expected filtered count")
        self.timeout = self.definition.get("timeout_seconds", Command.timeout(stage_id, arguments))
        if type(self.timeout) not in (int, float) or not math.isfinite(self.timeout) or self.timeout <= 0:
            raise EvidenceError("stage execution requires a positive finite bound")
        if stage_id.startswith("provider.live.") and self.timeout < Command.timeout(stage_id, arguments):
            raise EvidenceError("Provider live bound would truncate authored batch deadlines")

    def matching(self, prior):
        expected = {"schema": 1, "cwd": self.cwd, "compiler": self.compiler, "kind": self.kind,
                    "source_before": self.source, "source_after": self.source, "runtime": self.runtime,
                    "build_environment": self.build_environment, "returncode": 0, "status": "PASS", "cleanup_settled": True}
        for name, value in expected.items():
            if prior.get(name) != value:
                raise EvidenceError(f"prior receipt does not match current {name}")
        if Command.canonical(prior.get("command", [])) != self.canonical:
            raise EvidenceError("prior receipt command differs")
        if prior.get("execution") not in ("EXECUTED", "IMPORTED"):
            raise EvidenceError("reuse must resolve to original executed evidence")
        if prior["execution"] == "IMPORTED":
            origin = prior.get("origin", {})
            if not origin.get("description") or not origin.get("reviewer"):
                raise EvidenceError("imported evidence lacks a reviewed provenance description")
            JsonFile.verified(origin.get("artifact"))
            for artifact in origin.get("additional_artifacts", []):
                JsonFile.verified(artifact)
            cleanup = origin.get("cleanup_evidence", {})
            if (cleanup.get("settled") is not True
                    or cleanup.get("basis") not in ("historical_owner_completion", "observed_process_group_settlement")
                    or not cleanup.get("reason") or not isinstance(cleanup.get("artifacts"), list)
                    or not cleanup["artifacts"]):
                raise EvidenceError("imported evidence lacks its reviewed historical cleanup authority")
            for artifact in cleanup["artifacts"]:
                JsonFile.verified(artifact)
        if Command.live(self.stage_id, self.arguments) and not self.runtime:
            raise EvidenceError("live evidence cannot be reused without runtime configuration identity")
        log = JsonFile.verified(prior.get("log"))
        # Cargo/libtest writes harness outcomes to stdout. --nocapture stderr
        # diagnostics can otherwise split even the bytes of a test name/status.
        # Retain the full display log and validate its separate stdout evidence.
        outcome_log = JsonFile.verified(prior["stdout_log"]) if "stdout_log" in prior else log
        if "stdout_log" in prior and self.kind != "tests":
            raise EvidenceError("stdout outcome evidence is only valid for Cargo tests")
        output = outcome_log.read_text(errors="replace")
        inventory = prior.get("inventory")
        counts = Outcome.validate(self.kind, output, inventory, self.filtered, self.allow_empty, Command.supervised_parent(self.arguments))
        if counts != prior.get("counts"):
            raise EvidenceError("prior receipt counts disagree with raw output")
        if self.definition.get("inventory") is not None and self.definition["inventory"] != inventory:
            raise EvidenceError("prior receipt inventory differs from the stage declaration")
        return log

    def run(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        with (self.directory / "execution.lock").open("a") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            return self._locked()

    def _locked(self):
        if SourceIdentity.capture(self.root, self.arguments, self.cwd, self.override) != self.source:
            raise EvidenceError("source changed while awaiting stage admission")
        index_path = self.directory / "index.json"
        index = JsonFile.read(index_path) if index_path.exists() else {"schema": 1, "stages": {}}
        prior_reference = index["stages"].get(self.stage_id)
        if prior_reference:
            previous = JsonFile.read(JsonFile.verified(prior_reference))
            if previous.get("status") != "PASS":
                raise EvidenceError("a failed/incomplete stage cannot be retried automatically in one run")
            if (Command.canonical(previous.get("command", [])) != self.canonical
                    or previous.get("source_before") != self.source or previous.get("source_after") != self.source):
                raise EvidenceError("stage command or source changed within the current run")
            if previous.get("execution") == "REUSED":
                prior_reference = previous["reused_from"]
            if self.fresh:
                prior_reference = None
        elif not self.fresh:
            reference = self.definition.get("reuse")
            if reference:
                prior_reference = {"path": reference.get("receipt"), "sha256": reference.get("sha256")}
        # Crate and workspace owners can name the same literal gate differently.
        # Reuse that current-run execution only after the same full validation;
        # an alias must never provide an automatic retry of a failed command.
        for other_id, reference in index["stages"].items():
            if other_id == self.stage_id:
                continue
            other = JsonFile.read(JsonFile.verified(reference))
            if Command.canonical(other.get("command", [])) != self.canonical:
                continue
            if other.get("status") != "PASS":
                raise EvidenceError("a failed/incomplete command cannot be retried through another stage identity")
            if prior_reference or self.fresh:
                continue
            identity = {"source_before": self.source, "source_after": self.source,
                        "compiler": self.compiler, "cwd": self.cwd, "kind": self.kind,
                        "runtime": self.runtime, "build_environment": self.build_environment}
            if any(other.get(name) != value for name, value in identity.items()):
                continue
            if other.get("execution") == "REUSED":
                reference = other["reused_from"]
                other = JsonFile.read(JsonFile.verified(reference))
            self.matching(other)
            prior_reference = reference
        started = time.monotonic()
        stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        folder = self.directory / "stages" / self.stage_id
        folder.mkdir(parents=True, exist_ok=True)
        stem = uuid.uuid4().hex
        receipt_path, log_path = folder / (stem + ".json"), folder / (stem + ".log")
        receipt = {"schema": 1, "stage_id": self.stage_id, "command": self.arguments,
                   "canonical_command": self.canonical, "command_identity_rules": Command.identity_rules(self.arguments),
                   "cwd": self.cwd, "compiler": self.compiler,
                   "kind": self.kind, "source_before": self.source, "runtime": self.runtime,
                   "build_environment": self.build_environment, "status": "INCOMPLETE", "execution": "EXECUTED",
                   "started_utc": stamp, "fresh_required": self.fresh, "cleanup_settled": False,
                   "inventory": None, "counts": None}
        JsonFile.write(receipt_path, receipt)
        try:
            if prior_reference:
                prior_path = JsonFile.verified(prior_reference)
                prior = JsonFile.read(prior_path)
                source_log = self.matching(prior)
                print(f"REUSED {self.stage_id}: {prior_path}", file=sys.stderr, flush=True)
                with source_log.open("rb") as source, log_path.open("xb") as output:
                    while chunk := source.read(65536):
                        if output.write(chunk) != len(chunk):
                            raise EvidenceError("reused log capture wrote an incomplete chunk")
                        sys.stdout.buffer.write(chunk)
                    sys.stdout.buffer.flush()
                receipt.update(status="PASS", execution="REUSED", returncode=0, cleanup_settled=True,
                               inventory=prior.get("inventory"), counts=prior.get("counts"), reused_from=prior_reference)
                if "stdout_log" in prior:
                    receipt["stdout_log"] = prior["stdout_log"]
            else:
                print(f"EXECUTED {self.stage_id}: {' '.join(self.arguments)}", file=sys.stderr, flush=True)
                inventory = self.definition.get("inventory")
                if self.kind == "tests":
                    listing_path = folder / (stem + ".inventory.log")
                    listing = Command.listing(self.arguments)
                    with listing_path.open("xb") as output:
                        code = self.execute(listing, self.root, output, self.timeout, stream=False)
                    receipt["inventory_evidence"] = {"command": listing, "returncode": code,
                        "log": {"path": str(listing_path), "sha256": JsonFile.digest(listing_path)}}
                    if code != 0:
                        raise EvidenceError("test inventory command failed")
                    actual = Outcome.inventory(listing_path.read_text(errors="replace"))
                    if not actual and not self.allow_empty:
                        raise EvidenceError("test inventory is empty or duplicated")
                    if inventory is not None and sorted(actual) != sorted(inventory):
                        raise EvidenceError("current inventory differs from its declared expectation")
                    inventory = actual
                with log_path.open("xb") as output:
                    if self.kind == "tests":
                        stdout_path = folder / (stem + ".stdout.log")
                        with stdout_path.open("xb") as stdout:
                            code = self.execute(self.arguments, self.root, output, self.timeout, stdout_output=stdout)
                        receipt["stdout_log"] = {"path": str(stdout_path), "sha256": JsonFile.digest(stdout_path)}
                    else:
                        code = self.execute(self.arguments, self.root, output, self.timeout)
                receipt.update(returncode=code, cleanup_settled=True, inventory=inventory)
                if code != 0:
                    receipt["status"] = "FAIL"
                else:
                    outcome_path = Path(receipt["stdout_log"]["path"]) if "stdout_log" in receipt else log_path
                    text = outcome_path.read_text(errors="replace")
                    if self.kind == "inventory":
                        inventory = Outcome.inventory(text)
                        receipt["inventory"] = inventory
                    receipt["counts"] = Outcome.validate(self.kind, text, inventory, self.filtered, self.allow_empty, Command.supervised_parent(self.arguments))
                    receipt["status"] = "PASS"
            receipt["source_after"] = SourceIdentity.capture(self.root, self.arguments, self.cwd, self.override)
            if receipt["source_after"] != self.source:
                receipt.update(status="INCOMPLETE", error="source changed during stage")
        except (OSError, ValueError, subprocess.SubprocessError, InterruptedError, KeyboardInterrupt) as error:
            receipt.update(status="INCOMPLETE", error=str(error))
        finally:
            if log_path.is_file():
                receipt["log"] = {"path": str(log_path), "sha256": JsonFile.digest(log_path)}
            receipt["duration_seconds"] = round(time.monotonic() - started, 3)
            JsonFile.write(receipt_path, receipt)
            index["stages"][self.stage_id] = {"path": str(receipt_path), "sha256": JsonFile.digest(receipt_path)}
            JsonFile.write(index_path, index)
        print(f"{receipt['status']} {self.stage_id}: {receipt_path}", file=sys.stderr, flush=True)
        return 0 if receipt["status"] == "PASS" else 1

    @classmethod
    def main(cls, arguments=None):
        parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
        subparsers = parser.add_subparsers(dest="mode", required=True)
        stage = subparsers.add_parser("stage", allow_abbrev=False)
        stage.add_argument("stage_id")
        stage.add_argument("command", nargs=argparse.REMAINDER)
        options = parser.parse_args(arguments)
        command = options.command[1:] if options.command[:1] == ["--"] else options.command
        directory = os.environ.get("AI_KIT_RELEASE_RECEIPT_RUN")
        if not directory:
            parser.error("stage execution requires AI_KIT_RELEASE_RECEIPT_RUN")
        try:
            return cls(directory, options.stage_id, command).run()
        except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
            print(f"INCOMPLETE {options.stage_id}: {error}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(Stage.main())

#!/usr/bin/env bash
set -euo pipefail

if ! command -v python3 >/dev/null; then
    printf 'INCOMPLETE: missing embedded verifier prerequisite: python3\n' >&2
    exit 2
fi

exec python3 - "$0" "$@" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time


class Interrupted(BaseException):
    pass


class EmbeddedLanceRun:
    TARGETS = (
        "lancedb_native_feasibility",
        "lancedb_durability_feasibility",
        "lancedb_state_store",
    )

    def __init__(self, script, output):
        self.workspace = Path(script).resolve().parents[2]
        self.output = Path(output).resolve()
        self.receipt = {
            "suite": "embedded_lance",
            "execution": "INCOMPLETE",
            "scope": "three complete embedded targets, no default features",
            "limitations": "does not run other feature lanes, platforms, consumers or remote services",
            "commands": [],
            "errors": [],
        }
        self.status = 2
        self.launching = False
        self.pending_interruption = None

    def interrupted(self, signum, _frame):
        error = Interrupted(f"verifier interrupted by signal {signum}")
        if self.launching:
            # Popen must transfer its child handle before interruption unwinds.
            if self.pending_interruption is None:
                self.pending_interruption = error
            return
        raise error

    def metadata(self, command):
        return subprocess.run(
            command, cwd=self.workspace, check=True, timeout=30,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ).stdout.strip()

    def source(self):
        # Include untracked implementation/tests too. No Git index or clean-tree
        # assumption can silently omit the prototype from its execution receipt.
        paths = set()
        for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain", "rust-toolchain.toml"):
            path = self.workspace / name
            if path.is_file():
                paths.add(path)
        paths.update(self.workspace.glob("*-sdk/Cargo.toml"))
        for name in (".cargo", "storage-sdk/src", "storage-sdk/tests", "storage-sdk/scripts"):
            root = self.workspace / name
            if root.is_dir():
                paths.update(path for path in root.rglob("*") if path.is_file())
        files = []
        for path in sorted(paths):
            if path == self.output or self.output in path.parents:
                continue
            files.append({
                "path": path.relative_to(self.workspace).as_posix(),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            })
        encoded = json.dumps(files, separators=(",", ":")).encode()
        return {"digest": hashlib.sha256(encoded).hexdigest(), "files": files}

    def filesystem(self):
        path = Path(tempfile.gettempdir()).resolve()
        result = {"path": str(path), "status": "unavailable"}
        try:
            result["df"] = self.metadata(["df", "-P", str(path)])
            if platform.system() == "Linux":
                result["type"] = self.metadata(["stat", "-f", "-c", "%T", str(path)])
            else:
                # macOS stat's %T means file type, not filesystem type. Resolve
                # the longest containing mount from the system's mount table.
                matches = []
                for line in self.metadata(["mount"]).splitlines():
                    match = re.search(r" on (.+) \(([^,)]+)", line)
                    if match:
                        mount = Path(match[1])
                        if path == mount or mount in path.parents:
                            matches.append((len(mount.parts), match[2], line))
                if not matches:
                    raise RuntimeError("temporary dataset filesystem not found in mount table")
                _, result["type"], result["mount"] = max(matches)
            result["status"] = "recorded"
        except Exception as error:
            result["reason"] = str(error)
        return result

    def settle(self, process):
        # Every command owns one fresh group. Darwin can report EPERM while
        # the terminated direct child is briefly a zombie; neither that error
        # nor a returned exit code proves that the whole group is absent.
        observation_error = None
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError as error:
            observation_error = error
        deadline = time.monotonic() + 30
        while True:
            process.poll()
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                break
            except PermissionError as error:
                observation_error = error
            else:
                observation_error = None
            if time.monotonic() >= deadline:
                self.receipt["errors"].append("owned command group exceeded its cleanup deadline")
                if observation_error is not None:
                    self.receipt["errors"].append("owned command group observation remained unavailable")
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError as error:
                    self.receipt["errors"].append(
                        f"owned command forced termination failed ({type(error).__name__})")
                finally:
                    try:
                        process.wait(timeout=10)
                    except (OSError, subprocess.TimeoutExpired) as error:
                        self.receipt["errors"].append(
                            f"owned command child reap failed ({type(error).__name__})")
                raise RuntimeError("owned command required forced cleanup")
            time.sleep(0.05)
        process.wait(timeout=10)

    def command(self, target):
        args = [
            "cargo", "test", "--locked", "-p", "storage-sdk", "--no-default-features",
            "--test", target, "--", "--nocapture", "--test-threads=1",
        ]
        record = {"target": target, "argv": args, "log": f"{target}.log"}
        self.receipt["commands"].append(record)
        process = None
        started = time.monotonic()
        print(f"Running {target}; log: {self.output / record['log']}", flush=True)
        try:
            with (self.output / record["log"]).open("wb") as log:
                self.launching = True
                try:
                    process = subprocess.Popen(
                        args, cwd=self.workspace, stdout=log, stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                finally:
                    self.launching = False
                if self.pending_interruption is not None:
                    raise self.pending_interruption
                record["exit_code"] = process.wait()
                if record["exit_code"] != 0:
                    self.receipt["execution"] = "FAIL"
                    self.status = record["exit_code"] if record["exit_code"] > 0 else 1
                    self.receipt["errors"].append(
                        f"{target} exited {record['exit_code']}; see {record['log']}"
                    )
        except (Exception, Interrupted) as error:
            # Cleanup may also fail; retain the original failure and any signal
            # deferred during a failed launch before entering that boundary.
            for failure in (error, self.pending_interruption):
                if failure is not None and str(failure) not in self.receipt["errors"]:
                    self.receipt["errors"].append(str(failure))
            raise
        finally:
            # Repeated interruption must not cancel the owned cleanup itself.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            try:
                if process is not None:
                    self.settle(process)
            finally:
                if process is not None:
                    record["exit_code"] = process.returncode
                record["seconds"] = round(time.monotonic() - started, 3)
                self.pending_interruption = None
                signal.signal(signal.SIGTERM, self.interrupted)
                signal.signal(signal.SIGINT, self.interrupted)
        log = (self.output / record["log"]).read_text(errors="replace")
        summaries = re.findall(
            r"^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; "
            r"(\d+) ignored; (\d+) measured; (\d+) filtered out", log, re.MULTILINE,
        )
        if summaries:
            record["counts"] = dict(zip(
                ("passed", "failed", "ignored", "measured", "filtered_out"),
                map(int, summaries[-1]),
            ))
        if record["exit_code"] != 0:
            raise RuntimeError(f"{target} exited {record['exit_code']}; see {record['log']}")
        counts = record.get("counts")
        if not counts or counts["passed"] == 0 or any(
            counts[name] for name in ("failed", "ignored", "measured", "filtered_out")
        ):
            raise RuntimeError(f"{target} did not report a complete nonempty passing suite")

    def execute(self):
        # Exclusive creation prevents both accidental overwrite and two runs
        # racing to claim the same receipt directory.
        self.output.mkdir(parents=True, exist_ok=False)
        signal.signal(signal.SIGTERM, self.interrupted)
        signal.signal(signal.SIGINT, self.interrupted)
        print(f"Embedded Lance evidence: {self.output}", flush=True)
        try:
            self.receipt["source"] = self.source()
            self.receipt["lockfile_sha256"] = hashlib.sha256(
                (self.workspace / "Cargo.lock").read_bytes()
            ).hexdigest()
            for tool in ("rustc", "cargo"):
                if not shutil.which(tool):
                    raise RuntimeError(f"missing required embedded-test tool: {tool}")
            self.receipt["compiler"] = self.metadata(["rustc", "-vV"])
            if not self.receipt["compiler"].startswith("rustc 1.99.0 "):
                raise RuntimeError("embedded release evidence requires Rust 1.99.0")
            self.receipt["platform"] = self.metadata(["uname", "-a"])
            if platform.system() not in ("Darwin", "Linux"):
                raise RuntimeError("embedded directory tests require macOS or Linux")
            self.receipt["filesystem"] = self.filesystem()
            self.receipt["build_environment"] = {
                name: os.environ[name] for name in (
                    "RUSTUP_TOOLCHAIN", "CARGO_BUILD_JOBS", "CARGO_INCREMENTAL",
                    "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_TEST_DEBUG",
                    "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_TARGET_DIR",
                    "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
                ) if name in os.environ
            }
            for target in self.TARGETS:
                self.command(target)
            self.receipt["execution"] = "PASS"
            self.status = 0
        except (Exception, Interrupted) as error:
            if str(error) not in self.receipt["errors"]:
                self.receipt["errors"].append(str(error))
            print(f"{self.receipt['execution']}: {error}", file=sys.stderr, flush=True)
        finally:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            try:
                self.receipt["source_after"] = self.source()
                if self.receipt.get("source") != self.receipt["source_after"]:
                    self.receipt["errors"].append("source inputs changed during verification")
                    if self.receipt["execution"] != "FAIL":
                        self.receipt["execution"] = "INCOMPLETE"
                        self.status = 2
            except Exception as error:
                self.receipt["errors"].append(f"final source identity unavailable: {error}")
                if self.receipt["execution"] != "FAIL":
                    self.receipt["execution"] = "INCOMPLETE"
                    self.status = 2
            temporary = self.output / "summary.json.partial"
            temporary.write_text(json.dumps(self.receipt, indent=2) + "\n")
            temporary.replace(self.output / "summary.json")
        print(f"Embedded Lance execution: {self.receipt['execution']}; receipt: {self.output / 'summary.json'}", flush=True)
        return self.status


if len(sys.argv) != 3:
    print(f"usage: {sys.argv[1]} NEW_EVIDENCE_DIRECTORY", file=sys.stderr)
    sys.exit(2)
try:
    sys.exit(EmbeddedLanceRun(sys.argv[1], sys.argv[2]).execute())
except Exception as error:
    print(f"INCOMPLETE: {error}", file=sys.stderr)
    sys.exit(2)
PY

#!/usr/bin/env python3
"""Run App workflow/worker tests in explicit routine, diagnostic, or capacity scopes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import shlex
import shutil
import subprocess
import time
import tempfile
import tomllib


class AppWorkloads:
    ROUTINE = "notification_delivery_routine_preserves_receipts_replay_and_cleanup"
    DIAGNOSTIC = "delivery_capacity_prefix_measurement"
    GROWTH = "delivery_capacity_growth_measurement"
    CALIBRATORS = (
        "storage_observation_cancelled_wait_does_not_remain_active",
        "storage_observation_distinguishes_read_only_and_mutating_commits",
        "storage_transaction_observation_separates_native_calls_and_owner_drop",
        "checkpoint_poll_capture_excludes_suspended_time_and_retains_open_spans",
    )
    CAPACITY = (
        ('worker_contract', 'admission_cleanup_does_not_charge_lazy_execution_lifetimes', 9300),
        ('workflow_execution', 'retained_revision_scan_accepts_short_pages_at_4095_4096_4097', 9000),
        ('workflow_execution', 'reopen_accepts_valid_short_pages_beyond_the_old_scan_window', 3600),
        ('workflow_execution', 'late_foreign_prefix_is_observed_before_any_generation_claim', 9000),
        ('workflow_execution', 'late_invalid_inventory_is_observed_before_any_generation_claim', 4800),
    )
    RUNTIME = ("app_kit", "storage_kit", "agents_kit", "lance", "lancedb", "tokio", "serde_json")
    SUMMARY = re.compile(r"^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out", re.M)

    def __init__(self, root, mode, profile=None, case=None, retain_baseline=False, baseline_receipt=None):
        if mode not in ("routine", "diagnostic", "capacity"):
            raise ValueError("unknown workload scope")
        fixed_profile = "capacity" if mode == "capacity" else "test"
        if profile not in (None, "test", "capacity") or (mode != "diagnostic" and profile not in (None, fixed_profile)):
            raise ValueError("routine uses test; capacity uses capacity; only diagnostics select a profile")
        if mode == "routine" and case is not None:
            raise ValueError("routine scope cannot select an individual case")
        allowed = (self.DIAGNOSTIC, self.GROWTH) if mode == "diagnostic" else tuple(row[1] for row in self.CAPACITY)
        if case is not None and case not in allowed:
            raise ValueError("case does not belong to the selected workload scope")
        if (retain_baseline or baseline_receipt is not None) and mode != "diagnostic":
            raise ValueError("frozen executables are available only for diagnostic comparisons")
        if retain_baseline and baseline_receipt is not None:
            raise ValueError("cannot retain and reuse a baseline in the same invocation")
        self.root, self.mode = root, mode
        self.retain_baseline, self.baseline_receipt = retain_baseline, baseline_receipt
        self.profile = profile or fixed_profile
        self.case = case or (self.DIAGNOSTIC if mode == "diagnostic" else None)
        self.directory = root / "target/app-workload-checks" / (time.strftime("%Y%m%d-%H%M%S-") + mode + "-" + str(os.getpid()))
        self.directory.mkdir(parents=True, mode=0o700)
        self.child = None
        self.interrupted = None
        self.receipt = {"scope": mode, "profile": self.profile, "case": self.case,
                        "artifacts": {}, "status": "INCOMPLETE", "platform": list(os.uname()),
                        "dataset_filesystem": "record separately in the platform qualification receipt",
                        "commands": [], "process_cleanup_errors": []}

    def save(self):
        (self.directory / "receipt.json").write_text(json.dumps(self.receipt, indent=2) + "\n")

    def identity(self):
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root).strip()
        diff = subprocess.check_output(["git", "diff", "--binary", "HEAD", "--", ".", ":!tmp/**"], cwd=self.root)
        digest = hashlib.sha256(head + b"\0" + diff)
        names = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=self.root)
        for name in sorted(filter(None, names.split(b"\0"))):
            if name.startswith(b"tmp/"):
                continue
            path = self.root / os.fsdecode(name)
            digest.update(name + b"\0" + str(path.lstat().st_mode).encode() + b"\0")
            digest.update(os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes())
        return {"head": head.decode(), "candidate_sha256": digest.hexdigest(),
                "lock_sha256": hashlib.sha256((self.root / "Cargo.lock").read_bytes()).hexdigest()}

    def preflight(self):
        version_hint = os.environ.get("LANCE_USE_VERSION_HINT")
        if version_hint is not None and (version_hint != "0" or self.mode != "diagnostic" or self.case != self.DIAGNOSTIC):
            raise ValueError("LANCE_USE_VERSION_HINT must be unset except for exact value 0 in the prefix diagnostic")
        self.receipt["version_hint"] = {
            "mode": "native-default" if version_hint is None else "explicit-diagnostic-disable",
            "value": version_hint,
            "qualification": "ordinary workload policy; no hint override" if version_hint is None else
                "net-policy diagnostic only; not default, growth, or capacity qualification",
        }
        expected = {"opt-level": "3" if self.profile == "capacity" else "0",
                    "debug-assertions": "true", "overflow-checks": "true"}
        for name in ("RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
                     "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"):
            if os.environ.get(name):
                raise ValueError("compiler override prevents artifact qualification: " + name)
        if self.profile == "capacity" and os.environ.get("CARGO_INCREMENTAL") not in (None, "0"):
            raise ValueError("capacity forbids global incremental compilation override")
        recipe = {**expected, "debug": "0", "incremental": "false", "lto": "off", "codegen-units": "16"}
        flags = []
        config_identities = []
        for name, value in os.environ.items():
            if name == "CARGO_ENCODED_RUSTFLAGS":
                flags.extend(value.split("\x1f"))
            elif name in ("RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS") or (name.startswith("CARGO_TARGET_") and name.endswith("_RUSTFLAGS")):
                flags.extend(shlex.split(value))
            elif name.startswith("CARGO_PROFILE_" + self.profile.upper() + "_"):
                key = name[len("CARGO_PROFILE_" + self.profile.upper() + "_"):].lower().replace("_", "-")
                if self.profile == "capacity" and (key not in recipe or value.lower() != recipe[key]):
                    raise ValueError("conflicting capacity recipe override: " + name)
                if key in expected and value.lower() != expected[key]:
                    raise ValueError("conflicting profile override: " + name)
        # Cargo profile overrides appear in compiler-artifact records. Compiler
        # flags need their own check because Cargo's profile record omits them.
        configs = [parent / ".cargo" / name for parent in (self.root, *self.root.parents)
                   for name in ("config", "config.toml")]
        cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
        configs.extend(cargo_home / name for name in ("config", "config.toml"))
        for path in dict.fromkeys(configs):
            if not path.is_file():
                continue
            data = path.read_bytes()
            value = tomllib.loads(data.decode())
            config_identities.append({"path": str(path), "sha256": hashlib.sha256(data).hexdigest()})
            parents = (self.profile, "release" if self.profile == "capacity" else "dev")
            if any(value.get("profile", {}).get(name) for name in parents):
                raise ValueError("Cargo config profile overrides prevent recipe qualification")
            build = value.get("build", {})
            if any(build.get(name) for name in ("rustc", "rustc-wrapper", "rustc-workspace-wrapper")):
                raise ValueError("Cargo config compiler override prevents artifact qualification")
            if self.profile == "capacity" and build.get("incremental"):
                raise ValueError("Cargo config incremental override prevents recipe qualification")
            owners = [build, *value.get("target", {}).values()]
            for owner in owners:
                configured = owner.get("rustflags", [])
                flags.extend(shlex.split(configured) if isinstance(configured, str) else configured)
        manifest = self.root / "Cargo.toml"
        if manifest.is_file():
            profiles = tomllib.loads(manifest.read_text()).get("profile", {})
            parents = (self.profile, "release" if self.profile == "capacity" else "dev")
            if any(profiles.get(name, {}).get("package") for name in parents):
                raise ValueError("per-package profile overrides prevent runtime qualification")
        self.receipt["compiler_overrides"] = {"flag_sha256": hashlib.sha256("\0".join(flags).encode()).hexdigest(),
            "configuration": config_identities,
            "profile_environment_names": sorted(name for name in os.environ if name.startswith("CARGO_PROFILE_"))}
        index = 0
        while index < len(flags):
            item = flags[index]
            if item in ("-C", "--codegen"):
                index += 1
                if index == len(flags):
                    raise ValueError("incomplete compiler codegen override")
                item = flags[index]
            elif item.startswith("-C"):
                item = item[2:]
            elif item.startswith("--codegen="):
                item = item[len("--codegen="):]
            else:
                index += 1
                continue
            key, _, value = item.partition("=")
            normalized = {"yes": "true", "on": "true", "1": "true", "no": "false", "off": "false", "0": "false"}.get(value, value)
            if key == "opt-level":
                normalized = value
            if key in expected and normalized != expected[key]:
                raise ValueError("conflicting compiler flag: " + key)
            if self.profile == "capacity" and key == "incremental":
                raise ValueError("capacity forbids incremental compiler flag")
            native_recipe = {"debuginfo": "0", "lto": "off", "codegen-units": "16"}
            if self.profile == "capacity" and key in native_recipe and value != native_recipe[key]:
                raise ValueError("conflicting capacity compiler flag: " + key)
            index += 1

    def environment(self):
        self.receipt["environment"] = {name: os.environ[name] for name in
            ("CARGO_BUILD_JOBS", "CARGO_TARGET_DIR", "CARGO_INCREMENTAL",
             "CARGO_PROFILE_TEST_DEBUG", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_CAPACITY_DEBUG")
            if name in os.environ}
        self.receipt["filesystems"] = {}
        mount = subprocess.run(["mount"], capture_output=True, text=True, timeout=10)
        for name, path in (("workspace", self.root), ("dataset_parent", Path(tempfile.gettempdir()))):
            stat = os.statvfs(path)
            result = subprocess.run(["df", "-k", str(path)], capture_output=True, text=True, timeout=10)
            self.receipt["filesystems"][name] = {"path": str(path), "available_bytes": stat.f_bavail * stat.f_frsize,
                                                "df_exit": result.returncode, "df": result.stdout[-4096:]}
            devices = {line.split()[0] for line in result.stdout.splitlines()[1:] if line.split()}
            entries = [line for line in mount.stdout.splitlines() if line.split() and line.split()[0] in devices]
            self.receipt["filesystems"][name]["mount"] = entries or "unavailable: no matching mount entry"
        memory = ["vm_stat"] if os.uname().sysname == "Darwin" else ["free", "-b"]
        try:
            result = subprocess.run(memory, capture_output=True, text=True, timeout=10)
            self.receipt["memory_snapshot"] = {"command": memory, "exit": result.returncode,
                                                 "output": result.stdout[-8192:]}
        except (OSError, subprocess.SubprocessError) as error:
            self.receipt["memory_snapshot"] = {"unavailable": type(error).__name__}

    def runtime_identity(self, executable):
        if any(value for name, value in os.environ.items() if name.startswith("DYLD_") or name in ("LD_LIBRARY_PATH", "LD_PRELOAD", "LD_AUDIT")):
            raise ValueError("loader overrides prevent frozen-executable qualification")
        system = os.uname()
        command = ["otool", "-L", str(executable)] if system.sysname == "Darwin" else ["ldd", str(executable)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=True)
        lines = result.stdout.splitlines()[1:] if system.sysname == "Darwin" else result.stdout.splitlines()
        libraries = {}
        for line in lines:
            paths = re.findall(r"(?:^|\s)(/[^\s]+)", line)
            if not paths and ("@" in line or "not found" in line):
                raise ValueError("unresolved library prevents frozen-executable qualification")
            for name in paths:
                path = Path(name)
                if path.is_file():
                    with path.open("rb") as stream:
                        libraries[name] = hashlib.file_digest(stream, "sha256").hexdigest()
                elif system.sysname == "Darwin" and name.startswith(("/usr/lib/", "/System/Library/")):
                    libraries[name] = {"shared_cache": line.strip()}
                else:
                    raise ValueError("missing frozen-executable library")
        if not libraries:
            raise ValueError("empty frozen-executable library inventory")
        fixtures = ("app-kit/tests/workflow_execution.rs", "app-kit/tests/observability_support/mod.rs")
        identity = {"platform": list(system), "working_directory": str(self.root / "app-kit"),
                    "libraries": libraries,
                    "fixtures": {name: hashlib.sha256((self.root / name).read_bytes()).hexdigest() for name in fixtures},
                    "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        identity["runtime_environment"] = {name: hashlib.sha256(value.encode()).hexdigest()
            for name, value in os.environ.items()
            if name.startswith(("LANCE_", "LANCEDB_", "ARROW_", "DATAFUSION_", "RAYON_", "TOKIO_"))
            or name in ("TMPDIR", "TEMP", "TMP")}
        if system.sysname == "Darwin":
            identity["os_build"] = subprocess.check_output(["sw_vers", "-buildVersion"], text=True, timeout=10).strip()
        return identity

    def validate_artifact(self, target, artifact):
        profile, runtime = artifact["profile"], artifact["runtime_profiles"]
        expected_opt = "3" if self.profile == "capacity" else "0"
        if profile.get("opt_level") != expected_opt or any(profile.get(key) is not True for key in ("debug_assertions", "overflow_checks", "test")):
            raise ValueError("compiled artifact differs from the required profile: " + target)
        for name in self.RUNTIME:
            if not any(candidate.get("opt_level") == expected_opt and candidate.get("debug_assertions") is True and candidate.get("overflow_checks") is True for candidate in runtime.get(name, [])):
                raise ValueError("runtime artifact differs from the required profile: " + name)
        executable = Path(artifact["executable"])
        if not executable.is_absolute() or not executable.is_file():
            raise ValueError("compiled test executable is missing: " + target)
        with executable.open("rb") as binary:
            digest = hashlib.file_digest(binary, "sha256").hexdigest()
        if artifact.get("sha256", digest) != digest:
            raise ValueError("compiled test executable changed: " + target)
        artifact["sha256"] = digest
        return executable

    def build(self, target):
        if self.baseline_receipt is not None:
            path = Path(self.baseline_receipt).resolve()
            data = path.read_bytes()
            baseline = json.loads(data)
            required = {"source_before", "source_after", "compiler", "compiler_overrides", "artifacts", "baseline_runtime", target + "-full-inventory"}
            if not isinstance(baseline, dict) or not required <= baseline.keys():
                raise ValueError("baseline receipt is missing qualification evidence")
            if (baseline.get("status") != "PASS" or baseline.get("scope") != "diagnostic"
                    or baseline.get("profile") != self.profile or baseline.get("case") != self.case
                    or not baseline.get("retained_baseline") or baseline.get("process_cleanup_errors")
                    or baseline.get("interrupted") is not None
                    or baseline.get("source_before") != baseline.get("source_after")):
                raise ValueError("baseline is not a settled matching diagnostic PASS")
            source = baseline["source_before"]
            if not isinstance(source, dict) or not source.get("lock_sha256") or not source.get("candidate_sha256"):
                raise ValueError("baseline receipt has no original source identity")
            if (source["lock_sha256"] != self.receipt["source_before"]["lock_sha256"]
                    or baseline["compiler"] != self.receipt["compiler"]
                    or baseline["compiler_overrides"] != self.receipt["compiler_overrides"]):
                raise ValueError("baseline lock/compiler configuration differs")
            artifact = baseline["artifacts"][target]
            executable = self.validate_artifact(target, artifact)
            if executable.resolve().parent != path.parent or self.runtime_identity(executable) != baseline["baseline_runtime"]:
                raise ValueError("baseline fixture or runtime identity differs")
            self.receipt["artifacts"][target] = artifact
            self.receipt["executable_source"] = source
            self.receipt["baseline"] = {"receipt": str(path), "sha256": hashlib.sha256(data).hexdigest(),
                "inventory": baseline[target + "-full-inventory"], "runtime": baseline["baseline_runtime"],
                "build": "reused frozen verified executable; current source was not compiled"}
            self.receipt["qualification_scope"] = "frozen baseline diagnostic; not current-source qualification"
            self.save()
            return [str(executable)]
        output = self.command(target + "-build", ["cargo", "test", "--locked", "-p", "app-kit",
                              "--profile", self.profile, "--jobs", "1", "--test", target, "--no-run",
                              "--message-format=json"], 1800)
        artifacts = []
        runtime = {name: [] for name in self.RUNTIME}
        for line in output.splitlines():
            if not line.startswith("{"):
                continue
            item = json.loads(line)
            name = item.get("target", {}).get("name")
            if item.get("reason") == "compiler-artifact" and name in runtime and item.get("target", {}).get("kind") == ["lib"]:
                runtime[name].append(item["profile"])
            if item.get("reason") == "compiler-artifact" and item.get("target", {}).get("name") == target and item["target"].get("kind") == ["test"] and item.get("executable"):
                artifacts.append(item)
        if len(artifacts) != 1:
            raise ValueError("expected exactly one compiled test artifact for " + target)
        artifact = {"executable": artifacts[0]["executable"], "profile": artifacts[0]["profile"],
                    "runtime_profiles": runtime}
        executable = self.validate_artifact(target, artifact)
        if self.retain_baseline:
            retained = self.directory / ("retained-" + target)
            with executable.open("rb") as source, retained.open("xb") as destination:
                shutil.copyfileobj(source, destination)
            retained.chmod(0o500)
            artifact["build_executable"] = str(executable)
            artifact["executable"] = str(retained)
            executable = self.validate_artifact(target, artifact)
            self.receipt["baseline_runtime"] = self.runtime_identity(executable)
            self.receipt["retained_baseline"] = True
        self.receipt["artifacts"][target] = artifact
        self.save()
        return [str(executable)]

    def execution_command(self, arguments):
        if "execution_resources" not in self.receipt:
            tool = Path("/usr/bin/time")
            measurement = {"unavailable": "ordinary platform time tool unavailable"}
            if tool.is_file() and os.uname().sysname in ("Darwin", "Linux"):
                prefix = [str(tool), "-l" if os.uname().sysname == "Darwin" else "-v"]
                try:
                    probe = subprocess.run([*prefix, "/usr/bin/true"], capture_output=True, text=True, timeout=10)
                    if probe.returncode == 0 and "maximum resident set size" in probe.stderr.lower():
                        measurement = {"tool": prefix,
                            "scope": "test command only; build/listing excluded; descendant accounting follows the platform tool"}
                    else:
                        measurement = {"unavailable": "resource tool prerequisite failed", "probe_exit": probe.returncode,
                            "probe_stderr": probe.stderr[-2048:]}
                except (OSError, subprocess.SubprocessError) as error:
                    measurement = {"unavailable": type(error).__name__}
            self.receipt["execution_resources"] = measurement
        prefix = self.receipt["execution_resources"].get("tool", [])
        return [*prefix, *arguments]

    def stop(self, number, _frame):
        self.interrupted = number

    def settle(self):
        if self.child is None:
            return
        child, self.child = self.child, None
        try:
            # Settle the entire invocation-owned group even if its leader exited.
            for number, seconds in ((signal.SIGTERM, 20), (signal.SIGKILL, 10)):
                try:
                    os.killpg(child.pid, number)
                except ProcessLookupError:
                    break
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline:
                    child.poll()
                    try:
                        os.killpg(child.pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.1)
                else:
                    continue
                break
            else:
                raise RuntimeError("invocation-owned process group did not settle")
            child.wait(timeout=10)
        except (OSError, RuntimeError, subprocess.SubprocessError) as error:
            self.receipt["process_cleanup_errors"].append(type(error).__name__)
            raise

    def command(self, label, arguments, budget):
        if self.interrupted is not None:
            raise InterruptedError("interrupted before command admission")
        log = self.directory / (label + ".log")
        item = {"label": label, "command": arguments, "outer_budget_seconds": budget}
        self.receipt["commands"].append(item)
        if self.retain_baseline or self.baseline_receipt is not None:
            for target, artifact in self.receipt["artifacts"].items():
                if artifact["executable"] in arguments:
                    self.validate_artifact(target, artifact)
        is_artifact = any(item["executable"] in arguments for item in self.receipt["artifacts"].values())
        working_directory = self.root / "app-kit" if is_artifact else self.root
        item["working_directory"] = str(working_directory)
        started = time.monotonic()
        primary = None
        cpu_before = None
        execution = label.endswith("-execution")
        direct = is_artifact and any(artifact["executable"] == arguments[0] for artifact in self.receipt["artifacts"].values())
        with log.open("wb") as output:
            try:
                if execution:
                    item["child_cpu"] = {"unavailable": "execution is not a direct verified test child"}
                    if direct:
                        try:
                            cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
                        except (OSError, ValueError) as error:
                            item["child_cpu"] = {"unavailable": type(error).__name__}
                self.child = subprocess.Popen(arguments, cwd=working_directory, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
                process = self.child
                item["owned_process_group"] = process.pid
                self.receipt["active_command"] = label
                self.save()
                while process.poll() is None:
                    if self.interrupted is not None:
                        raise InterruptedError("interrupted while command was owned")
                    if budget is not None and time.monotonic() - started >= budget:
                        item["timed_out"] = True
                        raise TimeoutError("outer observation budget expired")
                    time.sleep(0.2)
                item["exit"] = process.returncode
                if process.returncode != 0:
                    self.receipt["status"] = "FAIL"
                    raise RuntimeError(f"{label} failed: exit={process.returncode}")
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                primary = error
                item["error"] = {"category": type(error).__name__, "message": str(error)}
            finally:
                try:
                    self.settle()
                except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                    item["cleanup_error"] = type(error).__name__
                    if primary is None:
                        primary = error
                finally:
                    if cpu_before is not None:
                        if "cleanup_error" in item or "exit" not in item:
                            item["child_cpu"] = {"unavailable": "normal child completion and owned settlement were not established"}
                        else:
                            try:
                                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                                user, system = after.ru_utime - cpu_before.ru_utime, after.ru_stime - cpu_before.ru_stime
                                if user < 0 or system < 0:
                                    raise ValueError("child CPU counters decreased")
                                item["child_cpu"] = {"user_seconds": user, "system_seconds": system,
                                    "scope": "direct test child spawn through wait/reap; compilation and inventory excluded; all child threads, no owner attribution"}
                            except (OSError, ValueError) as error:
                                item["child_cpu"] = {"unavailable": type(error).__name__}
                    item["elapsed_seconds"] = time.monotonic() - started
                    tail = log.read_text(errors="replace")
                    phases = [line[:1024] for line in tail.splitlines() if any(marker in line for marker in ("phase=", "settlement phase:", "settlement phase observation:", "delivery-prefix milestone=", "delivery-growth milestone="))]
                    item["last_observed_phase"] = phases[-1] if phases else None
                    summaries = self.SUMMARY.findall(tail)
                    item["observed_counts"] = list(map(int, summaries[-1])) if summaries else None
                    if label.endswith("-execution"):
                        resources = [line.strip() for line in tail.splitlines() if "maximum resident set size" in line.lower()]
                        item["maximum_resident_set_size_raw"] = resources[-1] if resources else "unavailable; see raw command log"
                    self.receipt.pop("active_command", None)
                    self.save()
        if primary is not None:
            raise primary
        print(f"{label}: exit={item['exit']}; log={log}", flush=True)
        return log.read_text(errors="replace")

    def inventory(self, label, command):
        text = self.command(label, [*command, "--list", "--format", "terse"], 1800)
        names = re.findall(r"^([^\r\n]+): test$", text, re.M)
        if not names or len(names) != len(set(names)):
            raise ValueError("empty or duplicate test inventory")
        self.receipt[label] = names
        return set(names)

    def run(self):
        for number in (signal.SIGINT, signal.SIGTERM):
            signal.signal(number, self.stop)
        print("Evidence: " + str(self.directory), flush=True)
        try:
            self.receipt["source_before"] = self.identity()
            self.preflight()
            self.environment()
            compiler = self.command("compiler", ["rustc", "-Vv"], 30)
            self.receipt["compiler"] = compiler
            if "release: 1.99.0\n" not in compiler:
                raise ValueError("Rust 1.99.0 is required")
            inventories = {}
            for target in ("workflow_execution", "worker_contract"):
                if self.mode == "diagnostic" and target == "worker_contract":
                    continue
                if self.mode == "capacity" and self.case is not None and not any(owner == target and name == self.case for owner, name, _ in self.CAPACITY):
                    continue
                command = self.build(target)
                full = self.inventory(target + "-full-inventory", command)
                if self.baseline_receipt is not None and full != set(self.receipt["baseline"]["inventory"]):
                    raise ValueError("frozen executable inventory differs from the original receipt")
                required = {case for owner, case, _ in self.CAPACITY if owner == target}
                if target == "workflow_execution":
                    required.update((self.ROUTINE, self.DIAGNOSTIC, self.GROWTH))
                    if self.mode in ("routine", "diagnostic"):
                        required.update(self.CALIBRATORS)
                if not required <= full:
                    raise ValueError("required workload tier is missing from " + target)
                inventories[target] = (command, full)
            selections = []
            if self.mode == "routine":
                for target, (command, full) in inventories.items():
                    excluded = {case for owner, case, _ in self.CAPACITY if owner == target}
                    if target == "workflow_execution":
                        excluded.update((self.DIAGNOSTIC, self.GROWTH))
                        excluded.update(self.CALIBRATORS)
                    selected = list(command)
                    for name in sorted(excluded):
                        selected.extend(("--skip", name))
                    # Preserve check.sh's existing observation policy for routine targets.
                    selections.append((target, selected, full - excluded, excluded, None))
            else:
                if self.mode == "diagnostic":
                    command, full = inventories["workflow_execution"]
                    for name in self.CALIBRATORS:
                        # These test the measurement owner, not App behavior.
                        # Keep their former routine supervision policy and run
                        # them separately from measured workload execution.
                        selections.append(("diagnostic-preflight-" + name,
                            [*command, name, "--exact"], {name}, full - {name}, None))
                cases = tuple(row for row in self.CAPACITY if self.case is None or row[1] == self.case) if self.mode == "capacity" else (("workflow_execution", self.case, 2400),)
                for target, name, budget in cases:
                    command, full = inventories[target]
                    selections.append((name, [*command, name, "--exact"], {name}, full - {name}, budget))
            self.receipt["selections"] = []
            for label, selected, expected, excluded, budget in selections:
                actual = self.inventory(label + "-selected-inventory", selected)
                if actual != expected:
                    raise ValueError("selected inventory differs from the explicit scope: " + label)
                purpose = "diagnostic-preflight" if label.startswith("diagnostic-preflight-") else "workload"
                self.receipt["selections"].append({"label": label, "purpose": purpose, "selected": sorted(actual), "excluded": sorted(excluded), "outer_budget_seconds": budget})
            for label, selected, expected, excluded, budget in selections:
                print(f"Scope {self.mode}/{label}: {len(expected)} selected; {len(excluded)} explicitly excluded", flush=True)
                arguments = [*selected, "--nocapture", "--test-threads=1"]
                if label.startswith("diagnostic-preflight-"):
                    text = self.command(label + "-check", arguments, budget)
                else:
                    text = self.command(label + "-execution", self.execution_command(arguments), budget)
                rows = self.SUMMARY.findall(text)
                counts = tuple(map(int, rows[-1])) if rows else None
                if counts != (len(expected), 0, 0, 0, len(excluded)):
                    raise ValueError("execution did not pass its exact nonempty inventory: " + label)
            self.receipt["status"] = "PASS"
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            self.receipt["error"] = {"category": type(error).__name__, "message": str(error)}
        finally:
            try:
                self.settle()
                if self.receipt.get("baseline_runtime") or self.receipt.get("baseline"):
                    artifact = self.receipt["artifacts"]["workflow_execution"]
                    self.validate_artifact("workflow_execution", artifact)
                    runtime = self.runtime_identity(Path(artifact["executable"]))
                    original = self.receipt.get("baseline_runtime", self.receipt.get("baseline", {}).get("runtime"))
                    self.receipt["runtime_after"] = runtime
                    if runtime != original:
                        self.receipt["status"] = "INCOMPLETE"
                self.receipt["source_after"] = self.identity()
                if self.receipt.get("source_before") != self.receipt["source_after"]:
                    self.receipt["status"] = "INCOMPLETE"
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                self.receipt["finalization_error"] = type(error).__name__
                self.receipt["status"] = "INCOMPLETE"
            self.receipt["interrupted"] = self.interrupted
            if self.interrupted is not None or self.receipt["process_cleanup_errors"]:
                self.receipt["status"] = "INCOMPLETE"
            self.save()
        print("App workload scope " + self.mode + ": " + self.receipt["status"], flush=True)
        return 0 if self.receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scope", choices=("routine", "diagnostic", "capacity"))
    parser.add_argument("--profile", choices=("test", "capacity"), help="diagnostics only; routine/test and capacity/capacity are fixed")
    parser.add_argument("--case", help="one exact diagnostic or maximum-capacity case; defaults to prefix diagnostic or all capacity cases")
    parser.add_argument("--retain-baseline", action="store_true", help="diagnostics only: retain the verified executable for later same-fixture comparison")
    parser.add_argument("--baseline-receipt", type=Path, help="diagnostics only: reuse a retained executable from a settled matching PASS receipt")
    arguments = parser.parse_args()
    try:
        owner = AppWorkloads(Path(__file__).resolve().parents[2], arguments.scope, arguments.profile, arguments.case,
                             arguments.retain_baseline, arguments.baseline_receipt)
    except ValueError as error:
        parser.error(str(error))
    raise SystemExit(owner.run())

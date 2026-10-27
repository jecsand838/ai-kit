#!/usr/bin/env python3
"""Explicit local-only dependency regressions; never part of mock CI discovery."""

import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
import uuid

if __package__:
    from .mock_contract_runner import Runner
    from .security_dependencies import SecurityPatches
else:
    from mock_contract_runner import Runner
    from security_dependencies import SecurityPatches


class Compatibility:
    TARGETS = {
        "compatibility": (
            "zstd_reads_pre_update_frame_and_round_trips",
            "include_flate_reads_pre_update_frame_and_round_trips",
            "arrow_reads_pre_update_compressed_ipc_and_round_trips",
            "object_store_reads_escaped_s3_xml",
            "object_store_rejects_malformed_xml_and_unresolved_entities",
        ),
        "geometry": (
            "overlapping_rectangles_preserve_boolean_areas_and_regions",
            "polygon_holes_and_boundaries_preserve_contains_and_intersects",
            "square_capped_buffer_preserves_extents_and_area",
        ),
    }

    def __init__(self, root, output=None):
        self.root = Path(root).resolve()
        self.fixtures = self.root / "scripts/tests/fixtures/security"
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.output = Path(output).resolve() if output else self.root / "target/security-compatibility" / (stamp + "-" + uuid.uuid4().hex[:8])
        self.output.mkdir(parents=True, exist_ok=False)
        self.report = {"schema": 1, "status": "INCOMPLETE", "scope": "local dependency compatibility", "stages": []}
        self.environment = dict(os.environ, CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1",
                                CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0",
                                CARGO_TARGET_DIR=str(self.root / "target"),
                                AI_KIT_SECURITY_FIXTURES=str(self.fixtures))

    def save(self):
        (self.output / "report.json").write_text(json.dumps(self.report, indent=2) + "\n")

    def fixture_identity(self):
        metadata = json.loads((self.fixtures / "fixtures.json").read_text())
        if metadata.get("schema") != 1 or metadata.get("baseline_commit") != "061f62b":
            raise ValueError("unrecognized legacy-fixture provenance")
        expected = {"legacy-writer.rs", "legacy-Cargo.toml", "legacy-Cargo.lock", "payload.txt",
                    "zstd-0.13.3.zst", "include-flate-compress-0.3.2.zst", "arrow-ipc-58.3.0-zstd.arrow"}
        if set(metadata.get("files", {})) != expected:
            raise ValueError("legacy-fixture inventory differs")
        for name, checksum in metadata["files"].items():
            file = self.fixtures / name
            if file.is_symlink() or not file.is_file() or SecurityPatches.digest(file) != checksum:
                raise ValueError(f"legacy fixture changed or missing: {name}")
        inputs = (*sorted(expected), "fixtures.json", "compatibility.rs", "geometry.rs")
        return {name: SecurityPatches.digest(self.fixtures / name) for name in inputs}

    def prepare(self):
        package = self.output / "package"
        (package / "tests").mkdir(parents=True)
        for source in ("compatibility.rs", "geometry.rs"):
            shutil.copyfile(self.fixtures / source, package / "tests" / source)
        manifest = '''[package]
name = "ai-kit-security-compatibility"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
arrow-array = "=58.4.1"
arrow-schema = "=58.4.1"
arrow-ipc = { version = "=58.4.1", features = ["zstd"] }
include-flate-compress = { version = "=0.3.4", default-features = false, features = ["zstd"] }
object_store = { version = "=0.13.2", default-features = false, features = ["aws"] }
geo = { version = "=0.32.0", default-features = false, features = ["use-serde"] }
tokio = { version = "=1.54.1", features = ["macros", "rt", "time"] }
zstd = "=0.14.3"

[workspace]

[patch.crates-io]
'''
        for name, version in (("arrow-ipc", "58.4.1"), ("include-flate-compress", "0.3.4"),
                              ("object_store", "0.13.2"), ("geo", "0.32.0")):
            path = self.root / "third-party/security" / (name + "-" + version)
            manifest += f'{name} = {{ path = {json.dumps(str(path))} }}\n'
        (package / "Cargo.toml").write_text(manifest)
        return package

    def command(self, name, command, timeout):
        log = self.output / (name + ".log")
        stage = {"name": name, "command": command, "log": str(log), "status": "INCOMPLETE"}
        self.report["stages"].append(stage)
        self.save()
        print(f"==> {name}: {' '.join(command)}", flush=True)
        try:
            with log.open("w") as capture:
                result = Runner.owned_process(command, cwd=self.root, env=self.environment,
                                              stdout=capture, stderr=subprocess.STDOUT,
                                              text=True, timeout=timeout, check=False)
            stage["returncode"] = result.returncode
            stage["status"] = "PASS" if result.returncode == 0 else "FAIL"
            if result.returncode:
                raise ValueError(f"{name} failed; see {log}")
        finally:
            self.save()
            if log.exists():
                print(log.read_text(errors="replace"), end="", flush=True)
        return stage, log.read_text()

    @classmethod
    def inventory(cls, target, output):
        names = Runner.inventory(output)
        if sorted(names) != sorted(cls.TARGETS[target]):
            raise ValueError(f"{target} must retain its complete, nonempty dependency regression inventory")
        return names

    @staticmethod
    def execution(stage, output, inventory):
        Runner.validate_execution(stage, output, inventory)
        if stage["status"] != "PASS":
            raise ValueError(stage.get("error", "dependency regression execution failed"))

    def verify_lock(self, lockfile):
        packages = tomllib.loads(lockfile.read_text())["package"]
        versions = {}
        for package in packages:
            versions.setdefault(package["name"], set()).add(package["version"])
        required = {"arrow-ipc": "58.4.1", "include-flate-compress": "0.3.4", "object_store": "0.13.2",
                    "geo": "0.32.0", "zstd": "0.14.3", "zstd-safe": "8.2.0",
                    "zstd-sys": "2.1.2+zstd.1.5.7", "quick-xml": "0.41.0", "i_overlay": "4.5.2"}
        for name, version in required.items():
            if versions.get(name) != {version}:
                raise ValueError(f"compatibility graph must test {name} {version}, found {versions.get(name)}")
        self.report["versions"] = required
        self.report["lockfile_sha256"] = SecurityPatches.digest(lockfile)

    def run(self):
        self.save()
        try:
            patches = SecurityPatches(self.root)
            self.report["patch_fingerprint"] = patches.fingerprint()
            self.report["fixture_inputs"] = self.fixture_identity()
            package = self.prepare()
            toolchain = tomllib.loads((self.root / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
            cargo = ["cargo", "+" + toolchain]
            manifest = ["--manifest-path", str(package / "Cargo.toml")]
            self.command("resolve-local-harness", cargo + ["generate-lockfile", *manifest], 300)
            self.verify_lock(package / "Cargo.lock")
            for target in self.TARGETS:
                base = cargo + ["test", "--locked", *manifest, "--test", target, "--"]
                listing, output = self.command(target + "-inventory", base + ["--list", "--format", "terse"], 1200)
                inventory = self.inventory(target, output)
                listing["inventory"] = inventory
                stage, output = self.command(target, base + ["--test-threads=1"], 1200)
                self.execution(stage, output, inventory)
            if self.fixture_identity() != self.report["fixture_inputs"] or patches.fingerprint() != self.report["patch_fingerprint"]:
                raise ValueError("compatibility inputs changed during validation")
            self.report["status"] = "PASS"
        except (OSError, ValueError, subprocess.SubprocessError, KeyboardInterrupt) as error:
            self.report["error"] = str(error)
            self.report["status"] = "FAIL"
            print(error, file=sys.stderr)
        finally:
            self.save()
        print(f"{self.report['status']}: local security dependency compatibility; evidence: {self.output}", flush=True)
        return 0 if self.report["status"] == "PASS" else 1

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
        parser.add_argument("--root", default=str(Path(__file__).resolve().parent.parent))
        parser.add_argument("--output", help="new evidence directory; existing directories are rejected")
        args = parser.parse_args()
        return cls(args.root, args.output).run()


if __name__ == "__main__":
    sys.exit(Compatibility.main())

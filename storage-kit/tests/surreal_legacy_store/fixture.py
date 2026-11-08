"""Owned, digest-pinned 3.3 reader and fixture generation for the 4.0 boundary."""

import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid


class LegacyStore:
    IMAGE = "surrealdb/surrealdb@sha256:681c6c22c287421b5c7d99e0fde79b6e0d32c36c1ddeaab2762a1661cb04cd20"
    FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/surrealdb-3.3.0-surrealkv"
    SEED = """DEFINE NAMESPACE vendor_migration;
USE NS vendor_migration;
DEFINE DATABASE surreal_330;
USE DB surreal_330;
DEFINE TABLE records SCHEMAFULL;
DEFINE FIELD label ON records TYPE string;
DEFINE FIELD sequence ON records TYPE int;
CREATE records:a SET label = 'alpha', sequence = 1;
CREATE records:b SET label = 'beta', sequence = 2;
"""
    VERIFY = "SELECT * FROM records ORDER BY sequence; INFO FOR DB; INFO FOR TABLE records;"
    RECORDS = [
        {"id": "records:a", "label": "alpha", "sequence": 1},
        {"id": "records:b", "label": "beta", "sequence": 2},
    ]

    def __init__(self, directory, evidence):
        self.directory = Path(directory).resolve()
        self.evidence = Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.name = "storage-legacy-" + uuid.uuid4().hex
        self.receipt = {"image": self.IMAGE, "container": self.name, "commands": []}
        self.endpoint = None
        self.settled = False
        self.owns_container = False

    def command(self, *args, timeout=30):
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        self.receipt["commands"].append({"args": args, "exit": result.returncode,
                                         "stdout": result.stdout, "stderr": result.stderr})
        self.save()
        if result.returncode:
            raise RuntimeError(f"{args}: exit {result.returncode}: {result.stderr}")
        return result.stdout.strip()

    def save(self):
        (self.evidence / "receipt.json").write_text(json.dumps(self.receipt, indent=2) + "\n")

    def request(self, path, body=None, timeout=10):
        headers = {"Accept": "application/json", "Surreal-NS": "vendor_migration",
                   "Surreal-DB": "surreal_330",
                   "Authorization": "Basic " + base64.b64encode(b"root:fixture-secret").decode()}
        command = ["curl", "--fail", "--silent", "--show-error", "--connect-timeout", "2", "--max-time", str(timeout)]
        for name, value in headers.items():
            command.extend(["--header", name + ": " + value])
        if body is not None:
            command.extend(["--data-binary", "@-"])
        command.append(self.endpoint + path)
        result = subprocess.run(command, input=body, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(f"native request failed: {result.returncode}: {result.stderr}")
        value = result.stdout
        if body is None:
            return value
        result = json.loads(value)
        if not result or any(row.get("status") != "OK" for row in result):
            raise RuntimeError(f"native query failed: {result}")
        return [row["result"] for row in result]

    def start(self):
        deadline = time.monotonic() + 60
        self.directory.mkdir(parents=True, exist_ok=True)
        identity = json.loads(self.command("docker", "image", "inspect", self.IMAGE, timeout=max(0.1, deadline-time.monotonic())))
        self.receipt["image_identity"] = identity
        self.owns_container = True
        self.command("docker", "create", "--name", self.name, "--user", "0:0",
                     "--publish", "127.0.0.1::8000", "--mount",
                     f"type=bind,source={self.directory},target=/data", self.IMAGE,
                     "start", "--user", "root", "--pass", "fixture-secret", "surrealkv:/data/store", timeout=max(0.1, deadline-time.monotonic()))
        self.command("docker", "start", self.name, timeout=max(0.1, deadline-time.monotonic()))
        port = self.command("docker", "port", self.name, "8000/tcp", timeout=max(0.1, deadline-time.monotonic()))
        if not port.startswith("127.0.0.1:") or "\n" in port:
            raise RuntimeError(f"unexpected owned port: {port}")
        self.endpoint = "http://" + port
        while True:
            try:
                self.request("/ready", timeout=min(10, max(0.1, deadline-time.monotonic())))
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
        version = self.command("docker", "exec", self.name, "/surreal", "version", timeout=max(0.1, deadline-time.monotonic()))
        if version.split()[0] != "3.3.0":
            raise RuntimeError(f"wrong legacy version: {version}")
        self.receipt["version"] = version
        self.save()

    def verify(self, expected=None):
        actual = self.request("/sql", self.VERIFY)
        if actual[0] != self.RECORDS:
            raise AssertionError(f"legacy records differ: {actual}")
        if set(actual[1]["tables"]) != {"records"}:
            raise AssertionError(f"legacy catalog differs: {actual}")
        if set(actual[2]["fields"]) != {"label", "sequence"}:
            raise AssertionError(f"legacy fields differ: {actual}")
        if expected is not None and actual != expected:
            raise AssertionError(f"legacy exact records/catalog differ: {actual} != {expected}")
        self.receipt["verified"] = actual
        self.save()
        return actual

    def close(self):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        errors = []
        if not self.owns_container:
            self.settled = True
            self.receipt.update(settled=True, cleanup_errors=[])
            self.save()
            return errors
        # A failed create/start still leaves cleanup authority over this unique name.
        try:
            state = json.loads(self.command("docker", "inspect", self.name))[0]["State"]
            if state["Running"]:
                try:
                    self.command("docker", "stop", "--time", "10", self.name, timeout=20)
                except Exception as error:
                    errors.append(str(error))
                    self.command("docker", "kill", self.name, timeout=10)
            state = json.loads(self.command("docker", "inspect", self.name))[0]["State"]
            if state["Running"]:
                raise RuntimeError("owned old reader still running; retain directory")
            self.receipt["terminal_state"] = state
            if state["ExitCode"] != 0:
                errors.append(f"old reader exit {state['ExitCode']}")
            self.command("docker", "logs", self.name)
            self.command("docker", "rm", "--volumes", self.name)
            self.settled = True
        except Exception as error:
            errors.append(str(error))
        self.receipt["cleanup_errors"] = errors
        self.receipt["settled"] = self.settled
        self.save()
        return errors

    @staticmethod
    def interrupted(signum, _frame):
        raise InterruptedError(f"owned fixture interrupted by signal {signum}")

    @staticmethod
    def inventory(directory):
        return {str(p.relative_to(directory)): {"length": p.stat().st_size,
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                for p in sorted(directory.rglob("*")) if p.is_file()}

    @classmethod
    def generate(cls):
        if cls.FIXTURE.exists():
            raise RuntimeError("sealed fixture already exists; never overwrite it")
        cls.FIXTURE.mkdir(parents=True)
        native = cls.FIXTURE / "native"
        server = cls(native, cls.FIXTURE / "generation")
        primary = None
        try:
            server.start()
            server.request("/sql", cls.SEED)
            expected = server.verify()
        except Exception as error:
            primary = repr(error)
        errors = server.close()
        if primary or errors:
            raise RuntimeError(f"generation primary={primary}; cleanup={errors}")
        (cls.FIXTURE / "seed.surql").write_text(cls.SEED)
        (cls.FIXTURE / "verify.surql").write_text(cls.VERIFY + "\n")
        (cls.FIXTURE / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")
        manifest = {"generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "platform": platform.platform(), "image": cls.IMAGE,
                    "generation_command": "python3 storage-kit/tests/surreal_legacy_store/fixture.py generate",
                    "source_archive_sha256": "507ff33e50fece0cdd732198adc71a347a91f702baecbd6b2cb46c40d3034cc4",
                    "files": cls.inventory(native),
                    "generation_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    "support_files": {name: hashlib.sha256((cls.FIXTURE / name).read_bytes()).hexdigest()
                                      for name in ["seed.surql", "verify.surql", "expected.json", "generation/receipt.json"]}}
        (cls.FIXTURE / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        (cls.FIXTURE / "manifest.sha256").write_text(hashlib.sha256((cls.FIXTURE / "manifest.json").read_bytes()).hexdigest()+"\n")
        cls.check(None)

    @classmethod
    def fresh(cls, executable):
        owned = Path(tempfile.mkdtemp(prefix="storage-fresh-reopen-"))
        snapshots = []
        for phase in ("seed", "read"):
            environment = {k: v for k, v in os.environ.items() if "EXPERIMENTAL_UPGRADE" not in k.upper()}
            environment.update(STORAGE_FRESH_STORE=str(owned / "store"), STORAGE_FRESH_PHASE=phase,
                               STORAGE_FRESH_OUTCOME=str(owned / (phase + ".json")))
            result = subprocess.run([executable, "surreal_store_reopen::surrealdb_fresh_surrealkv_cold_reopen",
                                     "--exact", "--nocapture"], env=environment,
                                    capture_output=True, text=True, timeout=60)
            (owned / (phase + ".log")).write_text(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f"fresh {phase} failed: {result.stdout} {result.stderr}; evidence={owned}")
            snapshots.append(json.loads((owned / (phase + ".json")).read_text()))
        if snapshots[0] != snapshots[1]:
            raise AssertionError(f"cold public records/catalog differ: {snapshots}; evidence={owned}")
        print(json.dumps({"fresh_cold_reopen": "PASS", "evidence": str(owned)}))

    @classmethod
    def check(cls, executable):
        manifest = json.loads((cls.FIXTURE / "manifest.json").read_text())
        if hashlib.sha256((cls.FIXTURE / "manifest.json").read_bytes()).hexdigest() != (cls.FIXTURE / "manifest.sha256").read_text().strip():
            raise AssertionError("fixture manifest hash differs")
        original = cls.FIXTURE / "native"
        if manifest["image"] != cls.IMAGE or cls.inventory(original) != manifest["files"]:
            raise AssertionError("fixture identity differs")
        for name, digest in manifest["support_files"].items():
            if hashlib.sha256((cls.FIXTURE / name).read_bytes()).hexdigest() != digest:
                raise AssertionError(f"fixture provenance differs: {name}")
        expected = json.loads((cls.FIXTURE / "expected.json").read_text())
        signal.signal(signal.SIGTERM, cls.interrupted)
        signal.signal(signal.SIGINT, cls.interrupted)
        owned = Path(tempfile.mkdtemp(prefix="storage-legacy-copy-"))
        shutil.copytree(original, owned / "native")
        server = cls(owned / "native", owned / "reader")
        primary = None
        try:
            if executable:
                environment = {k: v for k, v in os.environ.items() if "EXPERIMENTAL_UPGRADE" not in k.upper()}
                environment["STORAGE_LEGACY_ATTEMPT"] = str(owned / "native/store")
                environment["STORAGE_LEGACY_OUTCOME"] = str(owned / "outcome.json")
                try:
                    result = subprocess.run([executable, "surreal_legacy_store::surrealdb_legacy_store_requires_native_upgrade", "--exact", "--nocapture"],
                                            env=environment, capture_output=True, text=True, timeout=30)
                    (owned / "attempt.log").write_text(result.stdout + result.stderr)
                    if result.returncode:
                        primary = f"public child failed: {result.returncode}: {result.stdout} {result.stderr}"
                    else:
                        outcome = json.loads((owned / "outcome.json").read_text())
                        message = outcome.get("error", "")
                        if outcome.get("operation") != "connect" or not all(fragment in message for fragment in ["written by SurrealDB 3.x", "experimental and is refused by default"]):
                            primary = f"specific native upgrade refusal required; observed {outcome}"
                except InterruptedError:
                    raise
                except subprocess.TimeoutExpired as error:
                    # subprocess.run kills and reaps the direct Rust child before returning.
                    primary = f"public connection exceeded external 30 second bound: {error}"
                except Exception as error:
                    primary = f"invalid public attempt evidence: {error!r}"
            server.start()
            server.verify(expected)
        except Exception as error:
            primary = f"{primary}; old reader/fixture failure: {error!r}"
        errors = server.close()
        if cls.inventory(original) != manifest["files"]:
            errors.append("sealed original changed")
        print(json.dumps({"primary": primary, "cleanup": errors, "evidence": str(owned),
                          "original_preserved": cls.inventory(original) == manifest["files"]}))
        # Receipts and the disposable copy are retained for both red and green evidence.
        if primary or errors:
            raise RuntimeError(f"primary={primary}; cleanup={errors}; evidence={owned}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["generate", "check", "fresh"])
    parser.add_argument("--executable")
    arguments = parser.parse_args()
    signal.signal(signal.SIGTERM, LegacyStore.interrupted)
    signal.signal(signal.SIGINT, LegacyStore.interrupted)
    if arguments.mode == "generate":
        LegacyStore.generate()
    elif arguments.mode == "fresh":
        LegacyStore.fresh(arguments.executable)
    else:
        LegacyStore.check(arguments.executable)

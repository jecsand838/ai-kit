#!/usr/bin/env python3
"""Verify registry dependency sources and retain raw, explicitly qualified audits."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib


REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
POLICY = "scripts/security_exceptions.toml"
ADVISORY = re.compile(r"RUSTSEC-[0-9]{4}-[0-9]{4}")
WORKSPACE_PACKAGES = ("providers-kit", "storage-kit", "tools-kit", "agents-kit", "app-kit")
WORKSPACE_PATHS = {
    "agents-kit": {"path": "agents-kit", "version": "=0.0.1"},
    "providers-kit": {"path": "providers-kit", "version": "0.0.1"},
    "storage-kit": {"path": "storage-kit", "version": "0.0.1", "default-features": False},
    "tools-kit": {"path": "tools-kit", "version": "0.0.1"},
}


class SecurityError(ValueError):
    """The dependency policy or captured audit cannot establish a valid result."""


class SecurityPolicy:
    def __init__(self, root):
        self.root = Path(root).resolve()

    @staticmethod
    def digest(path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def document(self, relative):
        path = self.root / relative
        current = path
        while current != self.root:
            if current.is_symlink():
                raise SecurityError(f"dependency input symlink is forbidden: {relative}")
            current = current.parent
        try:
            return tomllib.loads(path.read_text())
        except (OSError, ValueError) as error:
            raise SecurityError(f"{relative}: {error}") from error

    def advisories(self):
        document = self.document(POLICY)
        values = document.get("advisories")
        if (set(document) != {"advisories"} or not isinstance(values, list)
                or any(not isinstance(item, str) or not ADVISORY.fullmatch(item) for item in values)
                or len(set(values)) != len(values)):
            raise SecurityError(f"{POLICY}: require only an advisories array of unique full RUSTSEC-YYYY-NNNN IDs")
        return values

    def inputs(self):
        self.advisories()
        return (POLICY,)

    @classmethod
    def validate_manifest(cls, document, relative, context=()):
        if not context and any(key in document for key in ("patch", "replace")):
            raise SecurityError(f"{relative}: Cargo dependency patch and replace tables are forbidden")
        if isinstance(document, dict):
            if "path" in document:
                allowed = (relative == "Cargo.toml" and len(context) == 3
                           and context[:2] == ("workspace", "dependencies")
                           and document == WORKSPACE_PATHS.get(context[2]))
                if not allowed:
                    raise SecurityError(f"{relative}: forbidden Cargo path key at {'.'.join(context)}")
            for key, value in document.items():
                cls.validate_manifest(value, relative, (*context, key))
        elif isinstance(document, list):
            for value in document:
                cls.validate_manifest(value, relative, context)

    def verify(self):
        self.advisories()
        self.validate_manifest(self.document("Cargo.toml"), "Cargo.toml")
        local = {}
        for name in WORKSPACE_PACKAGES:
            relative = name + "/Cargo.toml"
            if (self.root / relative).exists():
                document = self.document(relative)
                self.validate_manifest(document, relative)
                package = document.get("package", {})
                if package.get("name") != name or not isinstance(package.get("version"), str):
                    raise SecurityError(f"{relative}: invalid workspace package identity")
                local[name] = package["version"]
        packages = self.document("Cargo.lock").get("package")
        if not isinstance(packages, list) or not packages:
            raise SecurityError("Cargo.lock: package inventory is missing")
        found = set()
        for package in packages:
            if (not isinstance(package, dict) or not isinstance(package.get("name"), str)
                    or not package["name"] or not isinstance(package.get("version"), str)
                    or not package["version"]):
                raise SecurityError("Cargo.lock: malformed package identity")
            name, version, source = package["name"], package["version"], package.get("source")
            if source is None:
                if local.get(name) != version or name in found:
                    raise SecurityError(f"Cargo.lock: foreign or duplicate path dependency {name} {version}")
                found.add(name)
            elif source != REGISTRY:
                raise SecurityError(f"Cargo.lock: non-default registry or Git dependency would escape cargo-audit: {name}")
        if found != set(local):
            raise SecurityError("Cargo.lock: workspace package inventory differs from manifests")

    def identity(self):
        self.verify()
        names = ["Cargo.toml", "Cargo.lock", *self.inputs()]
        names.extend(name + "/Cargo.toml" for name in WORKSPACE_PACKAGES if (self.root / name / "Cargo.toml").is_file())
        return {name: self.digest(self.root / name) for name in names}

    @classmethod
    def main(cls, arguments=None):
        parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
        commands = parser.add_subparsers(dest="command", required=True)
        verify = commands.add_parser("verify", allow_abbrev=False)
        verify.add_argument("--root", required=True)
        audit = commands.add_parser("audit", allow_abbrev=False)
        for name in ("root", "scanner", "database", "output"):
            audit.add_argument("--" + name, required=True)
        args = parser.parse_args(arguments)
        try:
            if args.command == "audit":
                return SecurityAudit(args.root, args.scanner, args.database, args.output).run()
            policy = cls(args.root)
            policy.verify()
            print(f"PASS: registry dependency sources; {len(policy.advisories())} explicit temporary advisory exceptions")
            return 0
        except (OSError, ValueError, KeyboardInterrupt) as error:
            print(f"Security dependency verification failed: {error}", file=sys.stderr)
            return 1


class SecurityAudit:
    def __init__(self, root, scanner, database, output, execute=None):
        self.root = Path(root).resolve()
        for name, value in (("scanner", scanner), ("database", database), ("output", output)):
            if not Path(value).is_absolute():
                raise SecurityError(f"{name} path must be absolute")
        self.scanner = Path(scanner).resolve()
        self.database = Path(database).resolve()
        self.output = Path(output).resolve()
        self.execute = execute or subprocess.run
        self.report = {
            "schema": 2, "status": "INCOMPLETE", "scans": [], "accepted_findings": [],
            "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "policy": {"deny": ["unsound"], "check_yanked": False},
        }

    def save(self):
        destination = self.output / "report.json"
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(self.report, indent=2) + "\n")
        temporary.replace(destination)

    @staticmethod
    def blocking(document, ignored):
        if not isinstance(document, dict):
            raise SecurityError("scanner JSON is not a report")
        settings = document.get("settings")
        if (not isinstance(settings, dict) or settings.get("ignore") != ignored
                or "severity" not in settings or settings["severity"] is not None
                or any(settings.get(name) != [] for name in ("target_arch", "target_os"))
                or not isinstance(settings.get("informational_warnings"), list)
                or "unsound" not in settings.get("informational_warnings", [])):
            raise SecurityError("scanner settings contain unexpected advisory filters")
        vulnerabilities = document.get("vulnerabilities")
        warnings = document.get("warnings")
        if (not isinstance(vulnerabilities, dict) or not isinstance(vulnerabilities.get("list"), list)
                or type(vulnerabilities.get("count")) is not int
                or vulnerabilities["count"] != len(vulnerabilities["list"])
                or type(vulnerabilities.get("found")) is not bool
                or vulnerabilities["found"] != bool(vulnerabilities["list"])
                or not isinstance(warnings, dict) or any(not isinstance(rows, list) for rows in warnings.values())):
            raise SecurityError("scanner JSON finding inventory is malformed")
        findings = [*vulnerabilities["list"], *warnings.get("unsound", [])]
        for finding in findings:
            if (not isinstance(finding, dict) or not isinstance(finding.get("advisory"), dict)
                    or not isinstance(finding["advisory"].get("id"), str)
                    or not ADVISORY.fullmatch(finding["advisory"]["id"])
                    or not isinstance(finding.get("package"), dict)
                    or not finding["package"].get("name") or not finding["package"].get("version")):
                raise SecurityError("scanner JSON blocking finding is malformed")
        return findings

    def scan(self, name, cwd, ignored, fetch):
        capture = self.output / (name + ".json")
        log = self.output / (name + ".log")
        command = [str(self.scanner), "audit", "--file", str(self.root / "Cargo.lock"),
                   "--db", str(self.database), "--no-yanked", "--deny", "unsound", "--color", "never", "--json"]
        if not fetch:
            command.append("--no-fetch")
        for advisory in ignored:
            command.extend(("--ignore", advisory))
        stage = {"name": name, "command": command, "json": str(capture), "log": str(log), "status": "INCOMPLETE"}
        self.report["scans"].append(stage)
        self.save()
        try:
            with capture.open("w") as stdout, log.open("w") as stderr:
                result = self.execute(command, cwd=cwd, stdout=stdout, stderr=stderr,
                                      text=True, timeout=120, check=False)
            stage["returncode"] = result.returncode
            document = json.loads(capture.read_text())
            findings = self.blocking(document, ignored)
            if result.returncode != (1 if findings else 0):
                raise SecurityError("scanner exit status disagrees with its blocking findings")
            stage["status"] = "FAIL" if findings else "PASS"
            stage["blocking_findings"] = findings
            stage["database"] = document.get("database")
        except (OSError, ValueError, subprocess.TimeoutExpired) as error:
            stage["error"] = str(error)
        finally:
            self.save()
        print(f"{name}: {stage['status']}; raw JSON: {capture}; diagnostics: {log}", flush=True)
        return stage

    def database_revision(self):
        result = subprocess.run(["git", "-C", str(self.database), "rev-parse", "HEAD"],
                                capture_output=True, text=True, timeout=15, check=False)
        revision = result.stdout.strip()
        if result.returncode or not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise SecurityError("cannot establish the advisory database Git revision")
        return revision

    def run(self):
        self.output.mkdir(parents=True, exist_ok=True)
        self.save()
        try:
            if not self.scanner.is_file() or not os.access(self.scanner, os.X_OK):
                raise SecurityError("the pinned scanner executable is missing or not executable")
            policy = SecurityPolicy(self.root)
            self.report["inputs"] = policy.identity()
            ignored = policy.advisories()
            self.report["policy"]["advisories"] = ignored
            self.report["scanner_sha256"] = policy.digest(self.scanner)
            self.save()
            # The empty local config shadows both checkout and HOME audit config.
            with tempfile.TemporaryDirectory(prefix="ai-kit-audit-") as temporary:
                cwd = Path(temporary).resolve()
                if cwd.is_relative_to(self.root):
                    raise SecurityError("scanner working directory must be outside the checkout")
                (cwd / ".cargo").mkdir()
                (cwd / ".cargo/audit.toml").write_text("")
                raw = self.scan("raw", cwd, [], fetch=True)
                raw["database_revision"] = self.database_revision()
                qualified = self.scan("qualified", cwd, ignored, fetch=False)
                qualified["database_revision"] = self.database_revision()
            if policy.identity() != self.report["inputs"]:
                raise SecurityError("dependency inputs changed while auditing")
            if policy.digest(self.scanner) != self.report["scanner_sha256"]:
                raise SecurityError("scanner executable changed while auditing")
            if any(stage["status"] == "INCOMPLETE" for stage in (raw, qualified)):
                raise SecurityError("one or more scanner runs did not complete")
            # cargo-audit omits commit/time metadata when --no-fetch is used.
            # Bind both runs to the on-disk Git revision fetched by the raw run.
            raw_database, qualified_database = raw["database"], qualified["database"]
            if (not isinstance(raw_database, dict) or not isinstance(qualified_database, dict)
                    or type(raw_database.get("advisory-count")) is not int
                    or raw_database["advisory-count"] <= 0
                    or qualified_database.get("advisory-count") != raw_database["advisory-count"]
                    or raw_database.get("last-commit") != raw["database_revision"]
                    or raw["database_revision"] != qualified["database_revision"]
                    or any(qualified_database.get(key) not in (None, raw_database.get(key))
                           for key in ("last-commit", "last-updated"))):
                raise SecurityError("advisory database changed between scans")
            self.report["accepted_findings"] = [finding for finding in raw["blocking_findings"] if finding["advisory"]["id"] in ignored]
            expected = [finding for finding in raw["blocking_findings"] if finding["advisory"]["id"] not in ignored]
            if expected != qualified["blocking_findings"]:
                raise SecurityError("qualified findings differ from the raw scan and explicit exceptions")
            self.report["status"] = ("FAIL" if expected else
                                     "PASS_WITH_EXCEPTIONS" if self.report["accepted_findings"] else "PASS")
        except (OSError, ValueError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
            self.report["error"] = str(error)
            self.report["status"] = "INCOMPLETE"
            print(f"Security audit incomplete: {error}", flush=True)
        finally:
            self.save()
        print(f"{self.report['status']}: dependency security audit; evidence: {self.output}", flush=True)
        return 0 if self.report["status"] in ("PASS", "PASS_WITH_EXCEPTIONS") else 1


if __name__ == "__main__":
    sys.exit(SecurityPolicy.main())

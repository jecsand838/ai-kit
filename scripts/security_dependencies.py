#!/usr/bin/env python3
"""Verify the closed upstream patch exception and audit every dependency identity."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import tempfile
import tomllib


REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
BASE = "third-party/security"
MANIFEST = BASE + "/manifest.json"
APPROVED = {
    "arrow-ipc": ("58.4.1", "165051f1f996f19b50c4d2cdb7ed02ddd400d8d984b422bf6848507eee099c3d"),
    "lance-encoding": ("13.0.0", "3c04a91e545b505b5e6ec98854c7eadc56689cbc072f138dd26805ef0095e81a"),
    "include-flate-compress": ("0.3.4", "44fbb9c5ccb9a5b67b4afa2974c27e5507ea1bf6d22828cef418e4dfaeca51dd"),
    "object_store": ("0.13.2", "622acbc9100d3c10e2ee15804b0caa40e55c933d5aa53814cd520805b7958a49"),
    "surrealdb-core": ("3.3.0", "1490b831b5090b9bdbdacec5e8060d9dbc1944574809d157fabb4de25e53b1aa"),
    "geo": ("0.32.0", "3f3901269ec6d4f6068d3f09e5f02f995bd076398dcd1dfec407cd230b02d11b"),
    "surrealdb-expr": ("3.3.0", "2e494e4696ac372ead8c0a8b38f830db056a34c8f4192be4cfadeea3089c8687"),
}
WORKSPACE_PACKAGES = ("provider-sdk", "storage-sdk", "tool-sdk", "agent-sdk", "app-sdk")


class SecurityError(ValueError):
    """The declared dependency identities or captured evidence are not trustworthy."""


class SecurityPatches:
    def __init__(self, root):
        self.root = Path(root).resolve()

    @staticmethod
    def digest(path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    @staticmethod
    def unique_pairs(pairs):
        document = {}
        for key, value in pairs:
            if key in document:
                raise SecurityError(f"duplicate provenance field: {key}")
            document[key] = value
        return document

    @staticmethod
    def relative(value):
        if (not isinstance(value, str) or not value or "\\" in value or "\0" in value
                or PurePosixPath(value).is_absolute()
                or any(part in ("", ".", "..") for part in value.split("/"))):
            raise SecurityError(f"noncanonical dependency input path: {value!r}")
        return value

    def path(self, relative):
        relative = self.relative(relative)
        current = self.root
        for part in PurePosixPath(relative).parts:
            current = current / part
            if current.is_symlink():
                raise SecurityError(f"dependency input symlink is forbidden: {relative}")
        return current

    @staticmethod
    def hash_value(value):
        return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

    def document(self):
        try:
            document = json.loads(self.path(MANIFEST).read_text(), object_pairs_hook=self.unique_pairs)
        except (OSError, ValueError) as error:
            raise SecurityError(f"{MANIFEST}: {error}") from error
        if (not isinstance(document, dict) or set(document) != {"schema", "packages"}
                or type(document["schema"]) is not int or document["schema"] != 1
                or not isinstance(document["packages"], list)):
            raise SecurityError(f"{MANIFEST}: unsupported provenance schema")
        return document

    def workspace(self):
        try:
            path = self.path("Cargo.toml")
            return tomllib.loads(path.read_text()) if path.exists() else {}
        except (OSError, ValueError) as error:
            raise SecurityError(f"Cargo.toml: {error}") from error

    def files(self, relative):
        directory = self.path(relative)
        if not directory.is_dir():
            raise SecurityError(f"dependency source directory is missing: {relative}")
        files = {}
        for current, directories, names in os.walk(directory, followlinks=False):
            for name in (*directories, *names):
                path = Path(current) / name
                mode = path.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise SecurityError(f"dependency input symlink is forbidden: {path.relative_to(self.root)}")
                if path.is_dir():
                    continue
                if not stat.S_ISREG(mode):
                    raise SecurityError(f"dependency input is not a regular file: {path.relative_to(self.root)}")
                files[path.relative_to(directory).as_posix()] = {
                    "sha256": self.digest(path),
                    "executable": bool(mode & 0o111),
                }
        return files

    def verify(self):
        workspace = self.workspace()
        manifest = self.path(MANIFEST)
        if not manifest.exists():
            if "patch" in workspace:
                raise SecurityError("Cargo.toml: dependency patches require the approved provenance manifest")
            self._verified_inputs = ()
            return ()
        document = self.document()
        entries = document["packages"]
        if len(entries) != len(APPROVED):
            raise SecurityError(f"{MANIFEST}: expected exactly {len(APPROVED)} approved upstream packages")
        by_name = {}
        keys = {"name", "version", "path", "source", "archive_url", "archive_sha256",
                "license_files", "patch", "files"}
        for package in entries:
            if not isinstance(package, dict) or set(package) != keys:
                raise SecurityError(f"{MANIFEST}: invalid package fields")
            name = package["name"]
            if not isinstance(name, str) or name not in APPROVED or name in by_name:
                raise SecurityError(f"{MANIFEST}: unknown or duplicate upstream package")
            by_name[name] = package
        expected_patches = {}
        roots = []
        expected_diffs = set()
        verified_inputs = {MANIFEST}
        for name, (version, checksum) in APPROVED.items():
            package = by_name[name]
            relative = f"{BASE}/{name}-{version}"
            archive_url = f"https://static.crates.io/crates/{name}/{name}-{version}.crate"
            if any(package[key] != expected for key, expected in (
                ("version", version), ("path", relative), ("source", REGISTRY),
                ("archive_url", archive_url), ("archive_sha256", checksum),
            )):
                raise SecurityError(f"{MANIFEST}: approved identity/provenance differs for {name}")
            roots.append(relative)
            declared = package["files"]
            if not isinstance(declared, dict) or not declared:
                raise SecurityError(f"{relative}: source inventory is empty or malformed")
            for filename, identity in declared.items():
                self.relative(filename)
                if (not isinstance(identity, dict) or set(identity) != {"sha256", "executable"}
                        or not self.hash_value(identity["sha256"])
                        or type(identity["executable"]) is not bool):
                    raise SecurityError(f"{relative}/{filename}: malformed file identity")
            actual = self.files(relative)
            if actual != declared:
                missing = sorted(set(declared) - set(actual))
                extra = sorted(set(actual) - set(declared))
                changed = sorted(name for name in set(actual) & set(declared) if actual[name] != declared[name])
                raise SecurityError(f"{relative}: source inventory differs; missing={missing}, extra={extra}, changed={changed}")
            verified_inputs.update(relative + "/" + filename for filename in declared)
            if not {"Cargo.toml", "Cargo.toml.orig"}.issubset(actual):
                raise SecurityError(f"{relative}: normalized and original upstream manifests are required")
            try:
                native = tomllib.loads(self.path(relative + "/Cargo.toml").read_text()).get("package", {})
            except (OSError, ValueError) as error:
                raise SecurityError(f"{relative}/Cargo.toml: {error}") from error
            if native.get("name") != name or native.get("version") != version:
                raise SecurityError(f"{relative}/Cargo.toml: upstream package identity differs")
            licenses = package["license_files"]
            if (not isinstance(licenses, list) or not licenses
                    or any(not isinstance(item, str) for item in licenses)
                    or len(set(licenses)) != len(licenses)
                    or any(item not in actual for item in licenses)):
                raise SecurityError(f"{relative}: license files are missing or invalid")
            for license_file in licenses:
                if not self.path(relative + "/" + license_file).read_bytes().strip():
                    raise SecurityError(f"{relative}: license file is empty")
            diff = package["patch"]
            diff_path = f"{BASE}/patches/{name}-{version}.patch"
            if (not isinstance(diff, dict) or set(diff) != {"path", "sha256"}
                    or diff["path"] != diff_path or not self.hash_value(diff["sha256"])):
                raise SecurityError(f"{MANIFEST}: invalid patch provenance for {name}")
            path = self.path(diff_path)
            if not path.is_file() or self.digest(path) != diff["sha256"] or not path.read_bytes().strip():
                raise SecurityError(f"{diff_path}: patch diff is missing, empty, or changed")
            expected_diffs.add(Path(diff_path).name)
            verified_inputs.add(diff_path)
            if name in ("object_store", "geo"):
                alias = "object_store_013" if name == "object_store" else "geo_032"
                expected_patches[alias] = {"package": name, "path": relative}
            else:
                expected_patches[name] = {"path": relative}
        if workspace.get("patch") != {"crates-io": expected_patches}:
            raise SecurityError(f"Cargo.toml: patch table must contain exactly {len(APPROVED)} approved path overrides")
        exclusions = workspace.get("workspace", {}).get("exclude")
        if (not isinstance(exclusions, list) or any(not isinstance(item, str) for item in exclusions)
                or len(exclusions) != len(roots) or set(exclusions) != set(roots)):
            raise SecurityError(f"Cargo.toml: explicitly exclude exactly {len(APPROVED)} approved patch roots from the workspace")
        differences = self.files(BASE + "/patches")
        if set(differences) != expected_diffs:
            raise SecurityError(f"{BASE}/patches: unexpected or missing patch diff")
        readme = self.path(BASE + "/README.md")
        if readme.exists() and not readme.is_file():
            raise SecurityError(f"{BASE}/README.md: provenance notes must be a regular file")
        if readme.is_file():
            verified_inputs.add(BASE + "/README.md")
        actual_inputs = {BASE + "/" + path for path in self.files(BASE)}
        if actual_inputs != verified_inputs:
            raise SecurityError(f"{BASE}: unapproved dependency inputs: {sorted(actual_inputs - verified_inputs)}")
        self._verified_inputs = tuple(sorted(verified_inputs))
        return tuple(roots)

    def inputs(self):
        self.verify()
        return self._verified_inputs

    def fingerprint(self):
        digest = hashlib.sha256()
        for relative in self.inputs():
            path = self.path(relative)
            digest.update(relative.encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
            digest.update(b"1" if path.stat().st_mode & 0o111 else b"0")
        return digest.hexdigest()

    def audit_parents(self, lockfile):
        roots = self.verify()
        expected = dict(APPROVED) if roots else {}
        try:
            document = tomllib.loads(Path(lockfile).read_text())
        except (OSError, ValueError) as error:
            raise SecurityError(f"Cargo.lock: {error}") from error
        packages = document.get("package")
        if not isinstance(packages, list):
            raise SecurityError("Cargo.lock: package inventory is missing")
        found = set()
        for package in packages:
            if (not isinstance(package, dict) or not isinstance(package.get("name"), str)
                    or not package["name"] or not isinstance(package.get("version"), str)
                    or not package["version"]):
                raise SecurityError("Cargo.lock: malformed package identity")
            name, version, source = package.get("name"), package.get("version"), package.get("source")
            if source is None:
                if name in WORKSPACE_PACKAGES:
                    continue
                if (name not in expected or version != expected[name][0] or name in found):
                    raise SecurityError(f"Cargo.lock: unverified or duplicate path dependency {name} {version}")
                found.add(name)
            elif source != REGISTRY:
                raise SecurityError(f"Cargo.lock: non-default registry or Git dependency would escape cargo-audit: {name}")
        if found != set(expected):
            raise SecurityError(f"Cargo.lock: approved patched parent inventory differs: {sorted(found)}")
        return [
            {"name": name, "version": version, "source": REGISTRY, "checksum": checksum}
            for name, (version, checksum) in expected.items()
        ]

    @classmethod
    def main(cls, arguments=None):
        parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
        commands = parser.add_subparsers(dest="command", required=True)
        verify = commands.add_parser("verify", allow_abbrev=False)
        verify.add_argument("--root", required=True)
        verify.add_argument("--print-roots", action="store_true")
        audit = commands.add_parser("audit", allow_abbrev=False)
        for name in ("root", "scanner", "database", "output"):
            audit.add_argument("--" + name, required=True)
        args = parser.parse_args(arguments)
        try:
            if args.command == "audit":
                return SecurityAudit(args.root, args.scanner, args.database, args.output).run()
            roots = cls(args.root).verify()
            if args.print_roots:
                for root in roots:
                    print(root)
            else:
                print(f"PASS: verified {len(roots)} approved upstream patches")
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
            "schema": 1, "status": "INCOMPLETE", "scans": [],
            "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "policy": {"deny": ["unsound"], "check_yanked": False},
        }

    def save(self):
        destination = self.output / "report.json"
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(self.report, indent=2) + "\n")
        temporary.replace(destination)

    def scan(self, name, lockfile, cwd, fetch):
        log = self.output / (name + ".log")
        command = [str(self.scanner), "audit", "--file", str(lockfile),
                   "--db", str(self.database), "--no-yanked", "--deny", "unsound", "--color", "never"]
        if not fetch:
            command.append("--no-fetch")
        stage = {"name": name, "command": command, "log": str(log), "status": "INCOMPLETE"}
        self.report["scans"].append(stage)
        self.save()
        try:
            with log.open("w") as capture:
                result = self.execute(command, cwd=cwd, stdout=capture, stderr=subprocess.STDOUT,
                                      text=True, timeout=120, check=False)
            stage["returncode"] = result.returncode
            stage["status"] = "PASS" if result.returncode == 0 else "FAIL"
        except (OSError, subprocess.TimeoutExpired) as error:
            stage["error"] = str(error)
        finally:
            self.save()
        if log.is_file():
            print(f"==> {name}", flush=True)
            print(log.read_text(errors="replace"), end="", flush=True)

    def run(self):
        self.output.mkdir(parents=True, exist_ok=True)
        self.save()
        try:
            if not self.scanner.is_file() or not os.access(self.scanner, os.X_OK):
                raise SecurityError("the pinned scanner executable is missing or not executable")
            patches = SecurityPatches(self.root)
            lockfile = self.root / "Cargo.lock"
            parents = patches.audit_parents(lockfile)
            self.report["lockfile_sha256"] = patches.digest(lockfile)
            self.report["patch_fingerprint"] = patches.fingerprint()
            self.report["parents"] = parents
            supplemental = self.output / "patched-parents.lock"
            contents = "version = 4\n"
            for package in parents:
                contents += "\n[[package]]\n"
                contents += "".join(f"{key} = {json.dumps(value)}\n" for key, value in package.items())
            supplemental.write_text(contents)
            self.save()
            # cargo-audit reads configuration from cwd/.cargo. A fresh directory
            # prevents checkout-local config from hiding advisories.
            with tempfile.TemporaryDirectory(prefix="scanner-", dir=self.output) as cwd:
                self.scan("committed-lockfile", lockfile, Path(cwd), fetch=True)
                self.scan("patched-parents", supplemental, Path(cwd), fetch=False)
            if (patches.digest(lockfile) != self.report["lockfile_sha256"]
                    or patches.fingerprint() != self.report["patch_fingerprint"]):
                raise SecurityError("dependency inputs changed while auditing")
            states = [stage["status"] for stage in self.report["scans"]]
            self.report["status"] = "PASS" if states == ["PASS", "PASS"] else "FAIL" if "FAIL" in states else "INCOMPLETE"
        except (OSError, ValueError, KeyboardInterrupt) as error:
            self.report["error"] = str(error)
            self.report["status"] = "INCOMPLETE"
            print(f"Security audit incomplete: {error}", flush=True)
        finally:
            self.save()
        print(f"{self.report['status']}: dependency security audit; evidence: {self.output}", flush=True)
        return 0 if self.report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(SecurityPatches.main())

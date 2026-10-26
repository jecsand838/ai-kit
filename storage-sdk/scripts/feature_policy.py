#!/usr/bin/env python3
"""Storage's fixed backend selection policy. No services or builds are started."""
import argparse
import itertools
import json
import re
from pathlib import Path
import subprocess
import sys
import tomllib


class StorageSelection:
    BACKENDS = ("lance", "redb", "postgres", "surrealdb", "sqlite", "qdrant")
    ROOTS = {
        "lance": ("lancedb", "lance", "lance-table", "lance-io", "object_store", "arrow-array", "arrow-schema", "rustix"),
        "redb": ("redb",), "postgres": ("tokio-postgres",),
        "surrealdb": ("surrealdb", "surrealdb-core"),
        "sqlite": ("turso",), "qdrant": ("qdrant-edge", "reqwest"),
    }

    def __init__(self, root):
        self.root = root
        self.reasons = {"lance": ["always-on embedded products"]}
        self.paths = []
        self.base = None
        self.head = None

    def add(self, backends, reason):
        for backend in backends:
            reasons = self.reasons.setdefault(backend, [])
            if reason not in reasons:
                reasons.append(reason)

    def git(self, *args):
        result = subprocess.run(["git", *args], cwd=self.root, capture_output=True, check=True)
        return result.stdout

    def document(self, path, revision=None):
        content = self.git("show", f"{revision}:{path}").decode() if revision else (self.root / path).read_text()
        return tomllib.loads(content)

    @staticmethod
    def packages(lock):
        return {(p["name"], p["version"], p.get("source", "")): {**p, "dependencies": sorted(p.get("dependencies", []))} for p in lock["package"]}

    @staticmethod
    def dependency_key(value, packages):
        parts = value.split()
        candidates = [key for key in packages if key[0] == parts[0] and (len(parts) < 2 or key[1] == parts[1])]
        if len(parts) > 2:
            candidates = [key for key in candidates if key[2] == parts[2].strip("()")]
        if len(candidates) != 1:
            raise ValueError(f"unresolved dependency identity: {value}")
        return candidates[0]

    @classmethod
    def closure(cls, packages, roots):
        storage = [key for key in packages if key[0] == "storage-sdk" and not key[2]]
        if len(storage) != 1:
            raise ValueError("unresolved workspace storage-sdk package identity")
        direct = [cls.dependency_key(value, packages) for value in packages[storage[0]].get("dependencies", [])]
        pending = [key for key in direct if key[0] in roots]
        visited = set()
        while pending:
            key = pending.pop()
            if key in visited:
                continue
            visited.add(key)
            pending.extend(cls.dependency_key(dep, packages) for dep in packages[key].get("dependencies", []))
        return visited

    def dependencies(self, revision):
        old = self.packages(self.document("Cargo.lock", revision))
        new = self.packages(self.document("Cargo.lock"))
        changed = {key for key in old.keys() | new.keys() if old.get(key) != new.get(key)}
        for backend, roots in self.ROOTS.items():
            before, after = self.closure(old, roots), self.closure(new, roots)
            if before != after or changed & (before | after):
                self.add((backend,), "changed reachable dependency identity or edges")
        # Dependencies shared by every implementation (serde, Tokio, etc.) must
        # not disappear merely because they are not below a native backend root.
        shared = set()
        native = {name for names in self.ROOTS.values() for name in names}
        for source in (self.document("storage-sdk/Cargo.toml", revision), self.document("storage-sdk/Cargo.toml")):
            for (section, name), value in self.manifest_edges(source).items():
                package = value.get("package", name)
                if not value.get("optional", False) and (package not in native or section.endswith(("/dev", "/build"))):
                    shared.add(package)
        before, after = self.closure(old, shared), self.closure(new, shared)
        if before != after or changed & (before | after):
            self.add(self.BACKENDS, "changed shared Storage dependency closure")

    @staticmethod
    def manifest_edges(manifest):
        sections = []
        for target, values in [("", manifest), *manifest.get("target", {}).items()]:
            for kind, suffix in (("dependencies", ""), ("dev-dependencies", "/dev"), ("build-dependencies", "/build")):
                sections.append((target + suffix, values.get(kind, {})))
        edges = {}
        for target, dependencies in sections:
            for name, value in dependencies.items():
                value = {"version": value} if isinstance(value, str) else dict(value)
                if "features" in value:
                    value["features"] = sorted(value["features"])
                edges[(target, name)] = value
        return edges

    def dependency_owners(self, names, reason):
        for name in names:
            owners = [backend for backend, roots in self.ROOTS.items() if name in roots]
            self.add(owners or self.BACKENDS, reason)

    def manifest_dependencies(self, revision):
        old = self.document("storage-sdk/Cargo.toml", revision)
        new = self.document("storage-sdk/Cargo.toml")
        before, after = self.manifest_edges(old), self.manifest_edges(new)
        for key in before.keys() | after.keys():
            if before.get(key) != after.get(key):
                names = {values.get(key, {}).get("package", key[1]) for values in (before, after)}
                if key[0].endswith(("/dev", "/build")):
                    self.add(self.BACKENDS, "changed shared Storage test/build dependency edge")
                else:
                    self.dependency_owners(names, "changed Storage dependency requirement or feature edge")
        before = {name: sorted(values) for name, values in old.get("features", {}).items()}
        after = {name: sorted(values) for name, values in new.get("features", {}).items()}
        for name in before.keys() | after.keys():
            if before.get(name) != after.get(name):
                self.add((name,) if name in self.BACKENDS else self.BACKENDS, "changed Storage Cargo feature")
        for field in ("edition", "rust-version", "build", "links"):
            if old.get("package", {}).get(field) != new.get("package", {}).get(field):
                self.add(self.BACKENDS, "changed Storage compilation requirement")

    def workspace_dependencies(self, revision):
        old_document = self.document("Cargo.toml", revision)
        new_document = self.document("Cargo.toml")
        for field in ("patch", "replace", "resolver", "profile"):
            if old_document.get(field) != new_document.get(field):
                self.add(self.BACKENDS, "workspace resolution/build configuration changed")
        old = old_document.get("workspace", {})
        new = new_document.get("workspace", {})
        if old.get("resolver") != new.get("resolver"):
            self.add(self.BACKENDS, "workspace resolver changed")
        before = self.manifest_edges(old)
        after = self.manifest_edges(new)
        changed = {name for _, name in before.keys() | after.keys() if before.get(("", name)) != after.get(("", name))}
        used = {}
        shared = set()
        for manifest in (self.document("storage-sdk/Cargo.toml", revision), self.document("storage-sdk/Cargo.toml")):
            for (section, name), value in self.manifest_edges(manifest).items():
                if value.get("workspace", False):
                    used[name] = value.get("package", name)
                    if section.endswith(("/dev", "/build")):
                        shared.add(name)
        self.dependency_owners((used[name] for name in changed & used.keys() - shared), "Storage-inherited workspace dependency changed")
        if changed & shared:
            self.add(self.BACKENDS, "Storage-inherited shared test/build dependency changed")
        for field in ("edition", "rust-version"):
            if old.get("package", {}).get(field) != new.get("package", {}).get(field):
                self.add(self.BACKENDS, "workspace language/toolchain requirement changed")

    @staticmethod
    def rust_without_docs(source):
        # Recognize only unambiguous line documentation. Raw/multiline strings
        # and block comments remain conservative, never hiding executable text.
        result = []
        for line in source.splitlines(keepends=True):
            if line.lstrip().startswith(("///", "//!")):
                continue
            index = 0
            while index < len(line):
                if line[index:index + 2] == "//":
                    break
                if line[index:index + 2] == "/*" or re.match(r'(?:br|cr|r)\#*"', line[index:]):
                    return None
                if line[index] == "'":
                    literal = re.match(r"'(?:[^'\\\n]|\\(?:[nrt0\\'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]+\}))'", line[index:])
                    if literal:
                        index += len(literal[0])
                        continue
                if line[index] == '"':
                    index += 1
                    while index < len(line) and line[index] != '"':
                        index += 2 if line[index] == "\\" else 1
                    if index >= len(line):
                        return None
                index += 1
            result.append(line)
        return "".join(result)

    def documentation_only(self, path, revision):
        if not path.startswith("storage-sdk/") or not path.endswith(".rs") or not (self.root / path).is_file():
            return False
        try:
            before = self.git("show", f"{revision}:{path}").decode()
        except subprocess.CalledProcessError:
            return False
        before = self.rust_without_docs(before)
        after = self.rust_without_docs((self.root / path).read_text())
        return before is not None and before == after

    def classify(self, path):
        if path.endswith((".md", ".txt")) or path.startswith("docs/"):
            return
        if path in ("Cargo.lock", "Cargo.toml", "storage-sdk/Cargo.toml"):
            return  # Compared by resolved package identities, not filename alone.
        if path.startswith(".cargo/"):
            self.add(self.BACKENDS, f"shared dependency/feature configuration: {path}")
            return
        if not path.startswith("storage-sdk/"):
            return
        owners = {"lance": ("lance", "lancedb"), "redb": ("redb",), "postgres": ("postgres",),
                  "surrealdb": ("surreal", "surrealdb"), "sqlite": ("sqlite", "turso"), "qdrant": ("qdrant",)}
        parts = path.replace("-", "_").replace(".", "_").replace("/", "_").split("_")
        matches = [backend for backend, markers in owners.items() if any(marker in parts for marker in markers)]
        if matches:
            self.add(matches, f"backend-owned change: {path}")
            return
        if any(marker in path for marker in ("state_stores/", "state_handle", "state_store", "state_cases/", "src/state/")):
            self.add(("lance", "redb", "postgres", "sqlite"), f"shared StateStore change: {path}")
        elif any(marker in path for marker in ("databases/", "database_handle", "database_", "src/database/")):
            self.add(("lance", "surrealdb", "qdrant"), f"shared Database change: {path}")
        else:
            self.add(self.BACKENDS, f"shared Storage change: {path}")

    def changed_since(self, revision):
        self.base = self.git("rev-parse", "--verify", f"{revision}^{{commit}}").decode().strip()
        self.head = self.git("rev-parse", "HEAD").decode().strip()
        base = self.git("merge-base", self.base, self.head).decode().strip()
        # --no-renames includes both old/deleted and new paths without parsing
        # filenames as lines; spaces and embedded newlines remain ordinary data.
        data = self.git("diff", "--name-only", "-z", "--no-renames", base, self.head, "--")
        data += self.git("diff", "--name-only", "-z", "--no-renames", "HEAD", "--")
        data += self.git("ls-files", "--others", "--exclude-standard", "-z")
        self.paths = sorted({path.decode() for path in data.split(b"\0") if path})
        for path in self.paths:
            if not self.documentation_only(path, base):
                self.classify(path)
        if "Cargo.lock" in self.paths:
            self.dependencies(base)
        if "Cargo.toml" in self.paths:
            self.workspace_dependencies(base)
        if "storage-sdk/Cargo.toml" in self.paths:
            self.manifest_dependencies(base)

    def result(self, all_backends=False):
        backends = [backend for backend in self.BACKENDS if backend in self.reasons]
        lanes = [[]] + [[backend] for backend in backends if backend != "lance"]
        if all_backends:
            lanes.extend([["redb", "postgres", "surrealdb", "sqlite"], ["redb", "postgres", "surrealdb", "qdrant"]])
        return {"policy": 1, "base": self.base, "head": self.head, "paths": self.paths,
                "backends": backends, "reasons": self.reasons, "lanes": lanes}

    @classmethod
    def matrix(cls, legal):
        for count in range(6):
            for subset in itertools.combinations(cls.BACKENDS[1:], count):
                valid = not {"sqlite", "qdrant"}.issubset(subset)
                if valid == legal:
                    yield list(subset)

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--backend", choices=cls.BACKENDS, action="append", default=[])
        parser.add_argument("--all-backends", action="store_true")
        parser.add_argument("--changed-since")
        parser.add_argument("--format", choices=("json", "lanes", "backends", "legal", "illegal"), default="json")
        args = parser.parse_args()
        policy = cls(Path(__file__).resolve().parents[2])
        policy.add(args.backend, "explicit backend selection")
        if args.all_backends:
            policy.add(cls.BACKENDS, "explicit complete backend matrix")
        if args.changed_since is not None:
            policy.changed_since(args.changed_since)
        result = policy.result(args.all_backends)
        if args.format == "json":
            print(json.dumps(result, indent=2))
        elif args.format == "backends":
            print("\n".join(result["backends"]))
        else:
            lanes = result["lanes"] if args.format == "lanes" else cls.matrix(args.format == "legal")
            print("\n".join(",".join(lane) or "-" for lane in lanes))


if __name__ == "__main__":
    try:
        StorageSelection.main()
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError, tomllib.TOMLDecodeError) as error:
        print(f"Storage selection failed: {error}", file=sys.stderr)
        sys.exit(2)

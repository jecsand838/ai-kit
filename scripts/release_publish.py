#!/usr/bin/env python3
"""Admit manual main releases and publish immutable, checksum-verified packages."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

if __package__:
    from .mock_contract_runner import Runner
else:
    from mock_contract_runner import Runner


class ReleaseError(ValueError):
    """A release cannot safely proceed with the supplied evidence or identity."""


class Release:
    PACKAGES = ("providers-kit", "storage-kit", "tools-kit", "agents-kit", "app-kit")
    REPOSITORY = "jecsand838/ai-kit"

    def __init__(self, root, environment=None):
        self.root = Path(root).resolve()
        self.env = dict(os.environ if environment is None else environment)
        self.version = self.env.get("RELEASE_VERSION", "")
        self.revision = self.env.get("GITHUB_SHA", "")
        self.override = self.env.get("INITIAL_RELEASE_OVERRIDE", "false") == "true"
        self.output = self.root / "target/release"
        self.report = {"schema": 1, "status": "INCOMPLETE", "packages": []}

    def command(self, arguments, check=True):
        environment = dict(self.env, CARGO_TARGET_DIR=str(self.root / "target/release-build"))
        result = Runner.owned_process(arguments, cwd=self.root, env=environment, text=True, timeout=1800)
        if check:
            result.check_returncode()
        return result

    def git(self, *arguments):
        return subprocess.run(["git", *arguments], cwd=self.root, capture_output=True,
                              text=True, check=True, timeout=30).stdout.strip()

    def admit(self):
        if (self.env.get("GITHUB_ACTIONS") != "true"
                or self.env.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
                or self.env.get("GITHUB_REF") != "refs/heads/main"
                or self.env.get("GITHUB_REPOSITORY") != self.REPOSITORY):
            raise ReleaseError("releases require manual dispatch on jecsand838/ai-kit main")
        try:
            maintainers = json.loads(self.env.get("RELEASE_MAINTAINERS") or "[]")
        except json.JSONDecodeError as error:
            raise ReleaseError("RELEASE_MAINTAINERS must be a JSON username array") from error
        if (not isinstance(maintainers, list)
                or any(not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}", name)
                       for name in maintainers)):
            raise ReleaseError("RELEASE_MAINTAINERS must be a JSON username array")
        allowed = {self.REPOSITORY.split("/")[0].lower(), *(name.lower() for name in maintainers)}
        for key in ("GITHUB_ACTOR", "GITHUB_TRIGGERING_ACTOR"):
            if self.env.get(key, "").lower() not in allowed:
                raise ReleaseError(f"{key} is not the owner or an explicitly designated release maintainer")
        if not re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", self.version):
            raise ReleaseError("RELEASE_VERSION must be a numeric Cargo version without a v prefix")
        if not re.fullmatch(r"[0-9a-f]{40}", self.revision) or self.git("rev-parse", "HEAD") != self.revision:
            raise ReleaseError("checkout must exactly match the selected main revision")
        if self.git("status", "--porcelain=v1", "--untracked-files=normal"):
            raise ReleaseError("release checkout must be clean")
        if self.env.get("INITIAL_RELEASE_OVERRIDE", "false") not in ("true", "false"):
            raise ReleaseError("INITIAL_RELEASE_OVERRIDE must be true or false")
        if self.override:
            if self.version != "0.0.1":
                raise ReleaseError("the qualification override is authorized only for 0.0.1")
        else:
            if self.env.get("QUALIFICATION_REVISION") != self.revision:
                raise ReleaseError("qualification declaration must name this exact main revision")
            evidence = urllib.parse.urlparse(self.env.get("QUALIFICATION_URL", ""))
            if evidence.scheme != "https" or not evidence.netloc or evidence.username or evidence.password:
                raise ReleaseError("normal releases require an HTTPS qualification evidence URL")
        workspace = tomllib.loads((self.root / "Cargo.toml").read_text()).get("workspace", {})
        if set(workspace.get("members", [])) != set(self.PACKAGES):
            raise ReleaseError("workspace package inventory differs from the release inventory")
        for name in self.PACKAGES:
            package = tomllib.loads((self.root / name / "Cargo.toml").read_text())["package"]
            if package.get("name") != name or package.get("version") != self.version:
                raise ReleaseError(f"{name}: commit the requested package version before release")
        self.report.update(version=self.version, revision=self.revision,
                           qualification="INITIAL_0_0_1_OVERRIDE" if self.override else "MAINTAINER_DECLARED",
                           qualification_url=self.env.get("QUALIFICATION_URL", ""),
                           actor=self.env["GITHUB_ACTOR"], triggering_actor=self.env["GITHUB_TRIGGERING_ACTOR"],
                           deferred=("strict-live; slow integration, capacity and full qualification; "
                                     "Rust mock execution, rustdoc/doctests and package build verification"
                                     if self.override else "strict-live evidence is supplied by the maintainer; not rerun here"))
        return self.report

    def json_request(self, url, *, method="GET", payload=None, github=False):
        headers = {"Accept": "application/json", "User-Agent": "ai-kit-release (https://github.com/jecsand838/ai-kit)"}
        if github:
            token = self.env.get("GH_TOKEN")
            if not token:
                raise ReleaseError("GH_TOKEN is required for GitHub release identity checks")
            headers.update(Authorization="Bearer " + token, **{"X-GitHub-Api-Version": "2022-11-28"})
        body = None if payload is None else json.dumps(payload).encode()
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404 and method == "GET":
                return None
            raise ReleaseError(f"{method} {url}: HTTP {error.code}") from error
        except (OSError, ValueError) as error:
            raise ReleaseError(f"{method} {url}: request failed") from error

    def github(self, suffix, **kwargs):
        return self.json_request(f"https://api.github.com/repos/{self.REPOSITORY}/{suffix}", github=True, **kwargs)

    def tag_identity(self):
        reference = self.github("git/ref/tags/v" + self.version)
        if reference is None:
            return None
        obj = reference.get("object", {})
        for _ in range(5):
            if obj.get("type") == "commit":
                if obj.get("sha") != self.revision:
                    raise ReleaseError("existing release tag points to another revision")
                return self.revision
            if obj.get("type") != "tag" or not re.fullmatch(r"[0-9a-f]{40}", obj.get("sha", "")):
                break
            annotated = self.github("git/tags/" + obj["sha"])
            obj = annotated.get("object", {}) if isinstance(annotated, dict) else {}
        raise ReleaseError("cannot resolve the existing release tag to the selected revision")

    def registry(self, name):
        document = self.json_request(f"https://crates.io/api/v1/crates/{name}/{self.version}")
        if document is None:
            return None
        version = document.get("version", {})
        if (version.get("crate") != name or version.get("num") != self.version
                or not re.fullmatch(r"[0-9a-f]{64}", version.get("checksum", ""))
                or version.get("yanked") is not False):
            raise ReleaseError(f"{name}: invalid or yanked registry version")
        return version

    @staticmethod
    def same_archive(name, version, checksum):
        if version["checksum"] != checksum:
            raise ReleaseError(f"{name}: published version has different bytes; never overwrite or silently skip it")

    def save(self):
        self.output.mkdir(parents=True, exist_ok=True)
        (self.output / "report.json").write_text(json.dumps(self.report, indent=2) + "\n")

    def publish_package(self, name):
        row = {"name": name, "version": self.version, "status": "INCOMPLETE"}
        self.report["packages"].append(row)
        self.save()
        package = ["cargo", "package", "--locked", "-p", name]
        if self.override:
            package.append("--no-verify")
        self.command(package)
        archive = self.root / "target/release-build/package" / f"{name}-{self.version}.crate"
        checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
        row.update(sha256=checksum, archive=str(archive.relative_to(self.root)))
        existing = self.registry(name)
        if existing is not None:
            self.same_archive(name, existing, checksum)
            row["status"] = "ALREADY_PUBLISHED_IDENTICAL"
            self.save()
            return
        # Package verification was performed above, or explicitly waived for 0.0.1.
        result = self.command(["cargo", "publish", "--locked", "--no-verify", "-p", name], check=False)
        row["publish_exit"] = result.returncode
        # A nonzero Cargo result may occur after upload; retain it and inspect the
        # registry instead of retrying a potentially successful upload.
        for attempt in range(12):
            version = self.registry(name)
            if version is not None:
                self.same_archive(name, version, checksum)
                row["status"] = "PUBLISHED" if result.returncode == 0 else "PUBLISHED_AFTER_CARGO_ERROR"
                self.save()
                return
            if attempt < 11:
                time.sleep(5)
        raise ReleaseError(f"{name}: publication was not confirmed; inspect the retained Cargo result before rerunning")

    def notes(self):
        qualification = ("The maintainer explicitly authorized the initial 0.0.1 qualification override. "
                         "Strict-live, slow integration/capacity, Rust mock execution, rustdoc/doctests and "
                         "package build verification were deferred. This is not full release qualification. "
                         "Issues found after release are intended for a fast-follow 0.0.2."
                         if self.override else
                         "The release maintainer declared qualification for this exact revision: " + self.env["QUALIFICATION_URL"] +
                         ". The workflow validates the declaration's revision and URL, not the underlying receipts.")
        packages = "\n".join(f"- [{name} {self.version}](https://crates.io/crates/{name}/{self.version}) · "
                             f"[rustdoc](https://docs.rs/{name}/{self.version})" for name in self.PACKAGES)
        return (f"Commit: `{self.revision}`\n\n{packages}\n\n"
                "Backwards compatibility is not guaranteed before 1.0.0.\n\n" + qualification + "\n\n"
                "The dependency audit retains raw findings and the explicit temporary exceptions in "
                f"[the security policy](https://github.com/{self.REPOSITORY}/blob/{self.revision}/docs/security-dependencies.md). "
                "Accepted vulnerabilities remain unresolved and will be fixed ASAP.\n\n"
                "docs.rs builds versioned documentation automatically from these crates.io uploads; "
                "successful publication does not certify that those asynchronous builds have completed.\n")

    def publish(self):
        try:
            self.admit()
            self.save()
            if not self.env.get("CARGO_REGISTRY_TOKEN"):
                raise ReleaseError("CARGO_REGISTRY_TOKEN must be configured as an Actions secret")
            tag = self.tag_identity()
            existing_release = self.github("releases/tags/v" + self.version)
            if existing_release is not None and (tag is None or existing_release.get("draft")):
                raise ReleaseError("an existing draft or unbound release needs maintainer review")
            for name in self.PACKAGES:
                self.publish_package(name)
            if self.git("status", "--porcelain=v1", "--untracked-files=normal"):
                raise ReleaseError("source changed while publishing; GitHub release was not created")
            self.tag_identity()
            release = existing_release or self.github("releases", method="POST", payload={
                "tag_name": "v" + self.version, "target_commitish": self.revision,
                "name": "ai-kit v" + self.version, "body": self.notes(), "draft": False,
                "prerelease": False, "make_latest": "true"})
            self.report.update(status="PUBLISHED", release_url=release["html_url"])
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            self.report.update(status="FAIL", error=str(error))
            raise
        finally:
            self.save()
        return self.report

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("command", choices=("preflight", "publish"))
        parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
        args = parser.parse_args()
        release = cls(args.root)
        try:
            result = release.publish() if args.command == "publish" else release.admit()
            print(json.dumps(result, indent=2))
            return 0
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print(f"Release failed: {error}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(Release.main())

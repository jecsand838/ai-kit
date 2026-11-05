#!/usr/bin/env python3
"""Admit manual main releases and publish immutable, checksum-verified packages."""

import argparse
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import signal
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


class ReleaseRedirects(urllib.request.HTTPRedirectHandler):
    """Exact hosted documentation and image URLs must answer directly."""

    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


class RustdocPage(HTMLParser):
    """Extract the rendered crate identity and sidebar logo, not README links."""

    def __init__(self, body):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.crates = []
        self.versions = []
        self.logos = []
        self.heading = ""
        self.feed(body.decode("utf-8"))

    def handle_starttag(self, tag, attributes):
        attributes = dict(attributes)
        if tag == "meta" and attributes.get("name") == "rustdoc-vars":
            self.crates.append(attributes.get("data-current-crate"))
        sidebar = any("sidebar-crate" in attrs.get("class", "").split() for _, attrs in self.stack)
        if tag == "img" and sidebar and attributes.get("alt") == "logo":
            self.logos.append(attributes.get("src"))
        if tag not in ("area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"):
            self.stack.append((tag, attributes))

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        if any(tag == "h1" for tag, _ in self.stack):
            self.heading += data
        if (any("sidebar-crate" in attrs.get("class", "").split() for _, attrs in self.stack)
                and any("version" in attrs.get("class", "").split() for _, attrs in self.stack)
                and data.strip()):
            self.versions.append(data.strip())

    def matches(self, name, version, logo):
        crate = name.replace("-", "_")
        return (self.crates == [crate] and self.versions == [version] and self.logos == [logo]
                and re.match(r"^Crate\s+" + re.escape(crate) + r"(?:\s|$)", self.heading.strip()) is not None)


class Release:
    PACKAGES = ("providers-kit", "storage-kit", "tools-kit", "agents-kit", "app-kit")
    REPOSITORY = "jecsand838/ai-kit"
    DOCS_TIMEOUT = 1200

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
        if any(not re.fullmatch(r"[1-9][0-9]*", self.env.get(key, ""))
               for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")):
            raise ReleaseError("release admission requires an Actions run and attempt identity")
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
        self.report.update(version=self.version, revision=self.revision, repository=self.REPOSITORY,
                           run_id=self.env["GITHUB_RUN_ID"], run_attempt=self.env["GITHUB_RUN_ATTEMPT"],
                           qualification="INITIAL_0_0_1_OVERRIDE" if self.override else "MAINTAINER_DECLARED",
                           qualification_url=self.env.get("QUALIFICATION_URL", ""),
                           actor=self.env["GITHUB_ACTOR"], triggering_actor=self.env["GITHUB_TRIGGERING_ACTOR"],
                           deferred=("strict-live; slow integration, capacity and full qualification; "
                                     "Rust mock execution, rustdoc/doctests and package build verification"
                                     if self.override else "strict-live evidence is supplied by the maintainer; not rerun here"))
        return self.report

    def request(self, url, *, method="GET", payload=None, github=False, exact=False, timeout=30):
        headers = {"Accept": "*/*" if exact else "application/json",
                   "User-Agent": "ai-kit-release (https://github.com/jecsand838/ai-kit)"}
        if github:
            token = self.env.get("GH_TOKEN")
            if not token:
                raise ReleaseError("GH_TOKEN is required for GitHub release identity checks")
            headers.update(Authorization="Bearer " + token, **{"X-GitHub-Api-Version": "2022-11-28"})
        body = None if payload is None else json.dumps(payload).encode()
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        previous_alarm = None
        if exact:
            # Finalization runs on the Ubuntu runner's main thread. A wall-clock
            # alarm also bounds slow streaming bodies, unlike socket timeouts.
            if any(signal.getitimer(signal.ITIMER_REAL)):
                raise ReleaseError("hosted documentation probe cannot replace an active process deadline")
            previous_alarm = signal.signal(signal.SIGALRM, self.request_deadline)
            signal.setitimer(signal.ITIMER_REAL, timeout)
        try:
            open_request = urllib.request.build_opener(ReleaseRedirects()).open if exact else urllib.request.urlopen
            try:
                response = open_request(request, timeout=timeout)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                body = response.read(8 * 1024 * 1024 + 1)
                if len(body) > 8 * 1024 * 1024:
                    raise ReleaseError(f"{method} {url}: response exceeds the release probe limit")
                return {"status": response.status, "url": response.geturl(), "body": body,
                        "headers": {key.lower(): value for key, value in response.headers.items()}}
        except (OSError, ValueError) as error:
            raise ReleaseError(f"{method} {url}: request failed") from error
        finally:
            if exact:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, previous_alarm)

    @staticmethod
    def request_deadline(_signal_number, _frame):
        raise TimeoutError("hosted documentation request deadline reached")

    def json_request(self, url, *, method="GET", payload=None, github=False):
        response = self.request(url, method=method, payload=payload, github=github)
        if response["status"] == 404 and method == "GET":
            return None
        if not 200 <= response["status"] < 300:
            raise ReleaseError(f"{method} {url}: HTTP {response['status']}")
        try:
            return json.loads(response["body"])
        except ValueError as error:
            raise ReleaseError(f"{method} {url}: invalid JSON") from error

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

    def release_identity(self):
        tag = self.tag_identity()
        release = self.github("releases/tags/v" + self.version)
        if release is not None and (tag is None or release.get("draft")):
            raise ReleaseError("an existing draft or unbound release needs maintainer review")
        return release

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

    def publication_receipt(self, path):
        path = Path(path).resolve()
        raw = path.read_bytes()
        try:
            receipt = json.loads(raw)
        except ValueError as error:
            raise ReleaseError("publication receipt contains invalid JSON") from error
        if (not isinstance(receipt, dict) or type(receipt.get("schema")) is not int
                or receipt["schema"] != 1 or receipt.get("status") != "PACKAGES_PUBLISHED"):
            raise ReleaseError("invalid or incomplete publication receipt")
        for key in ("repository", "revision", "version", "run_id", "qualification", "qualification_url", "actor"):
            if receipt.get(key) != self.report[key]:
                raise ReleaseError(f"publication receipt {key} differs from this admitted release")
        attempt = receipt.get("run_attempt", "")
        if (not isinstance(attempt, str) or not re.fullmatch(r"[1-9][0-9]*", attempt)
                or int(attempt) > int(self.env["GITHUB_RUN_ATTEMPT"])):
            raise ReleaseError("invalid publication receipt run attempt")
        rows = receipt.get("packages")
        if (not isinstance(rows, list) or len(rows) != len(self.PACKAGES)
                or any(not isinstance(row, dict) for row in rows)
                or [row.get("name") for row in rows] != list(self.PACKAGES)):
            raise ReleaseError("publication receipt must contain the five ordered package identities")
        for row in rows:
            archive = f"release-build/package/{row['name']}-{self.version}.crate"
            checksum = row.get("sha256", "")
            if (row.get("version") != self.version
                    or row.get("status") not in ("PUBLISHED", "PUBLISHED_AFTER_CARGO_ERROR", "ALREADY_PUBLISHED_IDENTICAL")
                    or row.get("archive") != "target/" + archive
                    or not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum)):
                raise ReleaseError(f"{row['name']}: invalid publication archive identity")
            archived = (path.parent.parent / archive).resolve()
            if (not archived.is_relative_to(path.parent.parent)
                    or hashlib.sha256(archived.read_bytes()).hexdigest() != checksum):
                raise ReleaseError(f"{row['name']}: downloaded archive differs from the publication receipt")
        self.report.update(packages=rows, publication={"sha256": hashlib.sha256(raw).hexdigest(),
                           "run_id": receipt["run_id"], "run_attempt": attempt})

    def confirm_registry(self):
        for row in self.report["packages"]:
            version = self.registry(row["name"])
            if version is None:
                raise ReleaseError(f"{row['name']}: published registry version is missing")
            self.same_archive(row["name"], version, row["sha256"])

    def logo_identity(self):
        logos = set()
        for name in self.PACKAGES:
            source = (self.root / name / "src/lib.rs").read_text()
            urls = re.findall(r'html_logo_url\s*=\s*"([^"]+)"', source)
            if len(urls) != 1:
                raise ReleaseError(f"{name}: expected one reviewed rustdoc logo")
            logos.add(urls[0])
        if len(logos) != 1:
            raise ReleaseError("all five crates must share the reviewed rustdoc logo")
        url = logos.pop()
        match = re.fullmatch(r"https://raw\.githubusercontent\.com/" + re.escape(self.REPOSITORY)
                             + r"/[0-9a-f]{40}/(docs/assets/[^/]+\.png)", url)
        if match is None:
            raise ReleaseError("rustdoc logo must use the reviewed commit-pinned image URL")
        body = (self.root / match[1]).read_bytes()
        if not body.startswith(b"\x89PNG\r\n\x1a\n"):
            raise ReleaseError("reviewed logo is not a PNG image")
        return url, hashlib.sha256(body).hexdigest()

    def docs_probe(self, url, deadline, attempt, label):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ReleaseError("hosted documentation deadline reached")
        folder = self.output / "docs" / f"attempt-{attempt:03d}"
        folder.mkdir(parents=True, exist_ok=True)
        observation = {"requested_url": url, "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            response = self.request(url, exact=True, timeout=min(30, remaining))
            observation.update(status=response["status"], effective_url=response["url"],
                               headers={key: value for key, value in response["headers"].items()
                                        if key in ("content-type", "date", "cache-control", "age", "x-cache", "location")},
                               sha256=hashlib.sha256(response["body"]).hexdigest())
            (folder / (label + ".body")).write_bytes(response["body"])
            if time.monotonic() >= deadline:
                raise ReleaseError("hosted documentation deadline reached")
            if response["status"] != 200 or response["url"] != url:
                raise ReleaseError(f"{url}: not ready (HTTP {response['status']} or non-exact URL)")
            return response
        except ReleaseError as error:
            observation["error"] = str(error)
            raise
        finally:
            (folder / (label + ".json")).write_text(json.dumps(observation, indent=2) + "\n")

    def wait_for_docs(self):
        logo_url, logo_sha256 = self.logo_identity()
        deadline = time.monotonic() + min(self.DOCS_TIMEOUT, 1200)
        attempt = 0
        while True:
            if time.monotonic() >= deadline:
                raise ReleaseError("hosted documentation was not ready within the bounded wait; retry finalization only")
            attempt += 1
            rows = []
            for name in self.PACKAGES:
                row = {"name": name, "version": self.version, "status": "NOT_READY"}
                rows.append(row)
                try:
                    status = self.docs_probe(f"https://docs.rs/crate/{name}/{self.version}/status.json",
                                             deadline, attempt, name + "-status")
                    document = json.loads(status["body"])
                    if (status["headers"].get("content-type", "").split(";")[0].strip().lower() != "application/json"
                            or not isinstance(document, dict) or document.get("version") != self.version
                            or document.get("doc_status") is not True):
                        raise ReleaseError(f"{name}: exact-version documentation is not ready")
                    root = self.docs_probe(f"https://docs.rs/{name}/{self.version}/{name.replace('-', '_')}/index.html",
                                           deadline, attempt, name + "-root")
                    if (root["headers"].get("content-type", "").split(";")[0].strip().lower() != "text/html"
                            or not RustdocPage(root["body"]).matches(name, self.version, logo_url)):
                        raise ReleaseError(f"{name}: hosted root identity or logo does not match")
                    row["status"] = "READY"
                except (ValueError, UnicodeError) as error:
                    row["error"] = str(error)
            docs = {"attempt": attempt, "packages": rows, "logo_url": logo_url, "logo_sha256": logo_sha256}
            try:
                asset = self.docs_probe(logo_url, deadline, attempt, "logo")
                if (asset["headers"].get("content-type", "").split(";")[0].strip().lower() != "image/png"
                        or not asset["body"].startswith(b"\x89PNG\r\n\x1a\n")
                        or hashlib.sha256(asset["body"]).hexdigest() != logo_sha256):
                    raise ReleaseError("hosted logo bytes or content type differ from the reviewed asset")
                docs["logo_status"] = "READY"
            except ReleaseError as error:
                docs.update(logo_status="NOT_READY", logo_error=str(error))
            self.report["docs"] = docs
            self.save()
            if all(row["status"] == "READY" for row in rows) and docs["logo_status"] == "READY":
                docs["verified_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReleaseError("hosted documentation was not ready within the bounded wait; retry finalization only")
            time.sleep(min(30, remaining))

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
                "The five exact-version docs.rs roots and their reviewed logo were verified before "
                "GitHub release finalization. Publication and finalization receipts are retained in "
                f"[the release run](https://github.com/{self.REPOSITORY}/actions/runs/{self.env['GITHUB_RUN_ID']}).\n")

    def publish(self):
        try:
            self.admit()
            self.save()
            if not self.env.get("CARGO_REGISTRY_TOKEN"):
                raise ReleaseError("CARGO_REGISTRY_TOKEN must be configured as an Actions secret")
            self.release_identity()
            for name in self.PACKAGES:
                self.publish_package(name)
            if self.git("status", "--porcelain=v1", "--untracked-files=normal"):
                raise ReleaseError("source changed while publishing; GitHub release was not created")
            self.release_identity()
            self.report.update(status="PACKAGES_PUBLISHED")
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            self.report.update(status="FAIL", error=str(error))
            raise
        finally:
            self.save()
        return self.report

    def finalize(self, report_path):
        if Path(report_path).resolve() == (self.output / "report.json").resolve():
            raise ReleaseError("finalization requires a separate immutable publication artifact")
        self.report = {"schema": 1, "status": "INCOMPLETE", "packages": []}
        try:
            self.admit()
            self.publication_receipt(report_path)
            self.confirm_registry()
            self.release_identity()
            self.wait_for_docs()
            self.confirm_registry()
            if self.git("status", "--porcelain=v1", "--untracked-files=normal"):
                raise ReleaseError("source changed while finalizing; GitHub release was not created")
            release = self.release_identity() or self.github("releases", method="POST", payload={
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
        parser.add_argument("command", choices=("preflight", "publish", "finalize"))
        parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
        parser.add_argument("--report", help="immutable publication artifact's release/report.json")
        args = parser.parse_args()
        if (args.command == "finalize") != bool(args.report):
            parser.error("--report is required only for finalize")
        release = cls(args.root)
        try:
            if args.command == "finalize":
                result = release.finalize(args.report)
            else:
                result = release.publish() if args.command == "publish" else release.admit()
            print(json.dumps(result, indent=2))
            return 0
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print(f"Release failed: {error}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    sys.exit(Release.main())

# Local security dependency regressions

Run `python3 scripts/check_security_compatibility.py` from the repository root.
This explicit local command creates a temporary, separate Cargo package under
its evidence directory. It does not add a workspace member. GitHub Actions and
normal Python test discovery do not run these dependency tests.

The eight required cases cover Zstd decoding of pre-update bytes and truncation,
include-flate compression/decompression, pre-update compressed Arrow IPC,
object_store's actual S3 XML decoding of escaped and malformed data, and Geo
boolean operations, holes/boundaries, and square-capped buffering. The S3 cases
use a bounded local HTTP fixture, require no credentials, and incur no service
usage. These cases do not prove Lance-specific persisted encoding compatibility
or substitute for Storage's native backend gates.

`fixtures.json` records the immutable legacy inputs, generator toolchain and
dependency versions from baseline `061f62b`. The old versions in
`legacy-Cargo.lock` are historical fixture-generation evidence; they are not
dependencies of the workspace or the current compatibility runner. The runner
verifies their hashes before testing current dependencies and records its
separate resolved lock, complete test inventories, named execution results, and
zero ignored/filtered/failed counts.

To independently regenerate the legacy bytes, copy `legacy-Cargo.toml` and
`legacy-Cargo.lock` to `Cargo.toml` and `Cargo.lock` in a new temporary directory,
copy `legacy-writer.rs` to its `src/main.rs`, then run
`cargo +1.99.0 run --locked --manifest-path <temporary-directory>/Cargo.toml -- <new-output-directory>`.
Compare the four generated payload/frame/IPC hashes with `fixtures.json`.
Do not regenerate the committed fixtures during ordinary validation.

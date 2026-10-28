#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
source "${WORKSPACE_DIR}/scripts/release_stage.sh"
DOC_ROOT="${WORKSPACE_DIR}/target/doc"
DOC_BUILD_TARGET_DIR="${WORKSPACE_DIR}/target/docs-site-build"
DOC_BUILD_ROOT="${DOC_BUILD_TARGET_DIR}/doc"
DOC_PUBLISH_ROOT="${WORKSPACE_DIR}/target/docs-site-publish"
LANDING_PAGE="${DOC_ROOT}/index.html"
STATIC_INDEX="${WORKSPACE_DIR}/docs/rustdoc-index.html"
ASSET_SOURCE_DIR="${WORKSPACE_DIR}/docs/assets"
ASSET_TARGET_DIR="${DOC_ROOT}/assets"
# Expected generated pages:
# - target/doc/index.html
# - target/doc/agents_kit/index.html
# - target/doc/app_kit/index.html
# - target/doc/providers_kit/index.html
# - target/doc/storage_kit/index.html
# - target/doc/tools_kit/index.html

cd "${WORKSPACE_DIR}"

printf 'Using workspace: %s\n' "${WORKSPACE_DIR}"

printf '\n==> repository documentation inventory and links (Python 3.11+)\n'
release_stage docs.contract python3 -m unittest scripts.tests.test_documentation_contract

printf '\n==> cargo doc --locked -p providers-kit --all-features --no-deps\n'
release_stage_fresh docs.provider.rustdoc env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" RUSTDOCFLAGS="-D warnings -D missing-docs -D rustdoc::broken-intra-doc-links -D rustdoc::private-intra-doc-links -D rustdoc::invalid-codeblock-attributes -D rustdoc::invalid-rust-codeblocks -D rustdoc::invalid-html-tags -D rustdoc::bare-urls -D rustdoc::redundant-explicit-links -D rustdoc::missing-crate-level-docs -D rustdoc::unescaped-backticks" cargo doc --locked -p providers-kit --all-features --no-deps

printf '\n==> cargo doc --locked -p tools-kit --all-features --no-deps\n'
release_stage_fresh docs.tool.rustdoc env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" RUSTDOCFLAGS="-D warnings -D missing-docs -D rustdoc::broken-intra-doc-links -D rustdoc::private-intra-doc-links -D rustdoc::invalid-codeblock-attributes -D rustdoc::invalid-rust-codeblocks -D rustdoc::invalid-html-tags -D rustdoc::bare-urls -D rustdoc::redundant-explicit-links -D rustdoc::missing-crate-level-docs -D rustdoc::unescaped-backticks" cargo doc --locked -p tools-kit --all-features --no-deps

printf '\n==> cargo doc --locked -p storage-kit --no-deps\n'
release_stage_fresh docs.storage.rustdoc.lance env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" RUSTDOCFLAGS="-D warnings -D missing-docs -D rustdoc::broken-intra-doc-links -D rustdoc::private-intra-doc-links -D rustdoc::invalid-codeblock-attributes -D rustdoc::invalid-rust-codeblocks -D rustdoc::invalid-html-tags -D rustdoc::bare-urls -D rustdoc::redundant-explicit-links -D rustdoc::missing-crate-level-docs -D rustdoc::unescaped-backticks" cargo doc --locked -p storage-kit --no-deps

printf '\n==> cargo doc --locked -p storage-kit --no-default-features --features redb,postgres,surrealdb,sqlite --no-deps\n'
release_stage_fresh docs.storage.rustdoc.redb-postgres-surrealdb-sqlite env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" RUSTDOCFLAGS="-D warnings -D missing-docs -D rustdoc::broken-intra-doc-links -D rustdoc::private-intra-doc-links -D rustdoc::invalid-codeblock-attributes -D rustdoc::invalid-rust-codeblocks -D rustdoc::invalid-html-tags -D rustdoc::bare-urls -D rustdoc::redundant-explicit-links -D rustdoc::missing-crate-level-docs -D rustdoc::unescaped-backticks" cargo doc --locked -p storage-kit --no-default-features --features redb,postgres,surrealdb,sqlite --no-deps


printf '\n==> restore generated storage-kit docs to default Lance-only lane\n'
release_stage_fresh docs.storage.rustdoc.lance.restore env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" RUSTDOCFLAGS="-D warnings -D missing-docs -D rustdoc::broken-intra-doc-links -D rustdoc::private-intra-doc-links -D rustdoc::invalid-codeblock-attributes -D rustdoc::invalid-rust-codeblocks -D rustdoc::invalid-html-tags -D rustdoc::bare-urls -D rustdoc::redundant-explicit-links -D rustdoc::missing-crate-level-docs -D rustdoc::unescaped-backticks" cargo doc --locked -p storage-kit --no-deps

printf '\n==> cargo doc --locked -p agents-kit --all-features --no-deps\n'
release_stage_fresh docs.agent.rustdoc env RUSTDOCFLAGS="-D warnings -D missing-docs -D rustdoc::broken-intra-doc-links -D rustdoc::private-intra-doc-links -D rustdoc::invalid-codeblock-attributes -D rustdoc::invalid-rust-codeblocks -D rustdoc::invalid-html-tags -D rustdoc::bare-urls -D rustdoc::redundant-explicit-links -D rustdoc::missing-crate-level-docs -D rustdoc::unescaped-backticks" cargo doc --locked -p agents-kit --all-features --no-deps \
    --target-dir "${DOC_BUILD_TARGET_DIR}"

printf '\n==> cargo doc --locked -p app-kit --no-deps\n'
release_stage_fresh docs.app.rustdoc env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" RUSTDOCFLAGS="-D warnings -D missing-docs -D rustdoc::broken-intra-doc-links -D rustdoc::private-intra-doc-links -D rustdoc::invalid-codeblock-attributes -D rustdoc::invalid-rust-codeblocks -D rustdoc::invalid-html-tags -D rustdoc::bare-urls -D rustdoc::redundant-explicit-links -D rustdoc::missing-crate-level-docs -D rustdoc::unescaped-backticks" cargo doc --locked -p app-kit --no-deps

printf '\n==> stage generated rustdoc for target/doc\n'
rm -rf "${DOC_PUBLISH_ROOT}"
mkdir -p "${DOC_PUBLISH_ROOT}"
cp -R "${DOC_BUILD_ROOT}/." "${DOC_PUBLISH_ROOT}"

printf '\n==> install stable workspace docs index\n'
cp "${STATIC_INDEX}" "${DOC_PUBLISH_ROOT}/index.html"

printf '\n==> install shared docs assets\n'
mkdir -p "${DOC_PUBLISH_ROOT}/assets"
cp -R "${ASSET_SOURCE_DIR}/." "${DOC_PUBLISH_ROOT}/assets"

printf '\n==> replace target/doc with staged docs\n'
rm -rf "${DOC_ROOT}"
mv "${DOC_PUBLISH_ROOT}" "${DOC_ROOT}"

printf '\n==> cargo test --locked -p agents-kit --doc --all-features -- --nocapture\n'
release_stage docs.agent.doctest cargo test --locked -p agents-kit --doc --all-features \
    --target-dir "${DOC_BUILD_TARGET_DIR}" -- --nocapture

printf '\n==> cargo test --locked -p app-kit --doc -- --nocapture\n'
release_stage docs.app.doctest env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" cargo test --locked -p app-kit --doc -- --nocapture

printf '\n==> cargo test --locked -p providers-kit --doc --all-features -- --nocapture\n'
release_stage docs.provider.doctest env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" cargo test --locked -p providers-kit --doc --all-features -- --nocapture

printf '\n==> cargo test --locked -p tools-kit --doc --all-features -- --nocapture\n'
release_stage docs.tool.doctest env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" cargo test --locked -p tools-kit --doc --all-features -- --nocapture

printf '\n==> cargo test --locked -p storage-kit --doc -- --nocapture\n'
release_stage docs.storage.doctest.lance env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" cargo test --locked -p storage-kit --doc -- --nocapture

printf '\n==> cargo test --locked -p storage-kit --doc --no-default-features --features redb,postgres,surrealdb,sqlite -- --nocapture\n'
release_stage docs.storage.doctest.redb-postgres-surrealdb-sqlite env CARGO_TARGET_DIR="${DOC_BUILD_TARGET_DIR}" cargo test --locked -p storage-kit --doc --no-default-features --features redb,postgres,surrealdb,sqlite -- --nocapture


for page in \
    "${LANDING_PAGE}" \
    "${ASSET_TARGET_DIR}/ai-kit-logo.png" \
    "${DOC_ROOT}/agents_kit/index.html" \
    "${DOC_ROOT}/app_kit/index.html" \
    "${DOC_ROOT}/providers_kit/index.html" \
    "${DOC_ROOT}/storage_kit/index.html" \
    "${DOC_ROOT}/tools_kit/index.html"; do
    if [[ ! -f "${page}" ]]; then
        printf 'missing generated rustdoc page: %s\n' "${page}" >&2
        exit 1
    fi
done

printf '\nDocs landing page: %s\n' "${LANDING_PAGE}"

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn ui_fixture(name: &str) -> PathBuf {
    manifest_dir().join("tests/ui").join(name)
}

fn cargo_toml_path_value(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}

fn temp_project(name: &str, source: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().expect("temp dir should build");
    let crate_name = name.replace(['.', '-'], "_");
    let src_dir = temp.path().join("src");
    let cargo_dir = temp.path().join(".cargo");
    fs::create_dir_all(&src_dir).expect("src dir should exist");
    fs::create_dir_all(&cargo_dir).expect(".cargo dir should exist");
    fs::write(src_dir.join("main.rs"), source).expect("main.rs should write");
    fs::write(
        temp.path().join("Cargo.toml"),
        format!(
            r#"[package]
name = "{crate_name}"
version = "0.0.0"
edition = "2024"

[workspace]

[dependencies]
tools-kit = {{ path = "{}", features = ["mcp-server"] }}
"#,
            cargo_toml_path_value(&manifest_dir()),
        ),
    )
    .expect("Cargo.toml should write");
    fs::write(cargo_dir.join("config.toml"), "[net]\noffline = true\n")
        .expect(".cargo/config.toml should write");
    let workspace_lock = manifest_dir()
        .parent()
        .expect("workspace root should exist")
        .join("Cargo.lock");
    if workspace_lock.is_file() {
        fs::copy(workspace_lock, temp.path().join("Cargo.lock"))
            .expect("workspace Cargo.lock should copy");
    }
    temp
}

fn run_compile_fail_case(name: &str, expected_fragments: &[&str]) {
    let source = fs::read_to_string(ui_fixture(name)).expect("ui fixture should read");
    let project = temp_project(name, &source);
    let target_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("compile-contracts");
    let output = Command::new("cargo")
        .arg("check")
        .arg("--offline")
        .arg("--quiet")
        .current_dir(project.path())
        .env("CARGO_TARGET_DIR", target_dir)
        .output()
        .expect("nested cargo check should run");

    assert!(
        !output.status.success(),
        "expected `{name}` to fail to compile, but it succeeded"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    for fragment in expected_fragments {
        assert!(
            stderr.contains(fragment),
            "expected `{name}` stderr to contain `{fragment}`\nfull stderr:\n{stderr}"
        );
    }
}

#[test]
fn tools_kit_public_surface_rejects_backend_payload_access() {
    run_compile_fail_case(
        "toolset_direct_init.rs",
        &[
            "no `EmptyToolsetBackend` in the root",
            "error[E0599]",
            "named `Empty` found for struct `Toolset`",
        ],
    );
}

#[test]
fn tool_extension_contract_enforces_async_and_shared_ownership_bounds() {
    run_compile_fail_case(
        "tool_handler_future_not_send.rs",
        &["future cannot be sent between threads safely"],
    );
    run_compile_fail_case(
        "toolset_backend_not_sync.rs",
        &["cannot be shared between threads safely"],
    );
}

#[test]
fn tool_extension_contract_keeps_erased_adapters_private() {
    run_compile_fail_case(
        "tool_backend_erasure_private.rs",
        &["ErasedToolHandlerBackend", "ErasedToolsetBackend"],
    );
}

#[test]
fn child_handler_ingress_is_not_public() {
    run_compile_fail_case(
        "tool_handler_from_backend_private.rs",
        &[
            "error[E0599]",
            "named `from_backend` found for struct `ToolHandler`",
        ],
    );
}

#[test]
fn supplied_tool_behavior_requires_its_public_traits_in_scope() {
    run_compile_fail_case(
        "tool_behavior_without_traits.rs",
        &[
            "error[E0599]",
            "named `list_tools` found for struct `Toolset`",
            "named `tool_handler` found for struct `Toolset`",
            "named `shutdown` found for struct `Toolset`",
            "named `step` found for struct `ToolHandler`",
        ],
    );
}

#[test]
fn unique_runtime_owners_are_not_cloneable() {
    run_compile_fail_case("tool_handler_not_clone.rs", &["no method named `clone`"]);
    run_compile_fail_case("mcp_server_not_clone.rs", &["no method named `clone`"]);
}

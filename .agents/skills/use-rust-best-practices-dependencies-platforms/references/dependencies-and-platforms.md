# Rust dependencies, Cargo hygiene, and cross-platform support

Use this reference when selecting crates, shaping Cargo manifests, or keeping code portable across supported operating systems.

## 1. Toolchain and MSRV policy

Treat these as separate decisions:

- **current stable toolchain** — what you use to build new code today
- **edition** — the language edition for the package
- **`rust-version`** — the minimum Rust version the package intentionally supports

### Good policy

- respect repo pins and CI first
- use the current stable edition for new crates
- set `rust-version` only when the repository wants to guarantee support for that floor
- upgrade editions intentionally with `cargo fix --edition` plus normal build/test/lint review

### Avoid

- hard-coding a specific stable patch version into every local skill
- treating `rust-version` as “whatever stable is today”
- silently changing a repository’s support floor because a new toolchain exists

## 2. Prefer `std` and Cargo-native features first

Before adding a dependency, ask:

- Is this already in `std`?
- Does Cargo already solve this?
- Is there already a crate in the repo or workspace that covers the need?
- Would a small local adapter be better than a new transitive tree?

Do not add crates for trivial helpers unless the dependency meaningfully improves correctness, safety, or maintainability.

## 3. Use a durable dependency evaluation rubric

Instead of a hard-coded global “blessed list”, evaluate crates like this:

### Prefer

- official Rust/Cargo facilities when they exist
- well-documented, actively maintained crates
- crates with a clear MSRV/support story
- crates with focused feature surfaces
- crates with reasonable transitive dependency size
- crates whose unsafe/FFI use is documented and justified
- crates with licenses compatible with the repository

### Be cautious when

- documentation is thin
- the crate is abandoned or stale
- default features pull in a lot of unnecessary surface area
- the crate requires platform-specific native libraries
- the crate hides large amounts of unsafe code without clear explanation

### Commonly strong defaults in many Rust codebases

These are often good options when the repo’s policy allows them:

- `serde` / `serde_json`
- `thiserror`
- `anyhow` at application boundaries
- `tokio`
- `tracing` / `tracing-subscriber`
- `reqwest`
- `tonic`
- `clap`

But do not treat even these as mandatory. Repository context still matters.

## 4. Use precise Cargo version and feature syntax

### Good version specs

```toml
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Avoid wildcard versions

```toml
serde = "*"
```

### Disable default features when you do not want them

```toml
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
```

### Use optional dependencies deliberately

```toml
[dependencies]
metrics-exporter-prometheus = { version = "0.15", optional = true }

[features]
prometheus = ["dep:metrics-exporter-prometheus"]
```

The `dep:` syntax keeps the optional dependency internal unless you intentionally expose a feature name.

## 5. Centralize workspace dependencies and lints when helpful

### Good workspace inheritance

```toml
[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
thiserror = "2"
tracing = "0.1"
```

```toml
[dependencies]
serde.workspace = true
thiserror.workspace = true
tracing.workspace = true
```

### Important lint inheritance reminder

If the workspace defines `[workspace.lints]`, each member still needs:

```toml
[lints]
workspace = true
```

Do not assume workspace lints apply automatically.

## 6. Use target-specific dependencies for OS-specific crates

### Good

```toml
[target.'cfg(unix)'.dependencies]
nix = "0.30"

[target.'cfg(windows)'.dependencies]
windows-sys = { version = "0.61", features = ["Win32_System_Console"] }
```

### With `cargo add`

```bash
cargo add --target 'cfg(unix)' nix
cargo add --target 'cfg(windows)' windows-sys
```

This keeps portable crates free from platform-only dependencies when they are not needed.

## 7. Run supply-chain and compatibility checks

Use tooling proportionate to the repository’s risk profile.

### Strong defaults

```bash
cargo audit
cargo deny check
```

### Consider for stricter environments

```bash
cargo vet
cargo semver-checks check-release
```

Use these in CI where appropriate, especially for public libraries, security-sensitive code, or larger organizations.

## 8. Use `ProjectDirs` for application directories

Do not manually construct app config paths like:

- `~/.config/my-app`
- `%APPDATA%\My App`

Those rules differ by platform, especially on macOS.

### Good

```rust
use directories::ProjectDirs;
use std::path::PathBuf;

pub fn config_dir() -> PathBuf {
    let dirs = ProjectDirs::from("com", "Example Corp", "My App")
        .expect("platform should provide standard directories");
    dirs.config_dir().to_path_buf()
}
```

This follows Linux/XDG, Windows, and macOS conventions correctly.

### Avoid

```rust
use std::path::PathBuf;

pub fn bad_config_dir() -> PathBuf {
    if cfg!(windows) {
        std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .expect("APPDATA must exist")
            .join("My App")
    } else {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .expect("HOME must exist")
            .join(".config")
            .join("my-app")
    }
}
```

This misses platform conventions and special cases.

## 9. Use `Path` / `PathBuf` instead of string path manipulation

### Good

```rust
use std::path::{Path, PathBuf};

pub fn migration_path(root: &Path) -> PathBuf {
    root.join("migrations").join("20260413_init.sql")
}
```

### Avoid

```rust
pub fn bad_migration_path(root: &str) -> String {
    format!("{root}/migrations/20260413_init.sql")
}
```

String concatenation bakes in path separator assumptions.

## 10. Prefer `std::process::Command` over shell strings

### Good

```rust
use std::process::Command;

pub fn git_status() -> std::io::Result<std::process::Output> {
    Command::new("git")
        .arg("status")
        .arg("--short")
        .output()
}
```

### Avoid when possible

```rust
use std::process::Command;

pub fn bad_git_status() -> std::io::Result<std::process::Output> {
    if cfg!(windows) {
        Command::new("cmd").arg("/C").arg("git status --short").output()
    } else {
        Command::new("sh").arg("-c").arg("git status --short").output()
    }
}
```

Shelling out through a shell adds quoting and portability hazards unless shell features are explicitly required.

## 11. Isolate OS-specific behavior behind modules or adapters

### Good

```rust
#[cfg(unix)]
mod signals;

#[cfg(windows)]
mod signals;

pub use signals::install_shutdown_handler;
```

Keep the public surface stable while each platform-specific module handles its own details.

## 12. Be deliberate with environment mutation in Rust 2024-era code

Environment mutation APIs such as `std::env::set_var` and `remove_var` require extra care in Rust 2024-era guidance.

Practical advice:

- prefer process-local configuration objects over mutating global environment
- keep environment mutation at process bootstrap if it must exist
- avoid hidden global env mutation inside library functions

## 13. Dependency and platform dos and don'ts

### Do

- respect repo toolchain and MSRV policy
- centralize shared versions when it improves consistency
- disable unnecessary default features
- use target-specific dependencies
- use `ProjectDirs`, `PathBuf`, and `Command`
- run supply-chain checks appropriate to the repo

### Don't

- use wildcard dependency versions
- add dependencies for trivial helpers without a strong reason
- assume workspace lints auto-apply to members
- manually build OS-specific app directory paths
- shell out through `sh -c` / `cmd /C` when direct command invocation works
- treat the newest stable version as the package’s MSRV

## Further reading

- Cargo manifest reference: <https://doc.rust-lang.org/cargo/reference/manifest.html>
- Cargo features: <https://doc.rust-lang.org/cargo/reference/features.html>
- Cargo specifying dependencies: <https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html>
- Cargo lints: <https://doc.rust-lang.org/cargo/reference/lints.html>
- Cargo `cargo add`: <https://doc.rust-lang.org/cargo/commands/cargo-add.html>
- `directories::ProjectDirs`: <https://docs.rs/directories/latest/directories/struct.ProjectDirs.html>
- `std::process::Command`: <https://doc.rust-lang.org/std/process/struct.Command.html>


## 14. Additional merged guidance

### Blessed vs non-blessed crates

There is no universal official global “blessed crate list” for application development. Treat crate choices in these buckets instead:

- **official / authoritative**: standard library, Cargo, Rust documentation, RustSec data
- **widely adopted ecosystem defaults**: crates like `serde`, `thiserror`, `tokio`, `tracing`, `reqwest`, `tonic`, and `clap` when they fit the task
- **repo-approved local defaults**: crates your workspace has already vetted, documented, and standardized on

The right question is usually not “is this crate globally blessed?” but “is this crate justified, maintainable, and compatible with this repository’s policy?”

### Cross-compilation notes

- prefer publishing the supported target matrix explicitly in CI
- keep OS-specific crates behind `target.'cfg(...)'.dependencies` and narrow modules
- prefer `rustls`-based stacks when they reduce native TLS friction and fit the repo’s requirements
- add targets intentionally with `rustup target add ...` and verify build/test paths for the targets you claim to support

### Additional source links

- Rust 1.94.0 release announcement: <https://blog.rust-lang.org/2026/03/05/Rust-1.94.0/>
- Latest Rust release index: <https://blog.rust-lang.org/releases/latest/>
- Cargo `rust-version`: <https://doc.rust-lang.org/cargo/reference/rust-version.html>
- Cargo workspaces: <https://doc.rust-lang.org/cargo/reference/workspaces.html>
- Cargo specifying dependencies: <https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html>
- Cargo manifest reference: <https://doc.rust-lang.org/cargo/reference/manifest.html>
- Rust platform support: consult the official Rust target-tier documentation.
- Rust conditional compilation reference: <https://doc.rust-lang.org/reference/conditional-compilation.html>
- rustup cross-compilation guide: <https://rust-lang.github.io/rustup/cross-compilation.html>
- RustSec advisory database: <https://rustsec.org/>
- `cargo-audit`: <https://github.com/RustSec/rustsec/tree/main/cargo-audit>
- `cargo-deny`: <https://embarkstudios.github.io/cargo-deny/>
- `cargo-vet`: <https://mozilla.github.io/cargo-vet/>

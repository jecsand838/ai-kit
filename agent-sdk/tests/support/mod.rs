use std::fs;
use std::path::{Path, PathBuf};

pub(crate) fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("agent-sdk manifest lives below workspace root")
        .to_path_buf()
}

pub(crate) fn read_workspace_file(path: impl AsRef<Path>) -> String {
    let path = workspace_root().join(path);
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

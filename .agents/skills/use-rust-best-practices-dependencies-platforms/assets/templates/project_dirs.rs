use directories::ProjectDirs;
use std::path::PathBuf;

pub fn config_dir() -> PathBuf {
    let dirs = ProjectDirs::from("com", "Example Corp", "My App")
        .expect("platform should provide standard directories");
    dirs.config_dir().to_path_buf()
}

use std::process::Command;

#[test]
fn help_output_mentions_config_flag() {
    let output = Command::new(env!("CARGO_BIN_EXE_my-app"))
        .arg("--help")
        .output()
        .expect("binary should run");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout should be utf-8");
    assert!(stdout.contains("--config"));
}

/// Tests for the CLI meta-flags handled by `maybe_handle_meta_flags`.
/// These do not require a dbt project and are never marked `#[ignore]`.

fn bin() -> assert_cmd::Command {
    assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"))
}

#[test]
fn test_help_exits_zero() {
    bin().arg("--help").assert().success();
}

#[test]
fn test_help_short_flag_exits_zero() {
    bin().arg("-h").assert().success();
}

#[test]
fn test_help_contains_usage() {
    let output = bin().arg("--help").output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("USAGE"),
        "expected USAGE section in --help output, got:\n{stdout}"
    );
}

#[test]
fn test_help_lists_fix_flag() {
    let output = bin().arg("--help").output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--fix"),
        "expected --fix in --help output, got:\n{stdout}"
    );
}

#[test]
fn test_help_lists_no_color_flag() {
    let output = bin().arg("--help").output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--no-color"),
        "expected --no-color in --help output, got:\n{stdout}"
    );
}

#[test]
fn test_version_exits_zero() {
    bin().arg("--version").assert().success();
}

#[test]
fn test_version_short_flag_exits_zero() {
    bin().arg("-V").assert().success();
}

#[test]
fn test_version_contains_package_version() {
    let output = bin().arg("--version").output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "expected version {} in --version output, got:\n{stdout}",
        env!("CARGO_PKG_VERSION")
    );
}

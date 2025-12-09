use criterion::{Criterion, criterion_group, criterion_main};
use fs_extra::dir::CopyOptions;
use std::ffi::OsString;
use std::path::Path;
use tempfile::TempDir;

use dbt_lint_yaml::{
    check_all_with_report,
    config::ConfigFile,
    project::{DbtContext, load_project_from_cli_args},
    writeback,
};

/// Setup a fresh copy of the jaffle_shop fixture for benchmarking.
/// This ensures we're testing with a clean state each time.
fn setup_fixture() -> TempDir {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest_dir.join("tests").join("jaffle_shop");
    let temp = TempDir::new().expect("failed to create temp dir");
    let tests_dest = temp.path().join("tests");
    std::fs::create_dir_all(&tests_dest).expect("failed to create dest");
    let mut options = CopyOptions::new();
    options.copy_inside = true;
    fs_extra::dir::copy(&fixture, &tests_dest, &options).expect("failed to copy fixture");
    temp
}

fn load_project_at(temp_path: &Path) -> DbtContext {
    let args = vec![
        OsString::from("dbt-lint-yaml"),
        OsString::from("parse"),
        OsString::from("--project-dir"),
        OsString::from(temp_path.join("tests/jaffle_shop")),
    ];
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(load_project_from_cli_args(args))
        .expect("failed to load project")
}

fn criterion_benchmark(c: &mut Criterion) {
    // Benchmark 1: Check phase only (without writeback)
    // This establishes a baseline for the checking logic.
    let temp_check = setup_fixture();
    let project_check = load_project_at(temp_check.path());
    let config_check = match ConfigFile::resolve(&project_check.invocation_args) {
        Ok(cfg) => cfg,
        Err(err) => panic!("Failed to load configuration: {err}"),
    };

    c.bench_function("check_all_no_fix", |b| {
        b.iter(|| check_all_with_report(&project_check.manifest, &config_check, |_| {}))
    });

    // Benchmark 2: Full cycle with --fix (check + batched writeback)
    // This is the main benchmark showing the complete flow with the optimized batching.
    // With Option A (batch grouping), we read/write each file only once,
    // regardless of how many models are in that file.
    let temp_fix = setup_fixture();
    let project_fix = load_project_at(temp_fix.path());
    let config_fix = match ConfigFile::resolve(&project_fix.invocation_args) {
        Ok(cfg) => cfg.with_fix(true),
        Err(err) => panic!("Failed to load configuration: {err}"),
    };

    c.bench_function("check_all_with_fix_and_writeback", |b| {
        b.iter(|| {
            let check_result = check_all_with_report(&project_fix.manifest, &config_fix, |_| {});
            if !check_result.model_changes.is_empty() {
                let _ = writeback::apply_model_changes(
                    project_fix.project_dir.as_path(),
                    &check_result.model_changes,
                    &config_fix,
                );
            }
        })
    });

    // Benchmark 3: Writeback phase only
    // This isolates the performance of the writeback layer. Changes are pre-collected,
    // so we're measuring just the I/O and file manipulation logic.
    // Useful for understanding how efficiently we're batching file operations.
    let temp_writeback = setup_fixture();
    let project_writeback = load_project_at(temp_writeback.path());
    let config_writeback = match ConfigFile::resolve(&project_writeback.invocation_args) {
        Ok(cfg) => cfg.with_fix(true),
        Err(err) => panic!("Failed to load configuration: {err}"),
    };

    // Pre-compute changes once, then repeatedly run writeback on them
    let check_result =
        check_all_with_report(&project_writeback.manifest, &config_writeback, |_| {});

    c.bench_function("writeback_only", |b| {
        b.iter(|| {
            let _ = writeback::apply_model_changes(
                project_writeback.project_dir.as_path(),
                &check_result.model_changes,
                &config_writeback,
            );
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

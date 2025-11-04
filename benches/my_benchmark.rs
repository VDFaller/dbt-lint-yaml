use criterion::{Criterion, criterion_group, criterion_main};
use std::ffi::OsString;

use dbt_lint_yaml::{
    check_all,
    config::ConfigFile,
    project::{DbtContext, load_project_from_cli_args},
};

fn jaffle_shop_context() -> DbtContext {
    let args = vec![OsString::from("dbt-lint-yaml"), OsString::from("parse")];
    let _context: DbtContext = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(load_project_from_cli_args(args))
        .unwrap();
    _context
}

fn criterion_benchmark(c: &mut Criterion) {
    let project = jaffle_shop_context();
    let config = match ConfigFile::resolve(&project.invocation_args) {
        Ok(cfg) => cfg,
        Err(err) => {
            panic!("Failed to load configuration: {err}");
        }
    };
    c.bench_function("check_all", |b| {
        b.iter(|| check_all(&project.manifest, &config))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

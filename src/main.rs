use clap::Parser;
use dbt_lint_yaml::{
    check::{CheckEvent, ColumnChange, check_all_with_report},
    config::Config,
    writeback,
};

use dbt_common::{CodeLocation, FsResult, cancellation::CancellationTokenSource};
use dbt_jinja_utils::{
    invocation_args::InvocationArgs, listener::JinjaTypeCheckingEventListenerFactory,
};
use dbt_loader::{
    args::{IoArgs, LoadArgs},
    load,
};
use dbt_parser::{args::ResolveArgs, resolver::resolve};
use dbt_sa_cli::dbt_sa_clap::{Cli, from_main};
use dbt_schemas::{
    schemas::{Nodes, manifest::build_manifest},
    state::Macros,
};
use minijinja::{TypecheckingEventListener, machinery::Span};
use std::{any::Any, collections::HashSet, ffi::OsString, path::Path, rc::Rc, sync::Arc};

const PKG_NAME: &str = env!("CARGO_PKG_NAME");
const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

fn maybe_handle_version_override() {
    use std::ffi::OsStr;

    let mut args = std::env::args_os();
    // skip program name
    let _ = args.next();

    for arg in args {
        if arg == OsStr::new("--") {
            break;
        }

        if arg == OsStr::new("--version") || arg == OsStr::new("-V") {
            println!("{PKG_NAME} {PKG_VERSION}");
            std::process::exit(0);
        }
    }
}

fn extract_shimmed_flags(args: Vec<OsString>) -> (Vec<OsString>, bool, bool) {
    let mut verbose = false;
    let mut fix = false;
    let mut filtered = Vec::new();
    let mut iter = args.into_iter();

    if let Some(program) = iter.next() {
        filtered.push(program);
    }

    let mut passthrough = false;
    for arg in iter {
        if passthrough {
            filtered.push(arg);
            continue;
        }
        if arg == "--" {
            passthrough = true;
            filtered.push(arg);
            continue;
        }
        if arg == "--verbose" || arg == "-v" {
            verbose = true;
            continue;
        }
        if arg == "--fix" {
            fix = true;
            continue;
        }
        filtered.push(arg);
    }

    (filtered, verbose, fix)
}

#[derive(Default)]
struct NullJinjaTypeCheckingEventListenerFactory;

impl JinjaTypeCheckingEventListenerFactory for NullJinjaTypeCheckingEventListenerFactory {
    fn create_listener(
        &self,
        _io_args: &IoArgs,
        _location: CodeLocation,
        _ignored_warning_ids: Option<HashSet<u32>>,
        _package_name: &str,
    ) -> Rc<dyn TypecheckingEventListener> {
        Rc::new(NullTypecheckingEventListener)
    }

    fn destroy_listener(&self, _path: &Path, _listener: Rc<dyn TypecheckingEventListener>) {}
}

#[derive(Default)]
struct NullTypecheckingEventListener;

impl TypecheckingEventListener for NullTypecheckingEventListener {
    fn as_any(&self) -> &(dyn Any + 'static) {
        self
    }

    fn warn(&self, _message: &str) {}

    fn set_span(&self, _span: &Span) {}

    fn new_block(&self, _level: usize) {}

    fn flush(&self) {}

    fn on_lookup(&self, _span: &Span, _kind: &str, _name: &str, _segments: Vec<Span>) {}
}

fn report_event(event: CheckEvent<'_>, verbose: bool) {
    match event {
        CheckEvent::Model(model_result) => {
            if model_result.is_pass() {
                if verbose {
                    println!("\x1b[32msuccess:\x1b[0m {} passed", model_result.model_id());
                }
            } else {
                println!("\x1b[31merror:\x1b[0m {} failed", model_result.model_id());
                for reason in model_result.failure_reasons() {
                    println!("    * {reason}");
                }
            }
        }
        CheckEvent::Source(source_result) => {
            if source_result.is_pass() {
                if verbose {
                    println!(
                        "\x1b[32msuccess:\x1b[0m {} passed",
                        source_result.source_id()
                    );
                }
            } else {
                println!("\x1b[31merror:\x1b[0m {} failed", source_result.source_id());
                for reason in source_result.failure_reasons() {
                    println!("    * {reason}");
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> FsResult<()> {
    maybe_handle_version_override();

    let raw_args: Vec<OsString> = std::env::args_os().collect();
    let (filtered_args, verbose, fix_flag) = extract_shimmed_flags(raw_args);

    let cli = Cli::parse_from(filtered_args);
    let system_args = from_main(&cli);

    let eval_args = cli.to_eval_args(system_args)?;
    let invocation_id = eval_args.io.invocation_id.to_string();

    let load_args = LoadArgs::from_eval_args(&eval_args);
    let project_dir = load_args.io.in_dir.clone();
    let config = Config::from_toml(&project_dir).with_fix(fix_flag);

    let invocation_args = InvocationArgs::from_eval_args(&eval_args);
    let _cts = CancellationTokenSource::new();
    let token = _cts.token();

    let (dbt_state, threads, _) = load(&load_args, &invocation_args, &token).await?;

    let eval_args = eval_args
        .with_target(dbt_state.dbt_profile.target.to_string())
        .with_threads(threads);

    let resolve_args = ResolveArgs::try_from_eval_args(&eval_args)?;
    let invocation_args = InvocationArgs::from_eval_args(&eval_args);

    let listener_factory: Arc<dyn JinjaTypeCheckingEventListenerFactory> =
        Arc::new(NullJinjaTypeCheckingEventListenerFactory);

    let (resolved_state, _jinja_env) = resolve(
        &resolve_args,
        &invocation_args,
        dbt_state,
        Macros::default(),
        Nodes::default(),
        &token,
        listener_factory,
    )
    .await?;

    let dbt_manifest = build_manifest(&invocation_id, &resolved_state);

    let check_result = check_all_with_report(&dbt_manifest, &config, |event| {
        report_event(event, verbose);
    });

    for (model, model_changes) in check_result.model_changes.iter() {
        println!("Model: {model} has found changes");
        for (column, column_changes) in model_changes.column_changes.iter() {
            for change in column_changes {
                match change {
                    ColumnChange::DescriptionChanged { new, .. } => {
                        println!(
                            "  Column: {} - New Description: {}",
                            column,
                            new.as_deref().unwrap_or("None"),
                        );
                    }
                }
            }
        }
    }

    if config.fix {
        if let Some(model_changes) =
            (!check_result.model_changes.is_empty()).then_some(&check_result.model_changes)
        {
            match writeback::apply_model_changes(project_dir.as_path(), model_changes, &config) {
                Ok(applied) => {
                    for (model_id, columns) in applied {
                        if columns.is_empty() {
                            continue;
                        }
                        println!("Applied ruamel.yaml updates for {model_id}: {columns:?}");
                    }
                }
                Err(err) => {
                    eprintln!("Failed to apply YAML updates: {err}");
                }
            }
        }
    } else if !check_result.model_changes.is_empty() {
        println!("Fixes available; re-run with --fix to apply them.");
    }

    if check_result.has_failures() {
        std::process::exit(1);
    }
    println!("\x1b[32mAll checks passed\x1b[0m");

    Ok(())
}

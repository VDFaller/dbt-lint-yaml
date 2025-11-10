use dbt_common::FsResult;
use dbt_lint_yaml::{
    change_descriptors::ColumnChange,
    check::{CheckEvent, check_all_with_report},
    config::ConfigFile,
    project::load_project_from_cli_args,
    writeback,
};
use std::ffi::OsString;

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
        if arg == "parse" {
            // skip parse so it's backwards compatible with prior CLI
            continue;
        }
        filtered.push(arg);
    }

    (filtered, verbose, fix)
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
        CheckEvent::Exposure(exposure_result) => {
            if exposure_result.failures.is_empty() {
                if verbose {
                    println!(
                        "\x1b[32msuccess:\x1b[0m {} passed",
                        exposure_result.exposure_id
                    );
                }
            } else {
                println!(
                    "\x1b[31merror:\x1b[0m {} failed",
                    exposure_result.exposure_id
                );
                for reason in &exposure_result.failures {
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
    let (mut filtered_args, verbose, fix_flag) = extract_shimmed_flags(raw_args);
    filtered_args.insert(1, OsString::from("parse"));

    let project = load_project_from_cli_args(filtered_args).await?;

    // where I come in
    let config = match ConfigFile::resolve(&project.invocation_args) {
        Ok(cfg) => cfg.with_fix(fix_flag),
        Err(err) => {
            eprintln!("Failed to load configuration: {err}");
            std::process::exit(2);
        }
    };
    let check_result = check_all_with_report(&project.manifest, &config, |event| {
        report_event(event, verbose);
    });

    for (model, model_changes) in check_result.model_changes.iter() {
        println!("Model: {model} has found changes");
        for (column, column_changes) in model_changes.column_changes.iter() {
            for change in column_changes {
                match change {
                    ColumnChange::ChangePropertiesFile => {
                        println!("  Column: {column} - properties file will be regenerated");
                    }
                }
            }
        }
    }

    if config.fix {
        if let Some(model_changes) =
            (!check_result.model_changes.is_empty()).then_some(&check_result.model_changes)
        {
            match writeback::apply_model_changes(
                project.project_dir.as_path(),
                model_changes,
                &config,
            ) {
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

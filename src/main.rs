use clap::Parser;
use dbt_lint_yaml::{check_all, config::Config, writeback};

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
use std::{any::Any, collections::HashSet, path::Path, rc::Rc, sync::Arc};

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
        Rc::new(NullTypecheckingEventListener::default())
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

#[tokio::main]
async fn main() -> FsResult<()> {
    let cli = Cli::parse();
    let system_args = from_main(&cli);

    let eval_args = cli.to_eval_args(system_args)?;
    let invocation_id = eval_args.io.invocation_id.to_string();

    let load_args = LoadArgs::from_eval_args(&eval_args);
    let project_dir = load_args.io.in_dir.clone();
    let config = Config::from_toml(&project_dir);

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
        Arc::new(NullJinjaTypeCheckingEventListenerFactory::default());

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

    let check_result = check_all(&dbt_manifest, &config);

    for (model, model_changes) in check_result.model_changes.iter() {
        println!("Model: {} has found changes", model);
        for (column, column_changes) in model_changes.column_changes.iter() {
            for change in column_changes {
                println!(
                    "  Column: {} - New Description: {}",
                    column,
                    change.new_description.as_deref().unwrap_or("None"),
                );
            }
        }
    }

    if let Some(model_changes) =
        (!check_result.model_changes.is_empty()).then(|| &check_result.model_changes)
    {
        match writeback::apply_model_changes_with_ruamel(project_dir.as_path(), model_changes) {
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

    if !check_result.failures.is_empty() {
        println!("{}", check_result.failures);
        std::process::exit(1);
    }

    Ok(())
}

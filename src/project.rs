//! Module for loading a dbt project based on CLI arguments.
//! Steals heavily from dbt-sa-cli's project loading logic.
//! The main purpose is to just get the manifest (since serde can't read it from the manifest.json)
use clap::Parser;
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
    schemas::{
        Nodes,
        manifest::{DbtManifestV12, build_manifest},
    },
    state::Macros,
};
use minijinja::{TypecheckingEventListener, machinery::Span};
use std::{
    any::Any,
    collections::HashSet,
    ffi::OsString,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

pub struct DbtContext {
    pub manifest: DbtManifestV12,
    pub invocation_args: InvocationArgs,
    pub project_dir: PathBuf,
}

pub async fn load_project_from_cli_args(filtered_args: Vec<OsString>) -> FsResult<DbtContext> {
    let cli = Cli::parse_from(filtered_args);
    let system_args = from_main(&cli);

    let eval_args = cli.to_eval_args(system_args)?;
    let invocation_id = eval_args.io.invocation_id.to_string();

    let load_args = LoadArgs::from_eval_args(&eval_args);
    let project_dir = load_args.io.in_dir.clone();

    let invocation_args = InvocationArgs::from_eval_args(&eval_args);
    let _cts = CancellationTokenSource::new();
    let token = _cts.token();

    let (dbt_state, _threads, _) = load(&load_args, &invocation_args, &token).await?;

    let resolve_args = ResolveArgs::try_from_eval_args(&eval_args)?;
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

    let manifest = build_manifest(&invocation_id, &resolved_state);

    Ok(DbtContext {
        manifest,
        invocation_args,
        project_dir,
    })
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

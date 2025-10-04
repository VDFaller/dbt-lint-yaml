use clap::Parser;
use dbt_lint_yaml::check_all;

use dbt_common::{FsResult, cancellation::CancellationTokenSource};
use dbt_jinja_utils::invocation_args::InvocationArgs;
use dbt_loader::{args::LoadArgs, load};
use dbt_parser::{args::ResolveArgs, resolver::resolve};
use dbt_sa_cli::dbt_sa_clap::{Cli, from_main};
use dbt_schemas::{
    schemas::{Nodes, manifest::build_manifest},
    state::Macros,
};

#[tokio::main]
async fn main() -> FsResult<()> {
    let cli = Cli::parse();
    let system_args = from_main(&cli);

    let eval_args = cli.to_eval_args(system_args)?;
    let invocation_id = eval_args.io.invocation_id.to_string();

    let load_args = LoadArgs::from_eval_args(&eval_args);
    let invocation_args = InvocationArgs::from_eval_args(&eval_args);
    let _cts = CancellationTokenSource::new();
    let token = _cts.token();

    let (dbt_state, threads, _) = load(&load_args, &invocation_args, &token).await?;

    let eval_args = eval_args
        .with_target(dbt_state.dbt_profile.target.to_string())
        .with_threads(threads);

    let resolve_args = ResolveArgs::try_from_eval_args(&eval_args)?;
    let invocation_args = InvocationArgs::from_eval_args(&eval_args);

    let (resolved_state, _jinja_env) = resolve(
        &resolve_args,
        &invocation_args,
        dbt_state,
        Macros::default(),
        Nodes::default(),
        None, // omit the optional event listener for the simplest case
        &token,
    )
    .await?;
    
    let mut dbt_manifest = build_manifest(&invocation_id, &resolved_state);
    
    let failures = check_all(&mut dbt_manifest);
    // writing the yamls back to disk I figure will work like this
    // Step 1: Go through manifest in DAG order and create some type of "These columns got changed from None to Some(description)"
    // Step 2: Use the path information to read the yamls back in, update them, and write them back out
    //   The big problem with this is that serde_yaml doesn't preserve comments or formatting, so the output files would be very different from the input files.
    //   Could possibly do something cute with regex to just replace the description lines in the original files, but that seems fragile.
    //   The manifest isn't made to serialize to yaml directly anyway, so we need some transitional layer. 
    println!(
        "Nodes without description: {:?}",
        failures.model_failures.no_descriptions.len()
    );
    println!(
        "Number of models without tags: {}",
        failures.model_failures.no_tags.len()
    );
    println!(
        "Models with columns missing descriptions: {:?}",
        failures.model_failures.column_failures.len()
    );

    println!(
        "Sources without description: {:?}",
        failures.source_failures.no_descriptions.len()
    );

    Ok(())
}

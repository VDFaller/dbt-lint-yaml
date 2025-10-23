# dbt-lint configuration

This document describes how to configure dbt-lint for your project using `dbt-lint.toml`.

## Where to place the file

Place a file named `dbt-lint.toml` at the root of your dbt project (the same directory you run dbt from).

Example:

```
/my-dbt-project/
  dbt_project.yml
  models/
  seeds/
  dbt-lint.toml    <-- put your configuration here
```

If no `dbt-lint.toml` is present, the tool will use [built-in defaults](default-dbt-lint.toml).

## What you can configure

Top-level keys in `dbt-lint.toml` map to runtime options and selectors. Common keys:

- `select`: array of selectors to run
- `exclude`: array of selectors to exclude
- `fixable`: selectors that can be auto-fixed
- `unfixable`: selectors explicitly not fixable
- `model_fanout_threshold`: threshold for fanout checks
- `required_tests`: list of tests required
- `render_descriptions`: boolean, if false, descriptions will keep their raw jinja format.
- `writeback`: method for writeback ("python" or "rust")

All supported keys are validated; typos will be rejected with helpful suggestions.


## Target-aware overrides (dev vs ci vs prod)

It is recommended to have the least strict base configuration, and then override specific values per target (e.g. `dev`, `ci`, `prod`).
Names must match what is in your profiles.yml for the profile/target.

You can provide target-specific or profile+target-specific overrides in the same TOML file. Supported patterns are:

1) Target-only override

```
[target.dev]
# values here override top-level keys when target == "dev"
```

2) Profile + target override (most specific)

```
[target.ci.dbx]
# values here apply when profile == "ci" and target == "dbx"
```

Precedence rules:
- If both `profile` and `target` are set for the run, `target.<profile>.<target>` is applied (most specific).
- Otherwise, `target.<target>` is applied if present.

Merge semantics:
- TOML tables are deep-merged recursively.
- Non-table values (including arrays) are replaced by the override value.
  - Example: if `select` is an array in the override, it replaces the base `select` array (it does not append).


## Examples

1) Base config + per-target override

```toml
# dbt-lint.toml
select = ["missing_column_descriptions"]
model_fanout_threshold = 3

[target.dev]
model_fanout_threshold = 5
select = ["missing_column_descriptions", "missing_model_descriptions"]
```

Running with 
```
dbt-lint-yaml parse --target dev
```

results in `model_fanout_threshold = 5` and the `select` list from the `[target.dev]` block.

2) Profile + target override

```toml
model_fanout_threshold = 3

[target.databricks.ci]
model_fanout_threshold = 10
```

Running with 
```
dbt-lint-yaml parse --profile databricks --target ci
```

results in `model_fanout_threshold = 10`. If `ci` is the default target for `databricks`, the same value applies when running without `--target`.

## Error messages & validation

The tool validates the top-level keys in the merged configuration. If an unknown key is present, the error message will include:
- The unknown key name
- A suggested key if a close match is found
- The list of supported keys

Example:

```
Unknown config key `models_fanout_threshold`. Did you mean `model_fanout_threshold`?
Supported keys: select, exclude, fixable, unfixable, model_fanout_threshold, required_tests, render_descriptions, writeback
```

This helps catch typos early.


## Troubleshooting

- Override not applied:
  - Make sure the run is using the expected `target` name.
  - If `profile` is set, check for a `target.<profile>.<target>` override; that takes precedence over `target.<target>`.
  - Remember **arrays replace**, they do not append.

- Unknown key error:
  - Check the suggested key and the supported keys list in the error.

- Want to see the effective configuration?
  - Currently you can reproduce it by running the tool in the intended `profile`/`target` environment or use the test helper in the codebase.
  - Consider adding a small debug run in CI or enabling a `--show-effective-config` feature (future improvement).


## Best practices

- Keep base config minimal and put environment-specific changes under `[target.*]`.
- Be explicit in overrides (for arrays include the full desired list).
- Use `target.<profile>.<target>` for CI/more specific overrides.

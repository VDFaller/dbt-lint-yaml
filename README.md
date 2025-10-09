# dbt-lint-yaml
A dbt linter for YAML files.

This is a rust based CLI tool that takes inspiration from these three great tools:
* [dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis)
* [dbt-project-evaluator](https://github.com/dbt-labs/dbt-project-evaluator)
* [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint)

The goal is to have something like ruff for the yaml portion of dbt projects.  


## Installation
You can install the latest release from GitHub:

``` bash
curl -fsSL https://raw.githubusercontent.com/VDFaller/dbt-lint-yaml/refs/heads/main/install.sh | sh -s -- --update
```

In addition to Rust, the current write-back flow shells out to a Python helper that
uses [`ruamel.yaml`](https://pypi.org/project/ruamel.yaml/) to preserve formatting.
Make sure Python 3 is available on your `$PATH` (or your active environment's $PATH) and install the dependency with:

```bash
pip install ruamel.yaml
```

## Usage
For now we're piggy-backing on [dbt-sa-cli](https://github.com/dbt-labs/dbt-sa-cli) for command-line interface functionality. So you still have to pass a parse argument.

``` bash
dbt-lint-yaml parse
```

The rules are controlled by a `dbt-lint.yml` file in the root of your dbt project. 
The default rules can be found in the [default-dbt-lint.yml](./default-dbt-lint.toml) file.

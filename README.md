# dbt-lint-yaml
A dbt linter for YAML files.

This is a rust based CLI tool that takes inspiration from these three great tools:
* [dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis)
* [dbt-project-evaluator](https://github.com/dbt-labs/dbt-project-evaluator)
* [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint)

The goal is to have something like ruff for the yaml portion of dbt projects.  
Something like: 
```
dbt-lint-yaml check --fix
```


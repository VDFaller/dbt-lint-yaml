## DBT fusion pipedream for checking the project
I want a `dbtf check --fix` command that will run a series of checks on the dbt project, and fix any issues it can.
These fixes are based off [dbt_project_evaluator](https://dbt-labs.github.io/dbt-project-evaluator/latest/), but I think looking at [dbt_checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) would be useful as well. 


## safely fixable errors
* missing/invalid columns in properties file
    * non manifest
    * if the properties file doesn't exist, create it with best practices?
	* if properties file does exist, just add or delete columns
* missing/invalid column descriptions for models
    * missing could be pulled from manifest
    * pull from upstream if it's a passthrough or rename (like [dbt-osmosis](https://github.com/z3z1ma/dbt-osmosis))
        * if we want it to know if it's a passthrough, I think we need more than the manifest
	* keep it a docs block if it's a docs block
	* fct_undocumented_public_models/fct_undocumented_models
	* `force-inherit-descriptions` would overwrite existing descriptions
* missing/invalid column data types
    * sort of already done
* hard coded references? 
	* there might be some reasons why people hardcode
* fct_source_directories, could be moved to the appropriate yml.
* fct_test_directories, could be moved to the appropriate yml.
* fct_test_directories, could be moved to the appropriate yml.
* fct_public_models_without_contract??
    * could just add the config
	* would break stuff if the columns aren't documented correctly


## Unsafe Fixes
* Anything where we'd need to rename existing models
	* why it's unsafe: it could break downstream tools that rely on name
    * mart models only? 
	* if your branch creates a new model maybe we could fix it to fit convention?
	* fct_model_naming_conventions
* fct_duplicate_sources 
   * why it's unsafe: don't know which the user would want to keep
   * how we could fix: prefer the non-identifier one?
* fct_multiple_sources_joined
   * why it's unsafe: 
   * how we could fix: prefer the non-identifier one?
* fct_unused_sources
   * why it's unsafe: it's deleting stuff
   * how we could fix: delete it


### Harder Unsafe Fixes
* fct_multiple_sources_joined
    * could make base tables for them? 


## Fixes we shouldn't/couldn't do
* fct_model_fanout
   * why it's unsafe: no clear way to auto refactor
* fct_rejoining_of_upstream_concepts 
* fct_root_models
    * isn't this effectively the same as hard_coded_references, because there are plenty of exceptions I think of this one 
* fct_source_fanout
* fct_staging_dependent_on_marts_or_intermediate
* fct_staging_dependent_on_staging
* fct_too_many_joins
* fct_missing_primary_key_tests
    * though I think adding `dbt_expectations.expect_compound_columns_to_be_unique` might be a good idea for it.
* fct_sources_without_freshness
* fct_test_coverage
* fct_documentation_coverage
* fct_undocumented_source_tables
* fct_undocumented_sources
* fct_chained_views_dependencies
* fct_exposure_parents_materializations
* fct_exposures_dependent_on_private_models


## How we we control? 
* The `dbt_project.yml`?
   * I personally don't like using the variables as a way to configure tests. I think having everything together for linting would be better. 
* Similar to `ruff.toml`
* Do we have defaults? 
    * If so how do we do exceptions
* 

### Basic rules when running --fix
* keep anchors and aliases as written
* don't fight with ruff or sqlfluff, let them do their thing
* unsafe rules would be opt-in with a warning:


## Codegen
* https://github.com/dbt-labs/dbt-mcp/issues/265 talks about using toolsets for codegen.
* Missing Properties file
    * generate it, it should have all columns
        * Column descriptions should be pulled from upstream if it's a passthrough or rename
        * Data types should be known from fusion
    * no data tests
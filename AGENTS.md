# AGENTS.md
The overall goal of this project is located in [pipedream.md](pipedream.md).

I'm still learning Rust, so some of the code here may be a bit clunky.  Any suggestions for improvements are welcome! But please please please explain what I did wrong and why you're suggesting a change. 
 
## Dev environment tips
- This project heavily relies on the [dbt-labs/dbt-fusion](https://github.com/dbt-labs/dbt-fusion) family of crates.  Search there for examples of how to use the various crates.
  * I also have a local copy of dbt-fusion that I use for testing.  It is located at `../dbt-fusion` relative to this repo.

 
## Testing instructions
* Unit testing can be done with `cargo fmt && cargo test`
* Unit testing WITH Integration testing can be done with `cargo fmt && uv run cargo test -- --ignored` (since the integration tests are currently marked as ignored)
    * integration tests depend on ruamel.yaml being installed in your python environment.
* Linting can be done with clippy: `cargo clippy --all-targets --all-features -- -D warnings`

## Basic architecture

* [check](src/check/) is for all the rules
  * each module is a type of dbt asset (model, source, seed, etc)
  * each module has a set of rules that apply to that type of asset
  * we try to follow the pattern of one rule = one function
    * rules should return `Result<Option<XChange>, XFailure>` where `X` is the type of asset being checked
      * `Ok(None)` means the check passed
      * `Ok(Some(XChange))` means the check failed, and an autofix is given in the `XChange` struct  
      * `Err(XFailure)` means the check failed in a way that we couldn't autofix ()
* [writeback](src/writeback/) is for applying the changes to the actual files
  * the important thing to note here is that we have two paths for writeback:
    * python-based writeback using ruamel.yaml because it preserves comments and formatting
    * rust-based writeback using serde_yaml because it's faster and easier to maintain
    * THESE SHOULD NOT BE MIXED. If you're adding writeback in the python path, do not use dbt_serde_yaml to writeback, and vice versa.
* [osmosis](src/osmosis.rs) is used to infer upstream comments for dbt models based on column names currently.

## Documentation
* all the end user facing documentation is in the `docs/` folder.  
* A good template to use when adding new rules is [docs/rules/missing_column_descriptions.md](docs/rules/missing_column_descriptions.md)
* don't forget to update the [index.md](docs/rules/index.md) file when you add new rules!

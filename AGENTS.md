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

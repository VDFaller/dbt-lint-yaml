# AGENTS.md
The overall goal of this project is located in [pipedream.md](pipedream.md).

I'm still learning Rust, so some of the code here may be a bit clunky.  Any suggestions for improvements are welcome! But please please please explain what I did wrong and why you're suggesting a change. 
 
## Dev environment tips
- This project heavily relies on the [dbt-labs/dbt-fusion](https://github.com/dbt-labs/dbt-fusion) family of crates.  Search there for examples of how to use the various crates.
  * I also have a local copy of dbt-fusion that I use for testing.  It is located at `../dbt-fusion` relative to this repo.

 
## Testing instructions
- along with `cargo fmt` and `cargo test`, run `cargo clippy --all-targets --all-features -- -D warnings`
- pseudo integration tests can be run with `cargo run -- parse --project-dir tests/jaffle_shop`
 

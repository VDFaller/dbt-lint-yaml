# AGENTS.md
The overall goal of this project is located in [pipedream.md](pipedream.md).
 
## Dev environment tips
- This project heavily relies on the [dbt-labs/dbt-fusion](https://github.com/dbt-labs/dbt-fusion) family of crates.  Search there for examples of how to use the various crates.
  * I also have a local copy of dbt-fusion that I use for testing.  It is located at `../dbt-fusion` relative to this repo.

 
## Testing instructions
- along with `cargo fmt` and `cargo test`, I have setup a test repo so that you can run `cargo run -- parse --project-dir ../dbt-fusion/crates/dbt-init/assets/jaffle_shop/` to see how it works.  You can also point it at your own dbt project.
 

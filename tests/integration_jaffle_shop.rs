use assert_fs::TempDir;
use dbt_lint_yaml::writeback::properties::PropertyFile;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

// Helper to create a temp directory and copy the `tests/jaffle_shop` fixture into it.
fn setup_jaffle_shop_fixture(
    toml_override: Option<&str>,
) -> Result<assert_fs::TempDir, Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = manifest_dir.join("tests").join("jaffle_shop");
    let temp = TempDir::new()?;
    let tests_dest = temp.path().join("tests");
    std::fs::create_dir_all(&tests_dest)?;
    let mut options = fs_extra::dir::CopyOptions::new();
    options.copy_inside = true;
    fs_extra::dir::copy(&fixture, &tests_dest, &options)?;

    // If the caller provided a toml override, write it into the copied fixture to replace
    // the default `dbt-lint.toml` used by the fixture.
    if let Some(toml) = toml_override {
        let target = temp.path().join("tests/jaffle_shop/dbt-lint.toml");
        fs::write(target, toml)?;
    }

    Ok(temp)
}

// should work on python or rust
fn base_check(temp: &TempDir) -> Result<(), Box<dyn Error>> {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"));
    cmd.arg("parse")
        .arg("--fix")
        .arg("--project-dir")
        .arg(temp.path().join("tests/jaffle_shop"));
    cmd.assert().failure();

    // read the updated file and check for expected description updates
    let order_items_yml = temp
        .path()
        .join("tests/jaffle_shop/models/marts/order_items.yml");
    let contents = fs::read_to_string(&order_items_yml)?;
    let properties: PropertyFile = dbt_serde_yaml::from_str(&contents)?;

    // verify propogation of descriptions more than one level deep
    let model = &properties.models.expect("no models found")[0];
    assert_eq!(
        model.name.as_ref().expect("missing model name"),
        "order_items",
        "model name mismatch"
    );

    let order_item_id = &model.columns[0];
    // comes from stg_orders.order_item_id, goes no further because raw_orders is renamed as id.
    // This should be better after SDF
    assert_eq!(
        order_item_id
            .description
            .as_ref()
            .expect("missing order_item_id description"),
        "The unique key for each order item.",
        "order_items first column description mismatch"
    );
    let order_id = &model.columns[1];
    // verify that jinja doc() references are preserved
    // comes from stg_order_items.order_id -> raw_items.order_id
    assert_eq!(
        order_id
            .description
            .as_ref()
            .expect("missing order_id description"),
        "{{doc('order_id')}}",
        "order_items second column description mismatch"
    );

    // verify that it creates missing properties files
    let locations_yml = temp
        .path()
        .join("tests/jaffle_shop/models/marts/locations.yml");
    assert!(locations_yml.exists(), "locations.yml was not created");
    Ok(())
}

#[test]
#[ignore = "reason: Codegen won't work until compile is SA'd."]
fn test_parse_fix_updates_order_items_python() -> Result<(), Box<dyn Error>> {
    // run with --fix on jaffle shop fixture
    let temp = setup_jaffle_shop_fixture(None)?;
    base_check(&temp)
}

#[test]
#[ignore = "reason: Codegen won't work until compile is SA'd."]
fn test_parse_fix_updates_order_items_rust() -> Result<(), Box<dyn Error>> {
    // run with --fix on jaffle shop fixture, using rust parser
    let toml_override = r#"
    writeback = "rust"
    "#;
    let temp = setup_jaffle_shop_fixture(Some(toml_override))?;
    base_check(&temp)
}

#[test]
#[ignore = "reason: Don't have a profiles.yml yet."]
fn test_model_properties_layout_rebase() -> Result<(), Box<dyn Error>> {
    let temp = setup_jaffle_shop_fixture(None)?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"));
    cmd.arg("parse")
        .arg("--fix")
        .arg("-t")
        .arg("rebase")
        // using --project-dir so I can have my ENV var and github actions work
        .arg("--project-dir")
        .arg(temp.path().join("tests/jaffle_shop"));
    cmd.assert().success();

    // verify that the models have been moved to per-directory properties files
    let marts_yml = temp
        .path()
        .join("tests/jaffle_shop/models/marts/_marts__models.yml");
    assert!(marts_yml.exists(), "marts models file was not created");

    let customers_yml = temp
        .path()
        .join("tests/jaffle_shop/models/marts/customers.yml");
    assert!(!customers_yml.exists(), "customers.yml was not deleted");

    // order_items has unit tests so it should exist but shouldn't have model definitions
    let order_items_yml = temp
        .path()
        .join("tests/jaffle_shop/models/marts/order_items.yml");
    assert!(order_items_yml.exists(), "order_items.yml was not found");
    let contents = fs::read_to_string(&order_items_yml)?;
    let properties: PropertyFile = dbt_serde_yaml::from_str(&contents)?;
    assert!(
        properties.models.is_none(),
        "order_items.yml should not have model definitions"
    );

    Ok(())
}

/// Verifies that the `source_directories` check flags sources not in their expected
/// subdirectory, and that `--fix` extracts each source block into a per-source YAML file.
///
/// The jaffle_shop fixture ships with `models/staging/__sources.yml` which contains both
/// the `ecom` and `analytics` source blocks at the staging root — neither is in its own
/// `<source_name>/` subdirectory.  After `--fix` each source should live in:
///   `models/staging/ecom/_ecom__sources.yml`
///   `models/staging/analytics/_analytics__sources.yml`
/// and the original `__sources.yml` should be deleted.
#[test]
#[ignore = "needs a profiles.yml for full project load"]
fn test_source_directories_fix_splits_sources() -> Result<(), Box<dyn Error>> {
    let toml_override = r#"
select = ["source_directories"]
"#;
    let temp = setup_jaffle_shop_fixture(Some(toml_override))?;
    let staging = temp.path().join("tests/jaffle_shop/models/staging");

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"));
    cmd.arg("parse")
        .arg("--fix")
        .arg("--project-dir")
        .arg(temp.path().join("tests/jaffle_shop"));
    cmd.assert().failure(); // failures are still reported even when fixes are applied

    // Each source block should now have its own file under staging/<source_name>/
    assert!(
        staging.join("ecom/_ecom__sources.yml").exists(),
        "ecom source file should be created"
    );
    assert!(
        staging.join("analytics/_analytics__sources.yml").exists(),
        "analytics source file should be created"
    );

    // Original shared file should be gone
    assert!(
        !staging.join("__sources.yml").exists(),
        "original __sources.yml should be deleted after all sources are moved"
    );

    Ok(())
}

/// Verifies that a staging model placed outside the staging directory is flagged.
///
/// Moves `stg_customers.sql` and its YAML from `models/staging/` to `models/marts/` and
/// confirms the linter exits non-zero with `model_directories` enabled.
#[test]
#[ignore = "needs a profiles.yml for full project load"]
fn test_model_directories_flags_stg_model_in_marts() -> Result<(), Box<dyn Error>> {
    let toml_override = r#"
select = ["model_directories"]
"#;
    let temp = setup_jaffle_shop_fixture(Some(toml_override))?;
    let base = temp.path().join("tests/jaffle_shop/models");

    fs::rename(
        base.join("staging/stg_customers.sql"),
        base.join("marts/stg_customers.sql"),
    )?;
    fs::rename(
        base.join("staging/stg_customers.yml"),
        base.join("marts/stg_customers.yml"),
    )?;

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"));
    cmd.arg("parse")
        .arg("--project-dir")
        .arg(temp.path().join("tests/jaffle_shop"));
    cmd.assert().failure();

    Ok(())
}

// Verifies that passing `parse` explicitly produces the same exit code as omitting it.
// The shim strips the legacy `parse` positional arg before forwarding to dbt-fusion.
#[test]
#[ignore = "needs a profiles.yml for full project load"]
fn test_explicit_parse_arg_compat() -> Result<(), Box<dyn Error>> {
    let temp = setup_jaffle_shop_fixture(None)?;
    let project_dir = temp.path().join("tests/jaffle_shop");

    let mut without_parse = assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"));
    without_parse.arg("--project-dir").arg(&project_dir);

    let mut with_parse = assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"));
    with_parse
        .arg("parse")
        .arg("--project-dir")
        .arg(&project_dir);

    let code_without = without_parse.output()?.status.code();
    let code_with = with_parse.output()?.status.code();

    assert_eq!(
        code_without, code_with,
        "exit code should be the same whether 'parse' is passed explicitly or omitted"
    );
    Ok(())
}

// This test verifies that when a properties file is missing,
// we're able to get upstream column descriptions through osmosis
#[test]
#[ignore = "Codegen won't work until compile is SA'd."]
fn test_missing_properties_file_populates_column_descriptions_from_osmosis()
-> Result<(), Box<dyn Error>> {
    let toml_override = r#"
    select = ["missing_properties_file", "missing_column_descriptions"]
    "#;
    let temp = setup_jaffle_shop_fixture(Some(toml_override))?;

    let locations_yml = temp
        .path()
        .join("tests/jaffle_shop/models/marts/locations.yml");
    assert!(
        !locations_yml.exists(),
        "fixture unexpectedly contains locations.yml"
    );

    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!(env!("CARGO_PKG_NAME"));
    cmd.arg("parse")
        .arg("--fix")
        .arg("--project-dir")
        .arg(temp.path().join("tests/jaffle_shop"));
    let output = cmd.output()?;

    if !locations_yml.exists() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "locations.yml was not created by writeback\n--- child stdout ---\n{}\n--- child stderr ---\n{}\n",
            stdout, stderr
        );
    }

    let contents = fs::read_to_string(&locations_yml)?;
    let properties: PropertyFile = dbt_serde_yaml::from_str(&contents)?;

    let models = properties.models.expect("no models found in locations.yml");
    assert!(!models.is_empty(), "models list in locations.yml is empty");
    let model = &models[0];

    // The actual bug, is that the model has no columns because the ManifestModel
    // wasn't updated after osmosis ran.
    let location_col = model
        .columns
        .iter()
        .find(|c| c.name.as_str() == "location_id")
        .expect("location_id column not found in generated properties file");
    if location_col.description.is_none() {
        panic!(
            "missing description for location_id\n--- produced file contents ---\n{}\n",
            contents
        );
    }
    assert_eq!(
        location_col.description.as_ref().unwrap(),
        "The unique key for each location.", // from stg_locations.location_id
        "location_id description mismatch"
    );

    Ok(())
}

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
    cmd.arg("parse").arg("--fix").current_dir(temp.path());
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

use dbt_lint_yaml::config::Config;

fn main() {
    let project_dir = std::env::current_dir().expect("Failed to get current directory");
    let config = Config::default();
    let config_path = project_dir.join("default-dbt-lint.toml");
    if config_path.exists() {
        eprintln!(
            "Config file already exists at {}",
            config_path.to_string_lossy()
        );
    } else {
        config
            .write_to_file(&config_path)
            .expect("Failed to write default config file");
        println!(
            "Wrote default config file to {}",
            config_path.to_string_lossy()
        );
    }
}

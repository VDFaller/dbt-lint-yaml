use crate::change_descriptors::ColumnChange;
use crate::config::Config;
use dbt_schemas::schemas::dbt_column::DbtColumnRef;
use strum::AsRefStr;

#[derive(Debug, Clone, Default)]
pub struct ColumnResult {
    pub column_name: String,
    pub failures: Vec<ColumnFailure>,
    pub changes: Vec<ColumnChange>,
}

impl ColumnResult {
    pub fn is_pass(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn is_failure(&self) -> bool {
        !self.is_pass()
    }

    pub fn changes(&self) -> &[ColumnChange] {
        &self.changes
    }

    pub fn failure_reasons(&self) -> Vec<String> {
        self.failures
            .iter()
            .map(|failure| match failure {
                ColumnFailure::DescriptionMissing => {
                    format!("Column `{}`: Missing Description", self.column_name)
                }
            })
            .collect()
    }
}

impl std::fmt::Display for ColumnResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_pass() {
            write!(f, "ColumnResult: Pass:{}", self.column_name)
        } else {
            writeln!(f, "ColumnResult: Fail:{}", self.column_name)?;
            for reason in self.failure_reasons() {
                writeln!(f, "    {reason}")?;
            }
            Ok(())
        }
    }
}

// Column behavior and writeback wrapper are now centralized in
// `crate::writeback::changes::ColumnChange` and `ExecutableColumnChange`.

#[derive(Debug, Clone, Copy, AsRefStr, PartialEq, Eq)]
pub enum ColumnFailure {
    DescriptionMissing,
}

impl std::fmt::Display for ColumnFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[allow(clippy::match_single_binding)] // to allow future expansion
        let extra_info = match self {
            _ => String::new(),
        };
        write!(f, "{}{}", self.as_ref(), extra_info)
    }
}

/// Check if a column is missing a description.
/// A description is considered missing if it is:
/// - None
/// - An empty string (after trimming)
/// - Matches any of the configured invalid descriptions (case-insensitive, after trimming)
pub fn missing_description(column: &DbtColumnRef, config: &Config) -> Option<ColumnFailure> {
    let is_missing = match column.description.as_ref() {
        None => true,
        Some(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                true
            } else {
                // case-insensitive comparison against configured invalid descriptions
                config
                    .invalid_descriptions
                    .iter()
                    .any(|bad| bad.eq_ignore_ascii_case(trimmed))
            }
        }
    };

    is_missing.then_some(ColumnFailure::DescriptionMissing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use dbt_schemas::schemas::dbt_column::DbtColumn;
    use std::sync::Arc;

    #[test]
    fn test_missing_description_invalid_markers() {
        let col_tbd = Arc::new(DbtColumn {
            name: "id".to_string(),
            description: Some("TBD".to_string()),
            ..Default::default()
        });

        let config = Config::default();
        assert!(missing_description(&col_tbd, &config).is_some());

        let col_fill = Arc::new(DbtColumn {
            name: "id".to_string(),
            description: Some("  fill me out  ".to_string()),
            ..Default::default()
        });
        // default invalid_descriptions contains "FILL ME OUT", trimmed and case-insensitive
        assert!(missing_description(&col_fill, &config).is_some());

        let col_ok = Arc::new(DbtColumn {
            name: "id".to_string(),
            description: Some("A proper description".to_string()),
            ..Default::default()
        });
        assert!(missing_description(&col_ok, &config).is_none());
    }
}

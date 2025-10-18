use crate::change_descriptors::ColumnChange;
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

pub fn missing_description(column: &DbtColumnRef) -> Option<ColumnFailure> {
    column
        .description
        .is_none()
        .then_some(ColumnFailure::DescriptionMissing)
}

use dbt_schemas::schemas::manifest::DbtManifestV12;
use strum::AsRefStr;

#[derive(Debug, Clone, Default)]
pub struct DocResult {
    pub doc_name: String,
    pub failures: Vec<DocFailure>,
    pub changes: Vec<DocChange>,
}

impl DocResult {
    pub fn is_pass(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn is_failure(&self) -> bool {
        !self.is_pass()
    }

    pub fn changes(&self) -> &[DocChange] {
        &self.changes
    }

    pub fn failure_reasons(&self) -> Vec<String> {
        self.failures.iter().map(ToString::to_string).collect()
    }
}

impl std::fmt::Display for DocResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_pass() {
            write!(f, "DocResult: Pass:{}", self.doc_name)
        } else {
            writeln!(f, "DocResult: Fail:{}", self.doc_name)?;
            for reason in self.failure_reasons() {
                writeln!(f, "    {reason}")?;
            }
            Ok(())
        }
    }
}

#[derive(Debug, Clone, AsRefStr, PartialEq, Eq)]
pub enum DocFailure{
	DuplicateDocsBlock(Vec<String>),
}

impl std::fmt::Display for DocFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let extra_info = match self {
            DocFailure::DuplicateDocsBlock(dupes) => format!(" ({})", dupes.join(",")),
            _ => String::new(),
        };
        write!(f, "{}{}", self.as_ref(), extra_info)
    }
}


#[derive(Debug, Clone)]
pub enum DocChange {}


pub fn duplicate_docs(manifest: &DbtManifestV12) -> Option<Vec<DocFailure>> {
    let mut desc_to_ids: std::collections::HashMap<&str, Vec<String>> = std::collections::HashMap::new();
    for doc in manifest.docs.values() {
        let desc = doc.block_contents.as_str();
        desc_to_ids.entry(desc).or_default().push(doc.unique_id.clone());
    }
    let mut failures = Vec::new();
    for (_desc, ids) in desc_to_ids {
        if ids.len() > 1 {
            failures.push(DocFailure::DuplicateDocsBlock(ids));
        }
    }
    (!failures.is_empty()).then_some(failures)
}
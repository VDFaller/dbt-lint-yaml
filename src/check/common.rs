use strum::AsRefStr;

pub trait Failure {
	fn extra_info(&self) -> Option<String>;
}

impl<T> std::fmt::Display for T
where
    T: Failure,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        if let Some(extra) = self.extra_info() {
            write!(f, "{}{extra}", self.as_ref())
        } else {
            f.write_str(self.as_ref())
        }
    }
}
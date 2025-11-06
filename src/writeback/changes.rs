//! This module will house effect-oriented writeback planning in a follow-up change.
//!
//! Previously it contained filesystem helpers that now live in `writeback::rust`; keeping the
//! module allows us to migrate callers gradually without breaking public structure.

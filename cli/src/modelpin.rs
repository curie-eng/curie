//! Is the configured model a pinned snapshot, or a name that moves?
//!
//! Providers publish two kinds of model id. A **dated snapshot** ends in a
//! calendar date (`claude-haiku-4-5-20251001`, `gpt-4o-2024-08-06`) and always
//! serves the same weights. A **floating alias** omits the date
//! (`claude-sonnet-5`, `gpt-4o`) and is repointed at a newer snapshot whenever
//! the provider ships one. Both are valid to send; only one is reproducible.
//!
//! An alias is therefore an unversioned input to a versioned artifact. The
//! bundle is immutable and promotes across the ladder unchanged, and the eval
//! suite that approved it graded one set of weights -- but an alias can serve
//! different weights the next morning, with no deploy, no diff, and nothing in
//! any log to say so. That is the one behaviour change no gate in this repo can
//! see, which is why it is worth naming at the only moment a human is looking:
//! `curie doctor`.
//!
//! **Shape, not a catalog.** The rule here is purely the id's own form, so it
//! needs no list of known models, no provider credential, and no network call.
//! That is a deliberate limit: it can say "this name moves", and it cannot say
//! "a newer snapshot exists", because knowing that needs a list of what the
//! provider currently publishes. A curated list is the follow-up; asserting a
//! model id nobody fetched would be inventing data, and a stale hand-written
//! catalog is worse than no catalog.
//!
//! Classification is a pure function of the id, so every case below is a unit
//! test rather than a fixture.

/// The trailing calendar date in a model id, normalised to `YYYYMMDD`.
///
/// Two spellings are in use and both must be recognised, which is the kind of
/// thing worth pinning in a test rather than assuming:
///
/// - compact, `...-YYYYMMDD` (`claude-haiku-4-5-20251001`);
/// - hyphenated, `...-YYYY-MM-DD` (`gpt-4o-2024-08-06`).
///
/// Structural rather than a regex over known families, so a family this rule
/// has never seen classifies correctly the day it ships. The month and day are
/// range-checked so an ordinary numeric version suffix cannot read as a date,
/// and at least one non-date segment is required so a bare date is not a model
/// id.
fn dated_suffix(id: &str) -> Option<String> {
    fn digits(s: &str, len: usize) -> bool {
        s.len() == len && s.bytes().all(|b| b.is_ascii_digit())
    }
    fn calendar(year: &str, month: &str, day: &str) -> Option<String> {
        let m: u32 = month.parse().ok()?;
        let d: u32 = day.parse().ok()?;
        ((1..=12).contains(&m) && (1..=31).contains(&d)).then(|| format!("{year}{month}{day}"))
    }

    let parts: Vec<&str> = id.split('-').collect();
    let n = parts.len();

    // Compact: one trailing eight-digit segment, preceded by a name.
    if n >= 2 {
        let last = parts[n - 1];
        if digits(last, 8) {
            if let Some(date) = calendar(&last[0..4], &last[4..6], &last[6..8]) {
                return Some(date);
            }
        }
    }

    // Hyphenated: three trailing segments of 4, 2 and 2 digits, preceded by a
    // name. `n >= 4` is what keeps a bare `2024-08-06` from classifying.
    if n >= 4 {
        let (year, month, day) = (parts[n - 3], parts[n - 2], parts[n - 1]);
        if digits(year, 4) && digits(month, 2) && digits(day, 2) {
            if let Some(date) = calendar(year, month, day) {
                return Some(date);
            }
        }
    }

    None
}

/// What the configured model id is, as far as its own shape can say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinStatus {
    /// Nothing configured. The platform default applies, which is a choice the
    /// operator has not made rather than one they got wrong.
    Unset,
    /// A dated snapshot: reproducible, and what an eval result can be trusted
    /// against.
    Pinned {
        /// The full model id.
        id: String,
        /// Its `YYYYMMDD` suffix, for the detail line.
        date: String,
    },
    /// A floating alias: valid, but the weights behind it can change without
    /// any change here.
    Floating {
        /// The full model id.
        id: String,
    },
}

/// Classify the configured model id by its shape.
///
/// `None` and an all-whitespace value are both `Unset`: an exported-but-empty
/// variable is the #229 footgun, and reading it as a configured model would
/// report a pin that does not exist.
pub fn classify(model: Option<&str>) -> PinStatus {
    let Some(id) = model.map(str::trim).filter(|id| !id.is_empty()) else {
        return PinStatus::Unset;
    };
    match dated_suffix(id) {
        Some(date) => PinStatus::Pinned {
            id: id.to_string(),
            date,
        },
        None => PinStatus::Floating { id: id.to_string() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dated_suffix_is_a_pin() {
        assert_eq!(
            classify(Some("claude-haiku-4-5-20251001")),
            PinStatus::Pinned {
                id: "claude-haiku-4-5-20251001".into(),
                date: "20251001".into(),
            }
        );
        // The other provider hyphenates the date, so the trailing segment is
        // "06" rather than the whole date. Both spellings are load bearing and
        // this assertion is what caught the first rule for only handling one.
        assert_eq!(
            classify(Some("gpt-4o-2024-08-06")),
            PinStatus::Pinned {
                id: "gpt-4o-2024-08-06".into(),
                date: "20240806".into(),
            }
        );
    }

    #[test]
    fn an_undated_name_floats() {
        // The case that motivated this check: the name stayed and the weights
        // behind it changed.
        for id in ["gpt-4o", "claude-sonnet-5", "claude-opus-5", "qwen3:4b"] {
            assert_eq!(
                classify(Some(id)),
                PinStatus::Floating { id: id.into() },
                "{id} should read as floating"
            );
        }
    }

    #[test]
    fn unset_and_empty_are_both_unset() {
        assert_eq!(classify(None), PinStatus::Unset);
        assert_eq!(classify(Some("")), PinStatus::Unset);
        assert_eq!(classify(Some("   ")), PinStatus::Unset);
    }

    #[test]
    fn surrounding_whitespace_does_not_change_the_verdict() {
        assert_eq!(
            classify(Some("  claude-haiku-4-5-20251001  ")),
            PinStatus::Pinned {
                id: "claude-haiku-4-5-20251001".into(),
                date: "20251001".into(),
            }
        );
    }

    /// The eight-digit range check is what keeps an ordinary build or version
    /// suffix from being reported as a pinned date.
    #[test]
    fn an_eight_digit_suffix_that_is_not_a_date_still_floats() {
        for id in [
            "some-model-99999999",   // month 99
            "some-model-20251301",   // month 13
            "some-model-20250100",   // day 00
            "some-model-20250132",   // day 32
            "some-model-2025-13-01", // hyphenated, month 13
            "some-model-2025-01-32", // hyphenated, day 32
            "2024-08-06",            // a bare date is not a model id
        ] {
            assert_eq!(
                classify(Some(id)),
                PinStatus::Floating { id: id.into() },
                "{id} should not read as a date"
            );
        }
    }

    #[test]
    fn a_short_or_non_numeric_suffix_floats() {
        for id in ["model-2025", "model-latest", "model-v20251001x", "model-"] {
            assert_eq!(
                classify(Some(id)),
                PinStatus::Floating { id: id.into() },
                "{id} should read as floating"
            );
        }
    }

    /// An id with no separator at all must not panic on the `rsplit_once`.
    #[test]
    fn an_id_with_no_dash_floats() {
        assert_eq!(
            classify(Some("20251001")),
            PinStatus::Floating {
                id: "20251001".into()
            }
        );
    }
}

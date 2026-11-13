use super::*;

#[derive(Args, Debug, Clone)]
pub(crate) struct CodingInstructionsExpectedRevision {
    /// Revision returned by get. Use 0 when no document has been created.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) expected_revision: i64,
}

#[derive(Subcommand, Debug, Clone)]
pub(crate) enum ClusterCodingInstructionsAction {
    /// Read your coding instructions and current revision. Application to
    /// factory runs is unavailable.
    Get {
        #[command(flatten)]
        session: SubscriptionSession,
    },
    /// Replace your coding instructions from a UTF-8 file, preserving its
    /// whitespace. An empty file clears the document. The content reaches the
    /// model provider and must not contain credentials. Application to factory
    /// runs is unavailable.
    Set {
        #[command(flatten)]
        session: SubscriptionSession,
        /// UTF-8 instruction file. Its bytes are sent unchanged to the API.
        #[arg(long, value_name = "PATH")]
        file: PathBuf,
        #[command(flatten)]
        expected: CodingInstructionsExpectedRevision,
    },
    /// Clear your document while preserving its revision history.
    Clear {
        #[command(flatten)]
        session: SubscriptionSession,
        #[command(flatten)]
        expected: CodingInstructionsExpectedRevision,
    },
}

impl ClusterCodingInstructionsAction {
    pub(crate) fn session(&self) -> &SubscriptionSession {
        match self {
            Self::Get { session } | Self::Set { session, .. } | Self::Clear { session, .. } => {
                session
            }
        }
    }

    pub(crate) fn session_mut(&mut self) -> &mut SubscriptionSession {
        match self {
            Self::Get { session } | Self::Set { session, .. } | Self::Clear { session, .. } => {
                session
            }
        }
    }
}

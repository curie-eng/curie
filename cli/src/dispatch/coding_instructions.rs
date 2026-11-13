use super::*;

pub(super) async fn run(action: ClusterCodingInstructionsAction) -> Result<()> {
    use commands::CodingInstructionsVerb as Verb;

    let (verb, session) = match action {
        ClusterCodingInstructionsAction::Get { session } => (Verb::Get, session),
        ClusterCodingInstructionsAction::Set {
            session,
            file,
            expected,
        } => (
            Verb::Set {
                file,
                expected_revision: expected.expected_revision,
            },
            session,
        ),
        ClusterCodingInstructionsAction::Clear { session, expected } => (
            Verb::Clear {
                expected_revision: expected.expected_revision,
            },
            session,
        ),
    };
    emit(
        commands::coding_instructions(commands::CodingInstructionsOpts {
            verb,
            session_file: session.session_file,
            api_url: session.api_url,
            namespace: session.namespace,
            release: session.release,
            dry_run: session.dry_run,
        })
        .await?,
    )
}

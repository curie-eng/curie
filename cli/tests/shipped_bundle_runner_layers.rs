//! The shipped bundles that install stdio MCP servers declare their runner
//! layer, so `curie build` builds it and a cluster deploy binds it (#3420).
//! Without the declaration their agents run the bare platform runner, which
//! carries neither `mcp-server-github` nor `slack-mcp`.

use std::path::Path;

use curie::connector_build::{check_runner_source, load};

#[test]
fn shipped_bundles_declare_their_runner_layer() {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples");
    for name in ["dark-factory", "github-issues", "mean-tester"] {
        let dir = examples.join(name);
        let decl = load(&dir).unwrap_or_else(|err| panic!("{name}: {err:#}"));
        let runner = decl
            .runner
            .as_ref()
            .unwrap_or_else(|| panic!("{name}: connectors.yaml declares no runner layer"));
        assert_eq!(runner.build.dockerfile, "runner.Dockerfile", "{name}");
        check_runner_source(&dir, runner).unwrap_or_else(|err| panic!("{name}: {err:#}"));
    }
}

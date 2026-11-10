//! `curie skill eval-init`: a guided interview that writes `evals/cases.json`.
//!
//! Issue #260 (eval-experience epic #26): turn an author's intent into a runnable
//! starter suite by *interviewing* them -- one case at a time, ask for the prompt,
//! the grader kind, and the expected answer -- then emit a
//! [`crate::evals::EvalSuite`] in the frozen canonical eval-case shape. Because it
//! serializes the same `EvalSuite`/`EvalCase`/`Grader` types `curie skill eval`
//! loads (not a hand-mirrored JSON writer), the generated file is byte-compatible
//! with the frozen schema and re-loadable without a shape change -- and any drift
//! in that schema breaks this command at compile time, not at author time.
//!
//! This is the *interactive* sibling of `curie init --from-spec` (ADR-0021
//! decision 5), which is deliberately zero-prompt because a coding agent does the
//! interviewing there. Here a human is at the keyboard, so the interview refuses
//! to run without a TTY rather than block forever on a read a piped/agent stdin
//! can never answer -- an agent should assemble a spec's `evals` array instead.
//!
//! The interview loop and the file I/O are split so the loop is unit-testable off
//! canned `BufRead`/`Write` without a terminal.

use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::evals::{validate_suite, EvalCase, EvalSuite, ExpectedStatus, Grader, GraderKind};

/// Where the interview writes and whether it may overwrite an existing suite.
pub struct EvalInitOpts {
    /// Output path. Defaults to `evals/cases.json` under the plugin dir.
    pub out: PathBuf,
    /// Overwrite an existing suite file instead of refusing.
    pub force: bool,
}

/// Run the guided interview and write the assembled suite.
///
/// Refuses in a non-interactive session (piped/agent stdin), and refuses to
/// clobber an existing suite unless `force`.
pub fn run(opts: EvalInitOpts) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        return Err(crate::exit::CliError::usage(
            "eval-init is an interactive interview and needs a terminal; \
             a non-interactive author should assemble the spec's `evals` array \
             and run `curie init --from-spec` instead",
        )
        .with_fix("run it in an interactive shell, or use `curie init --from-spec`")
        .into());
    }

    if opts.out.exists() && !opts.force {
        return Err(crate::exit::CliError::usage(format!(
            "{} already exists; refusing to overwrite an existing eval suite",
            opts.out.display()
        ))
        .with_fix("pass --force to overwrite, or choose another --out path")
        .into());
    }

    let default_name = suite_name_hint(&opts.out);
    let stdin = std::io::stdin();
    let stderr = std::io::stderr();
    let suite = interview(&mut stdin.lock(), &mut stderr.lock(), &default_name)?;

    write_suite(&opts.out, &suite)?;

    let ui = crate::ui::ui();
    ui.success(&format!(
        "wrote {} case(s) to {}",
        suite.cases.len(),
        opts.out.display()
    ));
    ui.note("run `curie skill eval` to execute the suite against a local runner");
    Ok(())
}

/// A reasonable default suite name from the output path's grandparent dir (the
/// plugin dir), falling back to `default` -- e.g. `.../my-agent/evals/cases.json`
/// suggests `my-agent`.
fn suite_name_hint(out: &Path) -> String {
    out.parent()
        .and_then(Path::parent)
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .filter(|n| !n.is_empty() && *n != ".")
        .unwrap_or("default")
        .to_string()
}

/// The pure interview loop: prompt on `w`, read answers from `r`, return an
/// assembled + validated [`EvalSuite`]. Generic over the streams so a test can
/// drive it with canned input and no terminal.
pub fn interview<R: BufRead, W: Write>(
    r: &mut R,
    w: &mut W,
    default_name: &str,
) -> Result<EvalSuite> {
    writeln!(
        w,
        "Guided eval generation. Answer a few questions per case; \
         Ctrl-C to abort.\n"
    )?;

    let name = ask_default(
        r,
        w,
        &format!("Suite name [{default_name}]: "),
        default_name,
    )?;

    let mut cases: Vec<EvalCase> = Vec::new();
    loop {
        let n = cases.len() + 1;
        writeln!(w, "\n--- case {n} ---")?;

        let default_id = format!("case-{n}");
        let id = ask_default(r, w, &format!("  Case id [{default_id}]: "), &default_id)?;
        let input = ask_required(r, w, "  Prompt sent to the agent: ")?;
        let grader = ask_grader(r, w)?;

        // Scaffolded cases run isolated (`shared_history: false`) and assert the
        // default terminal status (`done`); a shared-history chain or a
        // gate-blocked assertion is authored by hand afterwards.
        cases.push(EvalCase {
            id,
            input,
            grader,
            shared_history: false,
            sender: None,
            expect_status: ExpectedStatus::default(),
        });

        if !ask_yes_no(r, w, "\nAdd another case? [Y/n]: ", true)? {
            break;
        }
    }

    // Hold the interview's output to the exact contract `curie skill eval`
    // enforces (non-empty, every regex grader compiles) before we claim success.
    validate_suite(&name, &cases)?;
    Ok(EvalSuite { name, cases })
}

/// Ask for a grader: kind (exact/contains/regex/tool_called), the expected
/// string, and case-sensitivity. Re-prompts on an unrecognized kind rather than
/// guessing.
fn ask_grader<R: BufRead, W: Write>(r: &mut R, w: &mut W) -> Result<Grader> {
    let kind = loop {
        let raw = ask_default(
            r,
            w,
            "  Grader kind (exact/contains/regex/tool_called) [contains]: ",
            "contains",
        )?;
        match raw.to_lowercase().as_str() {
            "exact" => break GraderKind::Exact,
            "contains" => break GraderKind::Contains,
            "regex" => break GraderKind::Regex,
            "tool_called" => break GraderKind::ToolCalled,
            other => writeln!(
                w,
                "  '{other}' is not a grader kind; choose exact, contains, regex, or tool_called."
            )?,
        }
    };

    let expected = match kind {
        GraderKind::Regex => ask_required(r, w, "  Expected (regex pattern to match): ")?,
        GraderKind::Exact => ask_required(r, w, "  Expected (exact answer, trimmed): ")?,
        GraderKind::Contains => {
            ask_required(r, w, "  Expected (substring the answer must contain): ")?
        }
        // tool_called grades the tool-call trajectory, so `expected` is the name
        // of the tool that must have been invoked during the turn.
        GraderKind::ToolCalled => {
            ask_required(r, w, "  Expected (name of the tool that must be called): ")?
        }
    };
    let case_sensitive = ask_yes_no(r, w, "  Case-sensitive? [y/N]: ", false)?;

    Ok(Grader {
        kind,
        expected,
        case_sensitive,
    })
}

/// Prompt and read a line; empty input yields `default`.
fn ask_default<R: BufRead, W: Write>(
    r: &mut R,
    w: &mut W,
    prompt: &str,
    default: &str,
) -> Result<String> {
    let line = prompt_line(r, w, prompt)?;
    let trimmed = line.trim();
    Ok(if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.to_string()
    })
}

/// Prompt and read a non-empty line, re-prompting until the author gives one.
fn ask_required<R: BufRead, W: Write>(r: &mut R, w: &mut W, prompt: &str) -> Result<String> {
    loop {
        let line = prompt_line(r, w, prompt)?;
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
        writeln!(w, "  (required -- please enter a value)")?;
    }
}

/// Prompt a y/n question with a default applied to empty input.
fn ask_yes_no<R: BufRead, W: Write>(
    r: &mut R,
    w: &mut W,
    prompt: &str,
    default: bool,
) -> Result<bool> {
    let line = prompt_line(r, w, prompt)?;
    Ok(match line.trim().to_lowercase().as_str() {
        "" => default,
        "y" | "yes" => true,
        "n" | "no" => false,
        _ => default,
    })
}

/// Write the prompt, flush, and read one line. An EOF (`read_line` -> 0) before
/// the interview finishes is a truncated session, surfaced as an error rather
/// than silently treated as an empty answer that would loop forever.
fn prompt_line<R: BufRead, W: Write>(r: &mut R, w: &mut W, prompt: &str) -> Result<String> {
    write!(w, "{prompt}")?;
    w.flush()?;
    let mut line = String::new();
    let read = r.read_line(&mut line).context("reading interview answer")?;
    if read == 0 {
        return Err(
            crate::exit::CliError::usage("input ended before the interview finished").into(),
        );
    }
    Ok(line)
}

/// Serialize the suite (pretty JSON, trailing newline) to `out`, creating the
/// parent `evals/` dir if needed.
fn write_suite(out: &Path, suite: &EvalSuite) -> Result<()> {
    if let Some(parent) = out.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
    }
    let mut json = serde_json::to_string_pretty(suite).context("serializing eval suite")?;
    json.push('\n');
    std::fs::write(out, json).with_context(|| format!("writing {}", out.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn drive(name: &str, script: &str) -> EvalSuite {
        let mut input = Cursor::new(script.as_bytes().to_vec());
        let mut out: Vec<u8> = Vec::new();
        interview(&mut input, &mut out, name).expect("interview should succeed")
    }

    #[test]
    fn assembles_a_multi_case_suite_in_the_frozen_shape() {
        // suite name (default), case1 (default id, prompt, contains grader,
        // not case-sensitive), add another -> yes, case2 (explicit), stop.
        let script = "\n\
                      \n\
                      What is 2+2?\n\
                      contains\n\
                      4\n\
                      \n\
                      y\n\
                      greeting\n\
                      Say hello\n\
                      exact\n\
                      hello world\n\
                      y\n\
                      n\n";
        let suite = drive("my-agent", script);

        assert_eq!(suite.name, "my-agent");
        assert_eq!(suite.cases.len(), 2);

        assert_eq!(suite.cases[0].id, "case-1"); // default id
        assert_eq!(suite.cases[0].input, "What is 2+2?");
        assert_eq!(suite.cases[0].grader.kind, GraderKind::Contains);
        assert_eq!(suite.cases[0].grader.expected, "4");
        assert!(!suite.cases[0].grader.case_sensitive);

        assert_eq!(suite.cases[1].id, "greeting");
        assert_eq!(suite.cases[1].grader.kind, GraderKind::Exact);
        assert!(suite.cases[1].grader.case_sensitive); // answered "y"

        // Round-trips: the emitted JSON reloads as the same frozen types.
        let json = serde_json::to_string(&suite).unwrap();
        let reloaded: EvalSuite = serde_json::from_str(&json).unwrap();
        assert_eq!(reloaded.cases.len(), 2);
    }

    #[test]
    fn reprompts_on_unknown_grader_kind() {
        // "fuzzy" is not a kind -> re-ask; then "regex".
        let script = "suite-x\n\
                      c1\n\
                      match a digit\n\
                      fuzzy\n\
                      regex\n\
                      \\d+\n\
                      n\n\
                      n\n";
        let suite = drive("fallback", script);
        assert_eq!(suite.name, "suite-x");
        assert_eq!(suite.cases[0].grader.kind, GraderKind::Regex);
        assert_eq!(suite.cases[0].grader.expected, "\\d+");
    }

    #[test]
    fn rejects_an_uncompilable_regex_grader() {
        // validate_suite compiles regex graders; an unbalanced group must fail
        // loudly at the end of the interview, not write a broken suite.
        let script = "suite-y\n\
                      c1\n\
                      prompt\n\
                      regex\n\
                      (unterminated\n\
                      n\n\
                      n\n";
        let mut input = Cursor::new(script.as_bytes().to_vec());
        let mut out: Vec<u8> = Vec::new();
        let err = interview(&mut input, &mut out, "n").unwrap_err();
        assert!(err.to_string().contains("invalid regex"));
    }

    #[test]
    fn suite_name_hint_uses_the_plugin_dir() {
        let hint = suite_name_hint(Path::new("/x/my-agent/evals/cases.json"));
        assert_eq!(hint, "my-agent");
        assert_eq!(suite_name_hint(Path::new("cases.json")), "default");
    }
}

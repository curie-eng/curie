//! Shared terminal-output core: color/unicode resolution, styled message
//! emission, an indicatif-backed step checklist, and a borderless table.
//!
//! The design principle is "dim plumbing, bright payload": diagnostics go to
//! stderr in muted colors, the thing the user asked for goes to stdout in bold
//! white. State is never encoded in color alone; every colored status carries a
//! glyph and a word. Commands reach a process-global `Ui` via `ui::ui()` so the
//! resolved flags do not have to thread through every options struct.

use std::io::{IsTerminal, Write};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anstyle::{AnsiColor, Color, RgbColor, Style};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

// ---------------------------------------------------------------------------
// Palette
// ---------------------------------------------------------------------------

fn rgb(r: u8, g: u8, b: u8) -> Color {
    Color::Rgb(RgbColor(r, g, b))
}

/// A palette role, defined by its 24-bit brand color and the nearest named
/// ANSI color to degrade to when the terminal does not advertise truecolor.
/// Resolved through `Ui::role`, which reads the per-`Ui` `truecolor` depth.
///
/// - success `#2EA043` -> `Green`
/// - error `#CF222E` -> `Red`
/// - amber `#BF8700` -> `Yellow`
/// - dim `#8B949E` -> `BrightBlack`
/// - cyan (urls) `#39C5CF` -> `Cyan`
/// - payload `#E6EDF3` (bold) -> `White` (bold)
fn role(truecolor: bool, r: u8, g: u8, b: u8, ansi: AnsiColor) -> Style {
    let color = if truecolor {
        rgb(r, g, b)
    } else {
        Color::Ansi(ansi)
    };
    Style::new().fg_color(Some(color))
}

/// Column at which a step's elapsed time is right-aligned.
const STEP_LABEL_WIDTH: usize = 44;

// ---------------------------------------------------------------------------
// Config resolution (pure)
// ---------------------------------------------------------------------------

/// The `--color` flag: honor the terminal, force on, or force off.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ColorFlag {
    /// Colorize only when stderr is a terminal (respecting NO_COLOR etc.).
    #[default]
    Auto,
    /// Always emit ANSI styling.
    Always,
    /// Never emit ANSI styling.
    Never,
}

/// A snapshot of the process environment, passed to `resolve` so color/unicode
/// resolution is a pure, testable function.
pub struct UiEnv {
    /// NO_COLOR present and non-empty (per no-color.org).
    pub no_color: bool,
    /// CLICOLOR=0.
    pub clicolor_zero: bool,
    /// CLICOLOR_FORCE present and != "0".
    pub clicolor_force: bool,
    /// TERM=dumb.
    pub term_dumb: bool,
    /// CI present.
    pub ci: bool,
    /// stderr is a terminal.
    pub stderr_tty: bool,
    /// stdout is a terminal.
    pub stdout_tty: bool,
    /// The locale advertises UTF-8.
    pub utf8: bool,
    /// The terminal advertises 24-bit truecolor (COLORTERM / terminfo).
    pub truecolor: bool,
}

/// The resolved output configuration every command reads.
#[derive(Copy, Clone, Debug)]
pub struct Ui {
    color_stdout: bool,
    color_stderr: bool,
    unicode: bool,
    truecolor: bool,
    debug: bool,
    quiet: bool,
    interactive: bool,
    json: bool,
}

/// Resolve whether a given stream should embed ANSI, honoring the `--color`
/// flag first, then the environment overrides, then the stream's own tty state.
fn want_color(flag: ColorFlag, env: &UiEnv, stream_tty: bool) -> bool {
    match flag {
        ColorFlag::Never => false,
        ColorFlag::Always => true,
        ColorFlag::Auto => {
            if env.clicolor_force {
                true
            } else if env.no_color || env.clicolor_zero || env.term_dumb {
                false
            } else {
                stream_tty
            }
        }
    }
}

impl Ui {
    /// Resolve output config from the flags and an environment snapshot.
    ///
    /// Color precedence: `--color=never` forces off and `--color=always`
    /// forces on (explicit intent wins over everything). Under `Auto`,
    /// CLICOLOR_FORCE forces on, then NO_COLOR / CLICOLOR=0 / TERM=dumb force
    /// off, otherwise it follows whether that stream is a terminal. Color is
    /// resolved per stream: stdout (payload) and stderr (diagnostics) each
    /// follow their own tty state, so redirecting one never taints the other.
    /// Interactivity (spinners and redraws) is independent of color: on only
    /// for a non-CI, non-dumb terminal. Unicode mirrors the locale. Under
    /// `--json` the run is never interactive: a spinner on stderr is fine, but
    /// the machine payload owns stdout and must not share it with progress redraws.
    pub fn resolve(color: ColorFlag, debug: bool, quiet: bool, json: bool, env: &UiEnv) -> Ui {
        let color_stdout = want_color(color, env, env.stdout_tty);
        let color_stderr = want_color(color, env, env.stderr_tty);
        let interactive = env.stderr_tty && !env.ci && !env.term_dumb && !json;
        Ui {
            color_stdout,
            color_stderr,
            unicode: env.utf8,
            truecolor: env.truecolor,
            debug,
            quiet,
            interactive,
            json,
        }
    }

    /// Snapshot the real process environment and resolve. Non-pure wrapper.
    pub fn from_process(color: ColorFlag, debug: bool, quiet: bool, json: bool) -> Ui {
        let env = UiEnv {
            no_color: std::env::var_os("NO_COLOR")
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            clicolor_zero: std::env::var("CLICOLOR").map(|v| v == "0").unwrap_or(false),
            clicolor_force: std::env::var("CLICOLOR_FORCE")
                .map(|v| v != "0")
                .unwrap_or(false),
            term_dumb: std::env::var("TERM").map(|v| v == "dumb").unwrap_or(false),
            ci: std::env::var_os("CI").is_some(),
            stderr_tty: std::io::stderr().is_terminal(),
            stdout_tty: std::io::stdout().is_terminal(),
            utf8: detect_utf8(),
            truecolor: anstyle_query::truecolor(),
        };
        Ui::resolve(color, debug, quiet, json, &env)
    }

    /// Whether `--json` is in effect: the human stdout emitters are suppressed so
    /// they cannot corrupt the single-line machine payload.
    pub fn json(&self) -> bool {
        self.json
    }

    /// Write one compact JSON line (plus a trailing newline) to stdout. No color;
    /// the payload is machine-consumed.
    pub fn emit_json(&self, value: &serde_json::Value) {
        if let Ok(line) = serde_json::to_string(value) {
            let _ = writeln!(anstream::stdout(), "{line}");
        }
    }

    /// The one success-path decision point (mirror of the centralized error emit
    /// in `main.rs`): under `--json` emit the value's single JSON object to
    /// stdout, otherwise render the human view. Handlers hand `emit` a
    /// `CliOutput` and stop deciding how to render, so no verb can silently emit
    /// empty stdout under `--json` (issue #456, ADR-0021).
    pub fn emit(&self, out: &dyn CliOutput) {
        if self.json {
            self.emit_json(&out.to_json());
        } else {
            out.render(self);
        }
    }

    /// Preserve a structured failed report's human view while the centralized
    /// error path emits its single JSON object and semantic exit code. Used by
    /// cluster status, whose unhealthy report still contains useful rows/URLs.
    pub fn failed_report(&self, out: &dyn CliOutput, error: anyhow::Error) -> anyhow::Error {
        if !self.json {
            out.render(self);
        }
        let (message, remedy) = crate::exit::present_error(&error);
        let (_, fix) = crate::exit::classify(&error);
        crate::exit::operator_context(
            crate::exit::with_json_payload(error, out.to_json()),
            message,
            remedy.or(fix),
        )
    }

    // -- styling helpers ---------------------------------------------------

    /// Wrap `s` in `style`'s ANSI codes when stdout color is on, else raw.
    /// Used by every stdout (payload) emitter.
    fn paint_out(&self, style: Style, s: &str) -> String {
        if self.color_stdout {
            format!("{}{}{}", style.render(), s, style.render_reset())
        } else {
            s.to_string()
        }
    }

    /// Wrap `s` in `style`'s ANSI codes when stderr color is on, else raw.
    /// Used by every stderr (diagnostics) emitter, including the indicatif
    /// spinner and frozen step lines that bypass anstream's stripping.
    fn paint_err(&self, style: Style, s: &str) -> String {
        if self.color_stderr {
            format!("{}{}{}", style.render(), s, style.render_reset())
        } else {
            s.to_string()
        }
    }

    // -- palette (depth-aware: 24-bit when truecolor, else nearest ANSI-16) --

    /// Success green, paired with the ok glyph and a word like "done"/"pass".
    fn green(&self) -> Style {
        role(self.truecolor, 0x2E, 0xA0, 0x43, AnsiColor::Green)
    }

    /// Error red, paired with the fail glyph and "failed"/"fail".
    fn red(&self) -> Style {
        role(self.truecolor, 0xCF, 0x22, 0x2E, AnsiColor::Red)
    }

    /// Amber warn, paired with the warn glyph and "warn".
    fn amber(&self) -> Style {
        role(self.truecolor, 0xBF, 0x87, 0x00, AnsiColor::Yellow)
    }

    /// Dim grey for plumbing, details, and elapsed times. Off truecolor it
    /// degrades to the portable faint attribute (SGR 2) with no explicit
    /// foreground rather than ANSI bright-black: faint derives from the
    /// terminal's own default foreground, so it stays readable on both light
    /// and dark backgrounds, and a terminal that ignores SGR 2 simply renders
    /// at normal intensity (still readable) instead of low-contrast dark grey.
    fn dim(&self) -> Style {
        if self.truecolor {
            Style::new().fg_color(Some(rgb(0x8B, 0x94, 0x9E)))
        } else {
            Style::new().dimmed()
        }
    }

    /// Cyan for URLs and resource ids.
    fn cyan(&self) -> Style {
        role(self.truecolor, 0x39, 0xC5, 0xCF, AnsiColor::Cyan)
    }

    /// Bright bold white: the payload the user asked for.
    fn payload_style(&self) -> Style {
        role(self.truecolor, 0xE6, 0xED, 0xF3, AnsiColor::White).bold()
    }

    /// Bright white without bold: the streamed/returned agent answer. Same
    /// foreground as `payload_style` (the brightest thing on screen) but not
    /// bold, since a bold multi-line paragraph reads too heavy; short status
    /// lines keep `payload`'s bold.
    fn answer_style(&self) -> Style {
        role(self.truecolor, 0xE6, 0xED, 0xF3, AnsiColor::White)
    }

    fn ok_glyph(&self) -> &'static str {
        if self.unicode {
            "\u{2713}" // check mark
        } else {
            "v"
        }
    }

    fn fail_glyph(&self) -> &'static str {
        if self.unicode {
            "\u{2717}" // ballot x
        } else {
            "x"
        }
    }

    fn warn_glyph(&self) -> &'static str {
        if self.unicode {
            "\u{26a0}" // warning sign
        } else {
            "!"
        }
    }

    fn bullet_glyph(&self) -> &'static str {
        if self.unicode {
            "\u{00b7}" // middle dot
        } else {
            "-"
        }
    }

    fn prompt_glyph(&self) -> &'static str {
        if self.unicode {
            "\u{276f}" // heavy right angle
        } else {
            ">"
        }
    }

    /// The bold-green prompt marker, ready to embed.
    pub fn prompt(&self) -> String {
        self.paint_out(self.green().bold(), self.prompt_glyph())
    }

    // -- stderr diagnostics (no-op when quiet) -----------------------------

    /// Dim "middot line" plumbing detail. Emitted only under `--debug`.
    pub fn plumbing(&self, line: &str) {
        if self.quiet || !self.debug {
            return;
        }
        let out = self.paint_err(self.dim(), &format!("{} {line}", self.bullet_glyph()));
        let _ = writeln!(anstream::stderr(), "{out}");
    }

    /// A dim informational line on stderr.
    pub fn note(&self, msg: &str) {
        if self.quiet {
            return;
        }
        let _ = writeln!(anstream::stderr(), "{}", self.paint_err(self.dim(), msg));
    }

    /// An amber warning: "warn glyph + warn + message".
    pub fn warn(&self, msg: &str) {
        if self.quiet {
            return;
        }
        let content = format!("{} warn  {msg}", self.warn_glyph());
        let _ = writeln!(
            anstream::stderr(),
            "{}",
            self.paint_err(self.amber(), &content)
        );
    }

    /// A green success line: "ok glyph + message".
    pub fn success(&self, msg: &str) {
        if self.quiet {
            return;
        }
        let line = format!("{} {msg}", self.paint_err(self.green(), self.ok_glyph()));
        let _ = writeln!(anstream::stderr(), "{line}");
    }

    /// A red failure line: "fail glyph + message".
    pub fn failure(&self, msg: &str) {
        if self.quiet {
            return;
        }
        let line = format!("{} {msg}", self.paint_err(self.red(), self.fail_glyph()));
        let _ = writeln!(anstream::stderr(), "{line}");
    }

    // -- stdout payload (always emitted, even under quiet) ------------------

    /// A bright bold-white payload line on stdout.
    pub fn payload(&self, line: &str) {
        if self.json {
            return;
        }
        let _ = writeln!(
            anstream::stdout(),
            "{}",
            self.paint_out(self.payload_style(), line)
        );
    }

    /// An unstyled payload line on stdout (raw data).
    pub fn payload_plain(&self, line: &str) {
        if self.json {
            return;
        }
        let _ = writeln!(anstream::stdout(), "{line}");
    }

    /// Write raw streamed tokens to stdout with no newline, flushing.
    pub fn print_tokens(&self, s: &str) {
        if self.json {
            return;
        }
        let mut out = anstream::stdout();
        let _ = write!(out, "{s}");
        let _ = out.flush();
    }

    /// Write agent answer text to stdout in the bright payload color (non-bold),
    /// no trailing newline, flushed. Used for streamed tokens and returned replies.
    pub fn answer(&self, s: &str) {
        if self.json {
            return;
        }
        let styled = self.paint_out(self.answer_style(), s);
        let mut out = anstream::stdout();
        let _ = write!(out, "{styled}");
        let _ = out.flush();
    }

    /// A key/value line on stdout: dim padded key, then value. The caller
    /// pre-styles the value (e.g. via `url`) when it is a URL or id.
    pub fn kv(&self, key: &str, val: &str) {
        if self.json {
            return;
        }
        let key_styled = self.paint_out(self.dim(), &format!("{key:<12}"));
        let _ = writeln!(anstream::stdout(), "{key_styled}  {val}");
    }

    /// Return `s` styled as a cyan URL/id, for embedding in another line.
    pub fn url(&self, s: &str) -> String {
        self.paint_out(self.cyan(), s)
    }

    // -- checklist / progress ---------------------------------------------

    /// Begin a checklist: a group of steps sharing one drawing area.
    pub fn checklist(&self) -> Checklist {
        Checklist {
            multi: MultiProgress::new(),
            ui: *self,
        }
    }

    /// A minimal determinate progress bar (message/eval use it later).
    pub fn progress_bar(&self, total: u64, label: &str) -> Bar {
        let pb = if self.interactive && !self.quiet {
            let pb = ProgressBar::new(total);
            if let Ok(style) = ProgressStyle::with_template("{msg} [{bar:24}] {pos}/{len}") {
                pb.set_style(style);
            }
            pb.set_message(label.to_string());
            pb
        } else {
            ProgressBar::hidden()
        };
        Bar { pb }
    }

    /// The frozen, styled line a finished interactive step commits.
    fn step_line(&self, status: StepStatus, label: &str, detail: &str, elapsed: &str) -> String {
        let (glyph, style) = match status {
            StepStatus::Ok => (self.ok_glyph(), self.green()),
            StepStatus::Fail => (self.fail_glyph(), self.red()),
            StepStatus::Warn => (self.warn_glyph(), self.amber()),
        };
        let mut left = format!("{} {label}", self.paint_err(style, glyph));
        if !detail.is_empty() {
            left.push_str("   ");
            left.push_str(&self.paint_err(self.dim(), detail));
        }
        let pad = STEP_LABEL_WIDTH.saturating_sub(display_width(&left));
        format!(
            "{left}{}{}",
            " ".repeat(pad + 1),
            self.paint_err(self.dim(), elapsed)
        )
    }
}

// ---------------------------------------------------------------------------
// CliOutput: the success-path emit contract
// ---------------------------------------------------------------------------

/// A command result that knows both how to serialize to one JSON object (for
/// `--json`) and how to render itself for humans. `Ui::emit` picks between them,
/// so the json-vs-human decision lives in exactly one place. Implement this for
/// any verb's output rather than calling a stdout emitter directly.
pub trait CliOutput {
    /// The single JSON object emitted under `--json`.
    fn to_json(&self) -> serde_json::Value;
    /// The human render (stdout payload lines) when not under `--json`.
    fn render(&self, ui: &Ui);
}

/// A generic `--dry-run` plan: the shell command lines a verb WOULD run. Its
/// JSON is `{"dry_run":true,"plan":[lines]}`; its human render is the same lines
/// verbatim (so operator dry-run output is byte-identical to before). The lines
/// come straight from `OpsCommand::display()` (already credential-masked), so
/// this type never re-derives argv or reads a raw secret.
#[derive(Debug)]
pub struct DryRunPlan {
    pub lines: Vec<String>,
}

impl CliOutput for DryRunPlan {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "dry_run": true, "plan": self.lines })
    }

    fn render(&self, ui: &Ui) {
        for line in &self.lines {
            ui.payload_plain(line);
        }
    }
}

/// Detect whether the locale advertises UTF-8. First set of LC_ALL / LC_CTYPE /
/// LANG decides; unset defaults to true off Windows.
fn detect_utf8() -> bool {
    for key in ["LC_ALL", "LC_CTYPE", "LANG"] {
        if let Ok(val) = std::env::var(key) {
            if !val.is_empty() {
                let up = val.to_uppercase();
                return up.contains("UTF-8") || up.contains("UTF8");
            }
        }
    }
    !cfg!(windows)
}

// ---------------------------------------------------------------------------
// Process-global handle
// ---------------------------------------------------------------------------

static UI: OnceLock<Ui> = OnceLock::new();

/// Install the resolved `Ui`. Color embedding is gated per stream by
/// `paint_out`/`paint_err`, so anstream's global choice is set to a passthrough
/// `Always`: it must not strip the ANSI we deliberately embed for a TTY stream,
/// and a global `Never` would wrongly strip the stderr color we keep. The
/// `anstream::stdout()`/`anstream::stderr()` wrappers still handle Windows VT
/// enablement.
pub fn init(ui: Ui) {
    anstream::ColorChoice::Always.write_global();
    let _ = UI.set(ui);
}

/// The process-global `Ui`. Falls back to a safe auto default (for tests and
/// any pre-init caller).
pub fn ui() -> &'static Ui {
    UI.get_or_init(|| Ui::from_process(ColorFlag::Auto, false, false, false))
}

// ---------------------------------------------------------------------------
// Checklist (indicatif-backed)
// ---------------------------------------------------------------------------

/// A group of sequential steps sharing one stderr drawing area.
pub struct Checklist {
    multi: MultiProgress,
    ui: Ui,
}

impl Checklist {
    /// Begin a step. Interactive terminals get a live spinner with a dim
    /// label; non-interactive or quiet runs draw nothing until the step
    /// resolves.
    pub fn step(&self, label: &str) -> Step {
        let pb = if self.ui.interactive && !self.ui.quiet {
            let (frames, interval) = if self.ui.unicode {
                ("\u{280b}\u{2819}\u{2839}\u{2838}\u{283c}\u{2834}\u{2826}\u{2827}\u{2807}\u{280f}", 80)
            } else {
                ("-\\|/", 130)
            };
            let pb = self.multi.add(ProgressBar::new_spinner());
            if let Ok(style) = ProgressStyle::with_template("{spinner} {msg} {elapsed:.dim}") {
                pb.set_style(style.tick_chars(frames));
            }
            pb.set_message(self.ui.paint_err(self.ui.dim(), label));
            pb.enable_steady_tick(Duration::from_millis(interval));
            Some(pb)
        } else {
            None
        };
        Step {
            pb,
            start: Instant::now(),
            ui: self.ui,
            label: label.to_string(),
        }
    }
}

/// One in-flight step: a spinner (when interactive) plus its start time.
pub struct Step {
    pb: Option<ProgressBar>,
    start: Instant,
    ui: Ui,
    label: String,
}

enum StepStatus {
    Ok,
    Fail,
    Warn,
}

impl Step {
    /// Update the live spinner's "why still waiting" suffix.
    pub fn tick_detail(&self, detail: &str) {
        if let Some(pb) = &self.pb {
            let msg = format!(
                "{}   {}",
                self.ui.paint_err(self.ui.dim(), &self.label),
                self.ui.paint_err(self.ui.dim(), detail)
            );
            pb.set_message(msg);
        }
    }

    /// Freeze the step to a success line with an optional detail.
    pub fn done(self, detail: &str) {
        self.finish(StepStatus::Ok, detail);
    }

    /// Freeze the step to a failure line (also used for timeouts).
    pub fn fail(self, detail: &str) {
        self.finish(StepStatus::Fail, detail);
    }

    /// Freeze the step to an amber recovery line. Used when the first attempt
    /// is a planned retry rather than a terminal failure.
    pub fn warn(self, detail: &str) {
        self.finish(StepStatus::Warn, detail);
    }

    /// Clear the step's spinner without committing any line. Used when a spinner
    /// only marks "waiting for the first token" and the payload itself follows.
    pub fn clear(self) {
        if let Some(pb) = self.pb {
            pb.finish_and_clear();
        }
    }

    fn finish(self, status: StepStatus, detail: &str) {
        let elapsed = fmt_elapsed(self.start.elapsed());
        if self.ui.quiet {
            if let Some(pb) = self.pb {
                pb.finish_and_clear();
            }
            return;
        }
        match self.pb {
            Some(pb) => {
                let line = self.ui.step_line(status, &self.label, detail, &elapsed);
                if let Ok(style) = ProgressStyle::with_template("{msg}") {
                    pb.set_style(style);
                }
                pb.finish_with_message(line);
            }
            None => {
                let line = match status {
                    StepStatus::Ok => format!("{}: ok ({elapsed})", self.label),
                    StepStatus::Fail => {
                        format!("{}: failed ({detail}) ({elapsed})", self.label)
                    }
                    StepStatus::Warn => format!("{}: retrying ({elapsed})", self.label),
                };
                let _ = writeln!(anstream::stderr(), "{line}");
            }
        }
    }
}

/// A minimal determinate bar handle.
pub struct Bar {
    pb: ProgressBar,
}

impl Bar {
    /// Advance the bar by `n`.
    pub fn inc(&self, n: u64) {
        self.pb.inc(n);
    }

    /// Clear the bar.
    pub fn finish(self) {
        self.pb.finish_and_clear();
    }
}

// ---------------------------------------------------------------------------
// Layout helpers
// ---------------------------------------------------------------------------

/// Format a duration: one decimal `s` under a minute, integer `s` past it.
pub fn fmt_elapsed(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 60.0 {
        format!("{secs:.1}s")
    } else {
        format!("{}s", d.as_secs())
    }
}

/// Display width of a string: char count with ANSI CSI sequences stripped, so
/// pre-styled cells still align.
fn display_width(s: &str) -> usize {
    let mut width = 0;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // Skip a CSI sequence up to and including its final letter.
            for c2 in chars.by_ref() {
                if c2.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            width += 1;
        }
    }
    width
}

/// Render a borderless aligned table. Columns are left-aligned except those in
/// `numeric_cols`, which are right-aligned. Widths are computed on display
/// width, so cells containing ANSI still align. Output is plain (no color) so
/// the function stays pure; callers pre-style cells and print via
/// `payload_plain`.
pub fn table(headers: &[&str], rows: &[Vec<String>], numeric_cols: &[usize]) -> String {
    let ncols = headers.len();
    let mut widths = vec![0usize; ncols];
    for (i, h) in headers.iter().enumerate() {
        widths[i] = widths[i].max(display_width(h));
    }
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < ncols {
                widths[i] = widths[i].max(display_width(cell));
            }
        }
    }
    let render = |cells: &[String]| -> String {
        let mut parts = Vec::with_capacity(ncols);
        for (i, width) in widths.iter().enumerate() {
            let cell = cells.get(i).map(String::as_str).unwrap_or("");
            let pad = width.saturating_sub(display_width(cell));
            let padded = if numeric_cols.contains(&i) {
                format!("{}{cell}", " ".repeat(pad))
            } else {
                format!("{cell}{}", " ".repeat(pad))
            };
            parts.push(padded);
        }
        parts.join("  ")
    };
    let header_cells: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    let mut out = render(&header_cells);
    for row in rows {
        out.push('\n');
        out.push_str(&render(row));
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn base_env() -> UiEnv {
        UiEnv {
            no_color: false,
            clicolor_zero: false,
            clicolor_force: false,
            term_dumb: false,
            ci: false,
            stderr_tty: false,
            stdout_tty: false,
            utf8: true,
            truecolor: true,
        }
    }

    #[test]
    fn always_beats_no_color() {
        let env = UiEnv {
            no_color: true,
            ..base_env()
        };
        assert!(Ui::resolve(ColorFlag::Always, false, false, false, &env).color_stderr);
    }

    #[test]
    fn never_beats_clicolor_force() {
        let env = UiEnv {
            clicolor_force: true,
            stderr_tty: true,
            ..base_env()
        };
        assert!(!Ui::resolve(ColorFlag::Never, false, false, false, &env).color_stderr);
    }

    #[test]
    fn auto_tty_on_no_tty_off() {
        let on = UiEnv {
            stderr_tty: true,
            ..base_env()
        };
        let off = base_env();
        assert!(Ui::resolve(ColorFlag::Auto, false, false, false, &on).color_stderr);
        assert!(!Ui::resolve(ColorFlag::Auto, false, false, false, &off).color_stderr);
    }

    #[test]
    fn clicolor_force_wins_over_no_color_under_auto() {
        let env = UiEnv {
            no_color: true,
            clicolor_force: true,
            ..base_env()
        };
        assert!(Ui::resolve(ColorFlag::Auto, false, false, false, &env).color_stderr);
    }

    #[test]
    fn ci_keeps_color_but_not_interactive() {
        let env = UiEnv {
            ci: true,
            stderr_tty: true,
            ..base_env()
        };
        let ui = Ui::resolve(ColorFlag::Auto, false, false, false, &env);
        assert!(ui.color_stderr, "color follows tty under CI+Auto");
        assert!(!ui.interactive, "CI is never interactive");
    }

    #[test]
    fn term_dumb_forces_color_off_and_not_interactive() {
        let env = UiEnv {
            term_dumb: true,
            stderr_tty: true,
            ..base_env()
        };
        let ui = Ui::resolve(ColorFlag::Auto, false, false, false, &env);
        assert!(!ui.color_stderr);
        assert!(!ui.interactive);
    }

    #[test]
    fn auto_colors_stderr_but_not_piped_stdout() {
        let env = UiEnv {
            stderr_tty: true,
            stdout_tty: false,
            ..base_env()
        };
        let ui = Ui::resolve(ColorFlag::Auto, false, false, false, &env);
        assert!(
            ui.color_stderr,
            "diagnostics on a terminal stderr are colored"
        );
        assert!(!ui.color_stdout, "payload piped to a file/pipe stays clean");
    }

    #[test]
    fn unicode_reflects_env() {
        let utf8 = base_env();
        let ascii = UiEnv {
            utf8: false,
            ..base_env()
        };
        assert!(Ui::resolve(ColorFlag::Auto, false, false, false, &utf8).unicode);
        assert!(!Ui::resolve(ColorFlag::Auto, false, false, false, &ascii).unicode);
    }

    #[test]
    fn palette_degrades_to_ansi16_without_truecolor() {
        // With truecolor off, success renders the 4-bit green SGR (32), not a
        // 24-bit 38;2 sequence.
        let tc_off = UiEnv {
            stderr_tty: true,
            truecolor: false,
            ..base_env()
        };
        let ui = Ui::resolve(ColorFlag::Always, false, false, false, &tc_off);
        let s = ui.paint_err(ui.green(), "x");
        assert!(
            s.contains("\x1b[32m") || s.contains("[32m"),
            "expected 4-bit green, got {s:?}"
        );
        assert!(
            !s.contains("38;2"),
            "must not emit truecolor when unsupported: {s:?}"
        );
        // And truecolor ON still emits 24-bit.
        let tc_on = UiEnv {
            stderr_tty: true,
            truecolor: true,
            ..base_env()
        };
        let ui2 = Ui::resolve(ColorFlag::Always, false, false, false, &tc_on);
        assert!(
            ui2.paint_err(ui2.green(), "x").contains("38;2"),
            "truecolor path emits 24-bit"
        );
        // dim off truecolor degrades to the portable faint attribute (SGR 2),
        // never ANSI bright-black (90m) which is unreadable on dark terminals,
        // and never a 24-bit foreground.
        let dim_off = ui.paint_err(ui.dim(), "x");
        assert!(
            !dim_off.contains("\x1b[90m") && !dim_off.contains("[90m"),
            "dim must not degrade to unreadable bright-black: {dim_off:?}"
        );
        assert!(
            !dim_off.contains("38;2"),
            "dim must not emit truecolor when unsupported: {dim_off:?}"
        );
        assert!(
            dim_off.contains("\x1b[2m") || dim_off.contains("[2m"),
            "dim degrades to the faint attribute: {dim_off:?}"
        );
    }

    #[test]
    fn fmt_elapsed_formats_sub_minute_and_past_minute() {
        assert_eq!(fmt_elapsed(Duration::from_millis(2900)), "2.9s");
        assert_eq!(fmt_elapsed(Duration::from_millis(12400)), "12.4s");
        assert_eq!(fmt_elapsed(Duration::from_secs(75)), "75s");
    }

    #[test]
    fn display_width_strips_ansi() {
        let styled = format!(
            "{}ok{}",
            Style::new().bold().render(),
            Style::new().bold().render_reset()
        );
        assert_eq!(display_width(&styled), 2);
        assert_eq!(display_width("plain"), 5);
    }

    #[test]
    fn table_aligns_columns_with_numeric_right() {
        let t = table(
            &["NAME", "COUNT"],
            &[
                vec!["a".to_string(), "1".to_string()],
                vec!["bb".to_string(), "20".to_string()],
            ],
            &[1],
        );
        let lines: Vec<&str> = t.lines().collect();
        assert_eq!(lines.len(), 3);
        let w = display_width(lines[0]);
        for l in &lines {
            assert_eq!(display_width(l), w, "misaligned row: {l:?}");
        }
        assert!(lines[0].starts_with("NAME"));
        assert!(
            lines[1].ends_with("    1"),
            "row1 numeric col: {:?}",
            lines[1]
        );
        assert!(
            lines[2].ends_with("   20"),
            "row2 numeric col: {:?}",
            lines[2]
        );
    }
}

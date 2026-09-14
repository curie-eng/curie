//! Interactive terminal interface for Curie.
//!
//! This is a ratatui/crossterm surface over the existing clap command grammar:
//! it does not invent a second implementation path. The TUI helps a human pick
//! a target and action, previews the exact `curie ...` command, prompts for
//! any required values, then keeps prompts, command output, and workflow results
//! inside the alternate-screen interface.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, BufReader, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
    MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::channel::{parse_terminal_message, TerminalAction, REPLY_FENCE};
use crate::comms::SLACK_ONE_APP_PER_RELEASE_NOTE;
use crate::recipes::{build_argv, recipes, ArgPart, Recipe, RecipeKind, Tier, TuiAction, Workflow};

/// The model credentials Curie accepts, in the order every lookup tries them.
///
/// One list so the workflow, the status readout, and the availability checks
/// cannot drift apart.
const MODEL_CREDENTIAL_NAMES: &[&str] = &[
    "CURIE_CREDENTIALS",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
];

#[derive(Clone, Debug, PartialEq, Eq)]
enum SecretNameChoice {
    Name(String),
    Custom,
}

#[derive(Clone, Debug)]
struct SelectChoice<T> {
    label: String,
    description: String,
    value: T,
}

#[derive(Clone, Debug)]
struct ExampleChoice {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    directory: &'static str,
    secrets: &'static [&'static str],
}

/// What the recipe prompt collected: the answered tier (`None` when the recipe
/// is not tier-bearing) and the answered field values.
#[derive(Clone, Debug)]
struct RecipeAnswers {
    tier: Option<Tier>,
    values: BTreeMap<String, String>,
}

#[derive(Debug)]
struct App {
    recipes: Vec<Recipe>,
    targets: Vec<&'static str>,
    target_idx: usize,
    selected: usize,
    message: String,
}

pub async fn run() -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(crate::exit::usage(
            "curie interactive requires an interactive terminal. In a non-interactive session, \
             run `curie doctor` to see what is set up and the command that fixes each gap.",
        ));
    }
    let mut app = App::new();
    let mut terminal = TerminalSession::enter()?;
    let result = event_loop(&mut terminal.terminal, &mut app);
    terminal.leave()?;
    result
}

fn event_loop(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, app: &mut App) -> Result<()> {
    loop {
        terminal.draw(|frame| draw(frame, app))?;
        if !event::poll(Duration::from_millis(200))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if app.handle_key(key, terminal)? {
            break;
        }
    }
    Ok(())
}

impl App {
    fn new() -> Self {
        let recipes = recipes();
        App {
            recipes,
            // "platform" leads (it is the default landing tab, target_idx 0): the
            // primary product functions -- parity/evals/observe/version/budget/
            // approve/remember -- come before the per-tier operator verbs.
            targets: vec![
                "platform", "all", "skill", "secrets", "local", "cluster", "dev",
            ],
            target_idx: 0,
            selected: 0,
            message: "Select an action. Enter runs it; q exits.".into(),
        }
    }

    fn visible_indices(&self) -> Vec<usize> {
        let target = self.targets[self.target_idx];
        self.recipes
            .iter()
            .enumerate()
            .filter_map(|(idx, recipe)| {
                (target == "all" || recipe.tabs.contains(&target)).then_some(idx)
            })
            .collect()
    }

    fn selected_recipe(&self) -> Option<&Recipe> {
        self.visible_indices()
            .get(self.selected)
            .and_then(|idx| self.recipes.get(*idx))
    }

    fn handle_key(
        &mut self,
        key: KeyEvent,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> Result<bool> {
        match (key.code, key.modifiers) {
            (KeyCode::Char('q'), _) | (KeyCode::Esc, _) => return Ok(true),
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(true),
            (KeyCode::Down | KeyCode::Char('j'), _) => self.move_selection(1),
            (KeyCode::Up | KeyCode::Char('k'), _) => self.move_selection(-1),
            (KeyCode::Tab | KeyCode::Right, _) => self.next_target(),
            (KeyCode::BackTab | KeyCode::Left, _) => self.prev_target(),
            (KeyCode::Enter, _) | (KeyCode::Char('r'), _) => self.run_selected(terminal)?,
            _ => {}
        }
        Ok(false)
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.visible_indices().len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        self.selected = ((self.selected as isize + delta).rem_euclid(len as isize)) as usize;
    }

    fn next_target(&mut self) {
        self.target_idx = (self.target_idx + 1) % self.targets.len();
        self.selected = 0;
    }

    fn prev_target(&mut self) {
        self.target_idx = if self.target_idx == 0 {
            self.targets.len() - 1
        } else {
            self.target_idx - 1
        };
        self.selected = 0;
    }

    fn run_selected(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> Result<()> {
        let Some(recipe) = self.selected_recipe().cloned() else {
            return Ok(());
        };
        let result = match &recipe.kind {
            RecipeKind::Tui(action) => run_tui_action(*action, terminal, self),
            RecipeKind::Command | RecipeKind::Workflow(_) => {
                run_recipe_in_tui(terminal, self, &recipe)
            }
        };
        self.message = match result {
            Ok(message) => message,
            Err(err) => format!("Action failed: {err:#}"),
        };
        Ok(())
    }
}

struct TerminalSession {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
    active: bool,
}

impl TerminalSession {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("enabling terminal raw mode")?;
        execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)
            .context("entering alternate screen")?;
        let backend = CrosstermBackend::new(io::stdout());
        let mut terminal = Terminal::new(backend).context("creating terminal")?;
        terminal.clear().ok();
        Ok(TerminalSession {
            terminal,
            active: true,
        })
    }

    fn leave(&mut self) -> Result<()> {
        if self.active {
            disable_raw_mode().ok();
            execute!(io::stdout(), DisableMouseCapture, LeaveAlternateScreen)
                .context("leaving alternate screen")?;
            self.active = false;
        }
        Ok(())
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = self.leave();
    }
}

/// The one tier prompt: which platform tier a tier-bearing recipe or workflow
/// drives. Shared by the governance recipes and the Deploy-to-Slack workflow so
/// the two surfaces cannot drift apart.
fn prompt_tier(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    title: &str,
) -> Result<Option<Tier>> {
    prompt_select(
        terminal,
        app,
        title,
        "Which tier?",
        vec![
            SelectChoice {
                label: "local (compose)".to_string(),
                description: "The full platform on your machine via docker compose.".to_string(),
                value: Tier::Local,
            },
            SelectChoice {
                label: "cluster (Kubernetes)".to_string(),
                description: "A deployed Helm release (must already be up); needs CURIE_API_URL and CURIE_API_KEY set for that release."
                    .to_string(),
                value: Tier::Cluster,
            },
        ],
    )
}

fn prompt_recipe_fields(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    recipe: &Recipe,
) -> Result<Option<RecipeAnswers>> {
    let mut values = BTreeMap::new();
    // The tier leads the per-field prompts so the steps read in argv order. A
    // recipe that is not tier-bearing answers no tier at all.
    let mut chosen_tier = None;
    if recipe.args.contains(&ArgPart::Tier) {
        let Some(tier) = prompt_tier(terminal, app, recipe.title)? else {
            return Ok(None);
        };
        chosen_tier = Some(tier);
    }
    for (idx, field) in recipe.fields.iter().enumerate() {
        let title = format!(
            "{} · Step {} of {}",
            recipe.title,
            idx + 1,
            recipe.fields.len()
        );
        let Some(value) = prompt_text(
            terminal,
            app,
            &title,
            field.label,
            field.default,
            false,
            !field.required,
        )?
        else {
            return Ok(None);
        };
        if field.required && value.is_empty() {
            return Ok(None);
        }
        values.insert(field.key.to_string(), value);
    }
    Ok(Some(RecipeAnswers {
        tier: chosen_tier,
        values,
    }))
}

fn run_recipe_in_tui(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    recipe: &Recipe,
) -> Result<String> {
    let Some(RecipeAnswers { tier, values }) = prompt_recipe_fields(terminal, app, recipe)? else {
        return Ok(format!("Canceled: {}", recipe.title));
    };
    if let RecipeKind::Workflow(workflow) = &recipe.kind {
        match workflow {
            Workflow::ExploreExamples => explore_examples(terminal, app, &values)?,
            Workflow::ParityLadder => parity_ladder(terminal, app)?,
            Workflow::DeployToSlack => deploy_to_slack(terminal, app)?,
        }
        return Ok(format!("Finished: {}", recipe.title));
    }
    let mut view = RunView::new(recipe.title);
    let run_result = match &recipe.kind {
        RecipeKind::Command => {
            let argv = build_argv(recipe, tier, &values);
            run_curie_in_tui(terminal, app, &argv, Path::new("."), &mut view)
        }
        RecipeKind::Tui(_) => Ok(()),
        RecipeKind::Workflow(_) => unreachable!("workflows run before the command view"),
    };
    if let Err(err) = &run_result {
        view.push("");
        view.push(format!("ERROR: {err:#}"));
    }
    show_run_view(terminal, app, &mut view)?;
    run_result?;
    Ok(format!("Finished: {}", recipe.title))
}

fn run_tui_action(
    action: TuiAction,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
) -> Result<String> {
    match action {
        TuiAction::SaveSecret => {
            let Some(choice) = prompt_select(
                terminal,
                app,
                "Save Secret",
                "Choose a secret name",
                secret_name_choices()?,
            )?
            else {
                return Ok("Save secret canceled.".to_string());
            };
            let name = match choice {
                SecretNameChoice::Name(name) => name,
                SecretNameChoice::Custom => {
                    let Some(name) = prompt_text(
                        terminal,
                        app,
                        "Save Secret",
                        "Custom secret name",
                        None,
                        false,
                        false,
                    )?
                    else {
                        return Ok("Save secret canceled.".to_string());
                    };
                    name
                }
            };
            crate::secrets::validate_name(&name)?;
            let Some(value) = prompt_text(terminal, app, "Save Secret", &name, None, true, false)?
            else {
                return Ok("Save secret canceled.".to_string());
            };
            crate::secrets::save_value(&name, &value)?;
            Ok(format!("Saved {name} in Curie private storage."))
        }
        TuiAction::ListSecrets => {
            let count = crate::secrets::list_names()?.len();
            Ok(match count {
                0 => "No Curie secrets saved.".to_string(),
                1 => "Showing 1 saved secret name.".to_string(),
                n => format!("Showing {n} saved secret names."),
            })
        }
        TuiAction::RemoveSecret => {
            let Some(choice) = prompt_select(
                terminal,
                app,
                "Remove Secret",
                "Choose a saved secret",
                saved_secret_choices()?,
            )?
            else {
                return Ok("Remove secret canceled.".to_string());
            };
            let name = match choice {
                SecretNameChoice::Name(name) => name,
                SecretNameChoice::Custom => {
                    let Some(name) = prompt_text(
                        terminal,
                        app,
                        "Remove Secret",
                        "Custom secret name",
                        None,
                        false,
                        false,
                    )?
                    else {
                        return Ok("Remove secret canceled.".to_string());
                    };
                    name
                }
            };
            crate::secrets::validate_name(&name)?;
            let Some(confirm) = prompt_text(
                terminal,
                app,
                "Remove Secret",
                &format!("Type {name} to confirm"),
                None,
                false,
                false,
            )?
            else {
                return Ok("Remove secret canceled.".to_string());
            };
            if confirm != name {
                return Ok("Remove secret canceled.".to_string());
            }
            crate::secrets::remove_all_values(&name)?;
            Ok(format!("Removed all stored entries for {name}."))
        }
    }
}

fn secret_name_choices() -> Result<Vec<SelectChoice<SecretNameChoice>>> {
    let saved_names = crate::secrets::list_names()?.into_iter().collect();
    Ok(secret_name_choices_for(&saved_names))
}

fn secret_name_choices_for(saved_names: &BTreeSet<String>) -> Vec<SelectChoice<SecretNameChoice>> {
    let common = [
        ("ANTHROPIC_API_KEY", "Anthropic API key for model calls"),
        (
            "CLAUDE_CODE_OAUTH_TOKEN",
            "Claude Code OAuth token for model calls",
        ),
        (
            "CURIE_CREDENTIALS",
            "Provider credential forwarded as Curie credentials",
        ),
        (
            "OPENAI_API_KEY",
            "OpenAI integrations (not Curie runner model auth)",
        ),
        (
            "GITHUB_PERSONAL_ACCESS_TOKEN",
            "GitHub token for MCP examples or bundles",
        ),
    ];
    let mut choices = common
        .into_iter()
        .map(|(name, description)| {
            let saved = saved_names.contains(name);
            SelectChoice {
                label: if saved {
                    format!("{name} ✓")
                } else {
                    name.to_string()
                },
                description: if saved {
                    format!("{description} (saved)")
                } else {
                    description.to_string()
                },
                value: SecretNameChoice::Name(name.to_string()),
            }
        })
        .collect::<Vec<_>>();
    choices.push(SelectChoice {
        label: "Custom secret name".to_string(),
        description: "Enter any env-style secret name".to_string(),
        value: SecretNameChoice::Custom,
    });
    choices
}

fn saved_secret_choices() -> Result<Vec<SelectChoice<SecretNameChoice>>> {
    let mut choices: Vec<SelectChoice<SecretNameChoice>> = crate::secrets::list_names()?
        .into_iter()
        .map(|name| SelectChoice {
            label: name.clone(),
            description: "Saved in Curie private storage".to_string(),
            value: SecretNameChoice::Name(name),
        })
        .collect();
    choices.push(SelectChoice {
        label: "Custom secret name".to_string(),
        description: "Remove a name not shown here".to_string(),
        value: SecretNameChoice::Custom,
    });
    Ok(choices)
}

fn prompt_select<T: Clone>(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    title: &str,
    label: &str,
    choices: Vec<SelectChoice<T>>,
) -> Result<Option<T>> {
    if choices.is_empty() {
        return Ok(None);
    }
    let mut selected = 0usize;
    loop {
        terminal.draw(|frame| {
            draw(frame, app);
            draw_select_prompt(frame, title, label, &choices, selected);
        })?;
        let Event::Key(key) = event::read()? else {
            continue;
        };
        match (key.code, key.modifiers) {
            (KeyCode::Esc, _) => return Ok(None),
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(None),
            (KeyCode::Enter, _) => return Ok(Some(choices[selected].value.clone())),
            (KeyCode::Down | KeyCode::Char('j'), _) => {
                selected = (selected + 1) % choices.len();
            }
            (KeyCode::Up | KeyCode::Char('k'), _) => {
                selected = if selected == 0 {
                    choices.len() - 1
                } else {
                    selected - 1
                };
            }
            (KeyCode::Char(ch), _) if ch.is_ascii_digit() => {
                if let Some(digit) = ch.to_digit(10) {
                    let idx = digit as usize;
                    if idx > 0 && idx <= choices.len() {
                        selected = idx - 1;
                    }
                }
            }
            _ => {}
        }
    }
}

fn prompt_text(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    title: &str,
    label: &str,
    default: Option<&str>,
    secret: bool,
    allow_empty: bool,
) -> Result<Option<String>> {
    let mut value = String::new();
    loop {
        terminal.draw(|frame| {
            draw(frame, app);
            draw_prompt(frame, title, label, default, &value, secret, allow_empty);
        })?;
        let Event::Key(key) = event::read()? else {
            continue;
        };
        match (key.code, key.modifiers) {
            (KeyCode::Esc, _) => return Ok(None),
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(None),
            (KeyCode::Enter, _) => {
                if value.is_empty() {
                    if let Some(default) = default {
                        return Ok(Some(default.to_string()));
                    }
                    if allow_empty {
                        return Ok(Some(String::new()));
                    }
                    continue;
                }
                return Ok(Some(value));
            }
            (KeyCode::Backspace, _) => {
                value.pop();
            }
            (KeyCode::Char(ch), _) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                value.push(ch);
            }
            _ => {}
        }
    }
}

/// Print-only "parity ladder" explainer: the framing for the whole Platform tab
/// -- what curie is (one bundle + one eval suite across skill/local/cluster) and
/// how to climb it. Mirrors the show_run_view pause used by the other workflows.
fn parity_ladder(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, app: &App) -> Result<()> {
    let mut view = RunView::new("The parity ladder");
    for line in [
        "curie runs ONE immutable bundle and ONE evals/cases.json identically",
        "across three tiers, so 'works on my laptop' and 'works deployed' are the",
        "same artifact -- not a hope.",
        "",
        "  skill    runner only, on your Docker. Fastest loop; no platform/queue.",
        "           -> curie skill up / skill message / skill eval",
        "  local    the full platform via docker compose (API, queue, worker,",
        "           sandbox, Langfuse). The real product loop, zero Slack/K8s.",
        "           -> curie local up / local deploy / local message",
        "  cluster  the same platform on Kubernetes (a Helm release).",
        "           -> curie cluster deploy / cluster message",
        "",
        "Climb it: iterate at skill, promote to local, then cluster. Run the SAME",
        "evals at each tier -- a tier-to-tier divergence is the harness catching a",
        "deployment bug, not a flaky test.",
        "",
        "The other Platform actions (evals, observability, versions, budget,",
        "approvals, memory) let you evaluate, observe, and govern a deployed agent.",
    ] {
        view.push(line);
    }
    show_run_view(terminal, app, &mut view)?;
    Ok(())
}

fn explore_examples(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    _values: &BTreeMap<String, String>,
) -> Result<()> {
    let choices = example_choices()
        .into_iter()
        .map(|example| SelectChoice {
            label: example.name.to_string(),
            description: example.description.to_string(),
            value: example,
        })
        .collect();
    let Some(example) = prompt_select(
        terminal,
        app,
        "Explore Examples",
        "Choose an agent to run",
        choices,
    )?
    else {
        return Ok(());
    };

    crate::secrets::sync_secret_file()?;
    ensure_model_credential_available(terminal, app)?;
    for name in example.secrets {
        ensure_secret_available(terminal, app, name)?;
    }
    let secret_env = example_secret_env(example.secrets)?;

    let repo_root = find_repo_root(std::env::current_dir().context("reading current directory")?)
        .context(
        "could not find Curie repo root; run this workflow from the source checkout",
    )?;
    let example_dir = repo_root.join(example.directory);
    if !example_dir.join(".claude-plugin/plugin.json").is_file() {
        anyhow::bail!("missing example bundle at {}", example_dir.display());
    }
    let starter_prompts = starter_prompts(&example_dir)?;

    let container_name = format!("curie-example-{}", example.id);
    let port = "7247";
    let url = format!("http://localhost:{port}");
    let mut setup = RunView::new(&format!("Starting {}", example.name));
    let mut started = false;
    let run_result = (|| -> Result<()> {
        let mut argv = vec![
            "skill".to_string(),
            "up".to_string(),
            "--plugin-dir".to_string(),
            example_dir.display().to_string(),
            "--port".to_string(),
            port.to_string(),
            "--name".to_string(),
            container_name.clone(),
        ];
        for name in example.secrets {
            argv.push("--secret".to_string());
            argv.push((*name).to_string());
        }
        run_curie_in_tui_with_env(terminal, app, &argv, &repo_root, &mut setup, &secret_env)?;
        started = true;
        chat_with_runner(terminal, &repo_root, &url, example.name, &starter_prompts)
    })();

    if started {
        let mut cleanup = RunView::new(&format!("Stopping {}", example.name));
        if let Err(err) = run_curie_in_tui(
            terminal,
            app,
            &["skill".to_string(), "down".to_string()],
            &example_dir,
            &mut cleanup,
        ) {
            cleanup.push(format!("Cleanup warning: {err:#}"));
            show_run_view(terminal, app, &mut cleanup)?;
        }
    }

    if let Err(err) = run_result {
        setup.push("");
        setup.push(format!("ERROR: {err:#}"));
        show_run_view(terminal, app, &mut setup)?;
        return Err(err);
    }
    Ok(())
}

/// Guided "Deploy to Slack" workflow: walk the operator through the one-time
/// Slack-app creation (the part that can't be automated), collect the tokens +
/// channel id into the secret vault, then run `<tier> deploy` -> `<tier> comms
/// --slack` (local additionally brings the compose stack up first) inside the
/// TUI. Mirrors `explore_examples`.
fn deploy_to_slack(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, app: &App) -> Result<()> {
    // One recipe, both tiers: ask which platform to target rather than listing a
    // near-duplicate recipe per tier.
    let Some(tier) = prompt_tier(terminal, app, "Deploy to Slack")? else {
        return Ok(());
    };
    let verb = tier.verb();

    // 1. The manual, browser-only part: create the Slack app and gather the two
    //    tokens + the channel id. Pause until the operator has them.
    let mut intro = RunView::new("Deploy to Slack — create your Slack app");
    let flow = match tier {
        Tier::Local => "compose stack: local up -> local deploy -> local comms --slack.",
        Tier::Cluster => "deployed release: cluster deploy -> cluster comms --slack.",
    };
    intro.push(format!(
        "This connects a Curie agent to a real Slack workspace through the {flow}"
    ));
    if tier == Tier::Cluster {
        intro.push("Requires an installed release (curie cluster up) with a model credential.");
    }
    intro.push(SLACK_ONE_APP_PER_RELEASE_NOTE);
    for line in [
        "",
        "STEP 1 (one time, in your browser -- not automatable):",
        "  1. Open https://api.slack.com/apps  ->  Create New App  ->  From a manifest",
        "  2. Paste the manifest from this repo: apps/dispatcher/slack-app-manifest.yaml",
        "  3. 'Basic Information' -> 'App-Level Tokens' -> Generate a token with the",
        "     'connections:write' scope. Copy the xapp-... value  (SLACK_APP_TOKEN).",
        "  4. 'Install App' -> 'Install to Workspace'. Copy the 'Bot User OAuth Token'",
        "     (xoxb-...)  (SLACK_BOT_TOKEN).",
        "  5. In Slack, invite the bot to a channel:  /invite @curie",
        "  6. Copy that channel's ID: channel name -> 'View channel details' -> the",
        "     C0... id at the very bottom.",
        "",
    ] {
        intro.push(line);
    }
    // Say what will actually be asked. The `ensure_*` helpers below already skip
    // a credential that is exported or saved, but they skip it SILENTLY -- so an
    // operator who is half set up cannot tell whether to have the Slack page
    // open before starting.
    let asks = planned_prompts(
        tier == Tier::Local,
        model_credential_available(),
        secret_available("SLACK_APP_TOKEN"),
        secret_available("SLACK_BOT_TOKEN"),
    );
    intro.push("");
    intro.push(format!("This will ask you for {} things:", asks.len()));
    for ask in &asks {
        intro.push(format!("  - {ask}"));
    }
    intro.push("");
    intro.push("Press Enter when you have them.".to_string());
    show_run_view(terminal, app, &mut intro)?;

    // 2. Save the model credential + Slack tokens into the vault (hidden input).
    //    On the cluster tier the model credential is configured on the release at
    //    `cluster up` time (a chart Secret), so only the Slack tokens are gathered.
    crate::secrets::sync_secret_file()?;
    if tier == Tier::Local {
        ensure_model_credential_available(terminal, app)?;
    }
    ensure_secret_available(terminal, app, "SLACK_APP_TOKEN")?;
    ensure_secret_available(terminal, app, "SLACK_BOT_TOKEN")?;

    // 3. Channel id + bundle dir, prompted after the operator has completed step 1.
    //    Resolved before the bundle-dir prompt so a picker over `agents/` bundles
    //    can be offered instead of a bare text box.
    let cwd = std::env::current_dir().context("reading current directory")?;
    let repo_root = find_repo_root(cwd.clone())
        .context("could not find Curie repo root; run this workflow from the source checkout")?;

    let channel = prompt_checked(
        terminal,
        app,
        "Deploy to Slack",
        "Slack channel ID (C0..., D0..., or G0...)",
        "A Slack channel ID is required to deploy to Slack",
        false,
        crate::credcheck::check_channel_id,
    )?;
    let plugin_dir = prompt_bundle_dir(terminal, app, "Deploy to Slack", &repo_root)?
        .filter(|dir| !dir.trim().is_empty())
        .unwrap_or_else(|| ".".to_string());

    // Cluster targets a specific Helm release; prompt for its namespace/release
    // so the workflow works for a release not named the default `curie` (the
    // API auto-discovery and `cluster comms` both key off these).
    let (namespace, release) = if tier == Tier::Cluster {
        let ns = prompt_text(
            terminal,
            app,
            "Deploy to Slack",
            "Kubernetes namespace",
            Some("curie"),
            false,
            true,
        )?
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "curie".to_string());
        let rel = prompt_text(
            terminal,
            app,
            "Deploy to Slack",
            "Helm release name",
            Some("curie"),
            false,
            true,
        )?
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "curie".to_string());
        (Some(ns), Some(rel))
    } else {
        (None, None)
    };

    // 4. Forward the model credential + Slack tokens into the child processes.
    //    `<tier> comms --slack` reads SLACK_APP_TOKEN/SLACK_BOT_TOKEN from its env;
    //    a value already exported is inherited by the child, so only vault-stored
    //    ones are forwarded here.
    let secret_env = slack_secret_env()?;

    // Resolve the bundle dir to an absolute path so `--plugin-dir` finds it
    // regardless of the run cwd (compose files / the chart are repo-relative on
    // a dev checkout, hence running everything from `repo_root` below).
    let plugin_abs = cwd.join(&plugin_dir);
    if !plugin_abs.join(".claude-plugin/plugin.json").is_file() {
        anyhow::bail!(
            "no agent bundle at {} (expected a .claude-plugin/plugin.json there)",
            plugin_abs.display()
        );
    }

    let mut view = RunView::new(&format!("Deploy to Slack — channel {channel}"));
    let run_result = (|| -> Result<()> {
        // Local brings the compose platform up first (idempotent); the cluster
        // release is expected to already be installed.
        if tier == Tier::Local {
            run_curie_in_tui_with_env(
                terminal,
                app,
                &["local".to_string(), "up".to_string()],
                &repo_root,
                &mut view,
                &secret_env,
            )?;
        }
        // Ship the bundle and bind it to the Slack channel. Local targets the
        // compose API on 28000; cluster auto-discovers the release's UI /api proxy.
        let mut deploy_argv = vec![
            verb.to_string(),
            "deploy".to_string(),
            "--plugin-dir".to_string(),
            plugin_abs.display().to_string(),
            "--slack-channel".to_string(),
            channel.clone(),
        ];
        if tier == Tier::Local {
            deploy_argv.push("--api-url".to_string());
            deploy_argv.push("http://localhost:28000".to_string());
        }
        // Cluster: target the named release (deploy auto-discovers the API from
        // it, comms upgrades it).
        if let (Some(ns), Some(rel)) = (&namespace, &release) {
            deploy_argv.extend([
                "--namespace".to_string(),
                ns.clone(),
                "--release".to_string(),
                rel.clone(),
            ]);
        }
        run_curie_in_tui_with_env(
            terminal,
            app,
            &deploy_argv,
            &repo_root,
            &mut view,
            &secret_env,
        )?;
        // Wire the real Slack tokens and start the dispatcher.
        let mut comms_argv = vec![verb.to_string(), "comms".to_string(), "--slack".to_string()];
        if let (Some(ns), Some(rel)) = (&namespace, &release) {
            comms_argv.extend([
                "--namespace".to_string(),
                ns.clone(),
                "--release".to_string(),
                rel.clone(),
            ]);
        }
        run_curie_in_tui_with_env(
            terminal,
            app,
            &comms_argv,
            &repo_root,
            &mut view,
            &secret_env,
        )
    })();

    if let Err(err) = run_result {
        view.push("");
        view.push(format!("ERROR: {err:#}"));
        show_run_view(terminal, app, &mut view)?;
        return Err(err);
    }

    let mut done = RunView::new("Deploy to Slack — connected");
    done.push(format!(
        "Your agent is deployed and wired to Slack channel {channel}."
    ));
    done.push("");
    done.push("Try it: in Slack, @mention the bot in that channel (or DM it) and send a");
    done.push("message -- the dispatcher routes it through the worker to your agent.");
    done.push("");
    for line in troubleshooting_lines(tier, namespace.as_deref(), release.as_deref()) {
        done.push(line);
    }
    done.push("");
    done.push(format!(
        "Disconnect Slack:  curie {verb} comms --slack --disconnect"
    ));
    if tier == Tier::Local {
        done.push("Stop the stack:    curie local down".to_string());
    }
    show_run_view(terminal, app, &mut done)?;
    Ok(())
}

/// Is `name` exported with a usable value?
///
/// An empty-string credential is absent, not supplied (issue #540): `var_os`
/// alone reports `NAME=""` as present, which would report a credential as
/// available and suppress the vault fallback while forwarding nothing usable.
/// Mirrors `local.rs::model_mode_from_env` and `ops.rs::resolve_up_credentials`.
fn env_credential_present(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.is_empty())
}

/// The env forwarded into the deploy/comms children for the Slack deploy
/// workflow: the first available model credential and the two Slack tokens, each
/// pulled from the vault only when not already exported (an exported value is
/// inherited by the child process directly). The cluster tier ignores the model
/// credential here (it lives on the chart Secret) but forwarding it is harmless.
fn slack_secret_env() -> Result<Vec<(String, String)>> {
    let mut env = Vec::new();
    for name in MODEL_CREDENTIAL_NAMES.iter().copied() {
        if env_credential_present(name) {
            break;
        }
        if let Some(value) = crate::secrets::get_value(name)? {
            env.push((name.to_string(), value));
            break;
        }
    }
    for name in ["SLACK_APP_TOKEN", "SLACK_BOT_TOKEN"] {
        if std::env::var_os(name).is_none() {
            if let Some(value) = crate::secrets::get_value(name)? {
                env.push((name.to_string(), value));
            }
        }
    }
    Ok(env)
}

fn example_choices() -> Vec<ExampleChoice> {
    vec![
        ExampleChoice {
            id: "github-issues",
            name: "GitHub issues",
            description:
                "Explore live repositories and issues through authenticated GitHub MCP tools",
            directory: "examples/github-issues",
            secrets: &["GITHUB_PERSONAL_ACCESS_TOKEN"],
        },
        ExampleChoice {
            id: "text-stats-engine",
            name: "Text stats engine",
            description: "Use an in-bundle MCP server to inspect and analyze text",
            directory: "examples/text-stats-engine",
            secrets: &[],
        },
        ExampleChoice {
            id: "weather",
            name: "Weather",
            description: "Chat with the minimal weather agent bundle",
            directory: "examples/weather",
            secrets: &[],
        },
    ]
}

fn starter_prompts(example_dir: &Path) -> Result<Vec<String>> {
    let path = example_dir.join(".claude-plugin/plugin.json");
    let value: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&path)
            .with_context(|| format!("reading example manifest {}", path.display()))?,
    )
    .with_context(|| format!("parsing example manifest {}", path.display()))?;
    Ok(value
        .get("starterPrompts")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|prompt| !prompt.trim().is_empty())
        .take(10)
        .collect())
}

/// Prompt for an agent bundle directory: a picker over discovered `agents/`
/// bundles when any exist (plus a trailing "Custom path..." choice), else the
/// same free-text prompt as always. `Ok(None)` only when the picker/prompt
/// itself was cancelled (Esc) -- callers keep their existing
/// `.filter(...).unwrap_or_else(|| ".".to_string())` fallback for that case,
/// same as an empty free-text answer.
fn prompt_bundle_dir(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    title: &str,
    repo_root: &Path,
) -> Result<Option<String>> {
    let bundles = crate::discover::discover_bundles(&repo_root.join("agents"))?;
    if bundles.is_empty() {
        return prompt_text(
            terminal,
            app,
            title,
            "Agent bundle directory",
            Some("."),
            false,
            true,
        );
    }

    #[derive(Clone)]
    enum BundleChoice {
        Discovered(PathBuf),
        Custom,
    }

    let mut choices: Vec<SelectChoice<BundleChoice>> = bundles
        .into_iter()
        .map(|b| SelectChoice {
            label: b.name,
            description: b.description,
            value: BundleChoice::Discovered(b.directory),
        })
        .collect();
    choices.push(SelectChoice {
        label: "Custom path...".to_string(),
        description: "Type a bundle directory instead (e.g. \".\" or examples/weather)".to_string(),
        value: BundleChoice::Custom,
    });

    let Some(choice) = prompt_select(terminal, app, title, "Choose an agent bundle", choices)?
    else {
        return Ok(None);
    };
    match choice {
        BundleChoice::Discovered(dir) => Ok(Some(dir.display().to_string())),
        BundleChoice::Custom => prompt_text(
            terminal,
            app,
            title,
            "Agent bundle directory",
            Some("."),
            false,
            true,
        ),
    }
}

fn example_secret_env(extra_secrets: &[&str]) -> Result<Vec<(String, String)>> {
    let mut env = Vec::new();
    if !MODEL_CREDENTIAL_NAMES
        .iter()
        .any(|name| env_credential_present(name))
    {
        for name in MODEL_CREDENTIAL_NAMES.iter().copied() {
            if let Some(value) = crate::secrets::get_value(name)? {
                env.push((name.to_string(), value));
                break;
            }
        }
    }
    for name in extra_secrets {
        if std::env::var_os(name).is_none() {
            if let Some(value) = crate::secrets::get_value(name)? {
                env.push(((*name).to_string(), value));
            }
        }
    }
    Ok(env)
}

/// Prompt until the pasted value has the right shape, or the operator cancels.
///
/// The loop is the point. Rejecting once and aborting the whole workflow would
/// make a mistyped character cost every prior step; re-prompting with the reason
/// in the label makes it a two-second correction. The reason replaces the label
/// rather than appending, so a third attempt does not stack three errors.
fn prompt_checked(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    title: &str,
    label: &str,
    cancel_recovery: &str,
    secret: bool,
    check: impl Fn(&str) -> crate::credcheck::CheckResult,
) -> Result<String> {
    let mut shown = label.to_string();
    loop {
        let Some(value) = prompt_text(terminal, app, title, &shown, None, secret, false)? else {
            anyhow::bail!("{cancel_recovery}");
        };
        match check(&value) {
            Ok(()) => return Ok(value),
            Err(reason) => shown = format!("{label} -- {reason}"),
        }
    }
}

/// Is this credential already available, from the environment or the vault?
///
/// Mirrors the first two checks in `ensure_secret_available` so the preview and
/// the prompt cannot disagree about what will be asked. A vault error reads as
/// "not available": the preview must never fail the workflow.
fn secret_available(name: &str) -> bool {
    env_credential_present(name) || crate::secrets::is_saved(name).unwrap_or(false)
}

/// Any accepted model credential, by the same rule `ensure_model_credential_available` uses.
fn model_credential_available() -> bool {
    MODEL_CREDENTIAL_NAMES.iter().any(|n| secret_available(n))
}

/// What the workflow will actually ask for, given what is already in place.
///
/// The `ensure_*` helpers already SKIP a credential that is exported or saved,
/// silently. Silently is the problem: an operator who has half of this set up
/// cannot tell whether the workflow is about to ask for four things or none,
/// so they cannot tell whether they need the Slack page open before starting.
///
/// Pure so the sequencing is testable; the caller supplies what it observed.
pub(crate) fn planned_prompts(
    needs_model_credential: bool,
    have_model_credential: bool,
    have_app_token: bool,
    have_bot_token: bool,
) -> Vec<&'static str> {
    let mut asks = Vec::new();
    if needs_model_credential && !have_model_credential {
        asks.push("a model credential");
    }
    if !have_app_token {
        asks.push("the Slack app-level token (xapp-)");
    }
    if !have_bot_token {
        asks.push("the Slack bot user token (xoxb-)");
    }
    asks.push("the Slack channel id");
    asks.push("the bundle directory");
    asks
}

/// What to do when the bot does not answer.
///
/// The closing view used to end at "try it in Slack", which is the happy path
/// and the whole story only when it works. The three commands below are the
/// ones that distinguish the three ways it silently does not: the dispatcher
/// never connected, the agent was never bound to that channel, or something
/// earlier in the install is missing.
pub(crate) fn troubleshooting_lines(
    tier: Tier,
    namespace: Option<&str>,
    release: Option<&str>,
) -> Vec<String> {
    let mut out = vec!["If the bot does not answer:".to_string()];
    match tier {
        Tier::Cluster => {
            let ns = namespace.unwrap_or("<namespace>");
            let rel = release.unwrap_or("<release>");
            let dispatcher = chart_dispatcher_deployment_name(rel);
            out.push(format!(
                "  1. is the dispatcher connected to Slack?   kubectl -n {ns} logs deploy/{dispatcher}"
            ));
            out.push(format!(
                "  2. did the bind take?                     curie cluster status --namespace {ns} --release {rel}"
            ));
        }
        Tier::Local => {
            out.push("  1. is the dispatcher running?              curie local status".to_string());
            out.push("  2. did the bind take?                      curie local status".to_string());
        }
    }
    out.push("  3. anything else missing?                  curie doctor".to_string());
    out.push(String::new());
    out.push(
        "The commands above succeeded, which means the bundle deployed and the tokens".to_string(),
    );
    out.push(
        "were accepted. That is not the same as a round trip -- only a real mention is."
            .to_string(),
    );
    out
}

/// The chart's default `curie.fullname` rule, followed by the dispatcher
/// suffix from `templates/dispatcher.yaml`. The guided flow only knows the
/// chosen release, so it intentionally uses the chart's no-override path.
///
/// The rule itself lives in `ops::chart_fullname` (#1533); this delegates
/// rather than carrying a second copy of it. Deliberately offline: the guided
/// troubleshooting preview prints suggested commands without contacting a
/// cluster, so it never calls `ops::release_fullname`. The printed name
/// therefore assumes a no-override install -- under `nameOverride` or
/// `fullnameOverride` the real Deployment is named differently.
fn chart_dispatcher_deployment_name(release: &str) -> String {
    crate::ops::chart_fullname(release).resource("dispatcher")
}

fn ensure_secret_available(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    name: &str,
) -> Result<()> {
    let recovery = format!(
        "{name} is required for this workflow. Save it with `curie secrets set {name}` \
         or `export {name}=...`, then retry"
    );
    if std::env::var_os(name).is_some() {
        return Ok(());
    }
    if crate::secrets::is_saved(name)? {
        return Ok(());
    }
    if crate::secrets::needs_vault_upgrade(name)? && crate::secrets::migrate_legacy_value(name)? {
        return Ok(());
    }
    let Some(save) = prompt_select(
        terminal,
        app,
        "Missing Credential",
        &format!("{name} is required"),
        vec![
            SelectChoice {
                label: "Save it now".to_string(),
                description: "Store it in Curie private storage".to_string(),
                value: true,
            },
            SelectChoice {
                label: "Cancel workflow".to_string(),
                description: "Return to Curie without running".to_string(),
                value: false,
            },
        ],
    )?
    else {
        anyhow::bail!("{recovery}");
    };
    if !save {
        anyhow::bail!("{recovery}");
    }
    let value = prompt_checked(terminal, app, "Save Secret", name, &recovery, true, |v| {
        crate::credcheck::check_secret(name, v)
    })?;
    crate::secrets::save_value(name, &value)
}

fn ensure_model_credential_available(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
) -> Result<()> {
    const RECOVERY: &str =
        "A model credential is required. `CURIE_MODEL_BASE_URL` selects the endpoint. Use \
        `curie secrets set <NAME>` or `export <NAME>=...`, then retry";
    for name in MODEL_CREDENTIAL_NAMES {
        if env_credential_present(name) {
            return Ok(());
        }
    }
    for name in MODEL_CREDENTIAL_NAMES {
        if crate::secrets::is_saved(name)? {
            return Ok(());
        }
    }
    let legacy_names = MODEL_CREDENTIAL_NAMES
        .iter()
        .filter_map(|name| {
            crate::secrets::needs_vault_upgrade(name)
                .ok()
                .filter(|needed| *needed)
                .map(|_| (*name).to_string())
        })
        .collect::<Vec<_>>();
    if let Some(name) = legacy_names.first() {
        if crate::secrets::migrate_legacy_value(name)? {
            return Ok(());
        }
    }

    let choices = MODEL_CREDENTIAL_NAMES
        .iter()
        .map(|name| SelectChoice {
            label: (*name).to_string(),
            description: "Supported by the Curie Claude SDK runner".to_string(),
            value: (*name).to_string(),
        })
        .collect();
    let Some(name) = prompt_select(
        terminal,
        app,
        "Missing Model Credential",
        "Choose a model credential to save",
        choices,
    )?
    else {
        anyhow::bail!(RECOVERY);
    };
    let value = prompt_checked(
        terminal,
        app,
        "Save Model Credential",
        &name,
        RECOVERY,
        true,
        crate::credcheck::check_model_credential,
    )?;
    crate::secrets::save_value(&name, &value)
}

fn model_credential_status(
    saved_names: &BTreeSet<String>,
    legacy_names: &BTreeSet<String>,
) -> &'static str {
    for name in MODEL_CREDENTIAL_NAMES.iter().copied() {
        if env_credential_present(name) || saved_names.contains(name) {
            return "available";
        }
        if legacy_names.contains(name) {
            return "saved (upgrade needed)";
        }
    }
    "missing"
}

fn secrets_status_lines() -> Vec<Line<'static>> {
    let indexed_names = match crate::secrets::list_names() {
        Ok(names) => names,
        Err(err) => {
            return vec![Line::from(format!(
                "Unable to read saved credential names: {err:#}"
            ))];
        }
    };
    let saved_names = indexed_names
        .iter()
        .filter(|name| crate::secrets::is_saved(name).unwrap_or(false))
        .cloned()
        .collect::<BTreeSet<_>>();
    let legacy_names = indexed_names
        .into_iter()
        .filter(|name| !saved_names.contains(name))
        .collect::<BTreeSet<_>>();
    vec![Line::from(format!(
        "Model credential: {}",
        model_credential_status(&saved_names, &legacy_names)
    ))]
}

fn maybe_add_secret_status(lines: &mut Vec<Line<'static>>, workflow: Workflow) {
    match workflow {
        Workflow::ExploreExamples | Workflow::DeployToSlack => {
            lines.push(Line::from(Span::styled(
                "Credential status",
                Style::default().fg(Color::Yellow).bold(),
            )));
            lines.extend(secrets_status_lines());
            lines.push(Line::from(""));
        }
        // The parity-ladder explainer needs no credential status.
        Workflow::ParityLadder => {}
    }
}

fn find_repo_root(mut dir: PathBuf) -> Option<PathBuf> {
    loop {
        if dir.join("runner/Dockerfile").is_file() && dir.join("pyproject.toml").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

#[derive(Debug)]
struct RunView {
    title: String,
    lines: Vec<String>,
    scroll: u16,
    follow: bool,
    running: bool,
}

impl RunView {
    fn new(title: &str) -> Self {
        Self {
            title: title.to_string(),
            lines: Vec::new(),
            scroll: 0,
            follow: true,
            running: true,
        }
    }

    fn push(&mut self, line: impl Into<String>) {
        self.lines.push(line.into());
    }

    fn follow_tail(&mut self, terminal_width: u16, terminal_height: u16) {
        if self.follow {
            self.scroll = self.max_scroll(terminal_width, terminal_height);
        }
    }

    fn max_scroll(&self, terminal_width: u16, terminal_height: u16) -> u16 {
        let (output_width, viewport) = run_view_dimensions(terminal_width, terminal_height);
        wrap_output_lines(&self.lines, output_width)
            .len()
            .saturating_sub(viewport)
            .min(u16::MAX as usize) as u16
    }

    fn scroll_up(&mut self, amount: u16, terminal_width: u16, terminal_height: u16) {
        if self.follow {
            self.scroll = self.max_scroll(terminal_width, terminal_height);
        }
        self.follow = false;
        self.scroll = self.scroll.saturating_sub(amount);
    }

    fn scroll_down(&mut self, amount: u16, terminal_width: u16, terminal_height: u16) {
        self.follow = false;
        self.scroll = self
            .scroll
            .saturating_add(amount)
            .min(self.max_scroll(terminal_width, terminal_height));
    }
}

#[derive(Debug)]
struct ChatView {
    agent_name: String,
    lines: Vec<String>,
    input: String,
    scroll: u16,
    follow: bool,
    thinking: bool,
    actions: Vec<TerminalAction>,
    action_idx: usize,
    action_prompt: String,
    allow_free_text: bool,
    composing: bool,
}

impl ChatView {
    fn new(agent_name: &str, suggestions: &[String]) -> Self {
        Self {
            agent_name: agent_name.to_string(),
            lines: vec![
                format!("{agent_name} is ready."),
                "Send a message to interact with this example agent.".to_string(),
                String::new(),
            ],
            input: String::new(),
            scroll: 0,
            follow: true,
            thinking: false,
            actions: suggestions
                .iter()
                .map(|value| TerminalAction {
                    label: value.clone(),
                    value: value.clone(),
                })
                .collect(),
            action_idx: 0,
            action_prompt: "Try a prompt".to_string(),
            allow_free_text: true,
            composing: false,
        }
    }

    fn max_scroll(&self, terminal_width: u16, terminal_height: u16) -> u16 {
        let (width, viewport) = chat_transcript_dimensions(self, terminal_width, terminal_height);
        wrap_output_lines(&self.lines, width)
            .len()
            .saturating_sub(viewport)
            .min(u16::MAX as usize) as u16
    }

    fn follow_tail(&mut self, terminal_width: u16, terminal_height: u16) {
        if self.follow {
            self.scroll = self.max_scroll(terminal_width, terminal_height);
        }
    }

    fn choice_count(&self) -> usize {
        self.actions.len() + usize::from(self.allow_free_text)
    }

    fn free_text_selected(&self) -> bool {
        self.allow_free_text && self.action_idx == self.actions.len()
    }

    fn move_selection(&mut self, delta: isize) {
        let count = self.choice_count();
        if count > 0 {
            self.action_idx =
                ((self.action_idx as isize + delta).rem_euclid(count as isize)) as usize;
        }
    }

    fn scroll_up(&mut self, amount: u16, terminal_width: u16, terminal_height: u16) {
        if self.follow {
            self.scroll = self.max_scroll(terminal_width, terminal_height);
        }
        self.follow = false;
        self.scroll = self.scroll.saturating_sub(amount);
    }

    fn scroll_down(&mut self, amount: u16, terminal_width: u16, terminal_height: u16) {
        self.follow = false;
        self.scroll = self
            .scroll
            .saturating_add(amount)
            .min(self.max_scroll(terminal_width, terminal_height));
    }
}

fn chat_with_runner(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    cwd: &Path,
    url: &str,
    agent_name: &str,
    suggestions: &[String],
) -> Result<()> {
    let mut chat = ChatView::new(agent_name, suggestions);
    loop {
        let Some(message) = read_chat_input(terminal, &mut chat)? else {
            return Ok(());
        };
        chat.lines.push("You".to_string());
        chat.lines.push(message.clone());
        chat.lines.push(String::new());
        chat.lines.push("Agent".to_string());
        chat.thinking = true;
        chat.follow = true;
        chat.actions.clear();
        chat.action_idx = 0;
        chat.action_prompt = "Responses".to_string();
        chat.allow_free_text = true;
        chat.composing = false;
        let argv = [
            "skill".to_string(),
            "message".to_string(),
            message,
            "--url".to_string(),
            url.to_string(),
        ];
        run_chat_turn(terminal, cwd, &argv, &mut chat)?;
        chat.thinking = false;
        chat.lines.push(String::new());
    }
}

fn read_chat_input(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    chat: &mut ChatView,
) -> Result<Option<String>> {
    chat.input.clear();
    loop {
        let size = terminal.size()?;
        chat.follow_tail(size.width, size.height);
        terminal.draw(|frame| draw_chat_view(frame, chat))?;
        let event = event::read()?;
        if let Event::Mouse(mouse) = event {
            match mouse.kind {
                MouseEventKind::ScrollUp => chat.scroll_up(3, size.width, size.height),
                MouseEventKind::ScrollDown => chat.scroll_down(3, size.width, size.height),
                _ => {}
            }
            continue;
        }
        let Event::Key(key) = event else { continue };
        match (key.code, key.modifiers) {
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(None),
            (KeyCode::Esc, _) if chat.composing => chat.composing = false,
            (KeyCode::Esc, _) => return Ok(None),
            (KeyCode::Enter, _) if chat.composing && !chat.input.trim().is_empty() => {
                return Ok(Some(chat.input.trim().to_string()));
            }
            (KeyCode::Enter, _) => {
                if chat.free_text_selected() {
                    chat.composing = true;
                } else if let Some(action) = chat.actions.get(chat.action_idx) {
                    return Ok(Some(action.value.clone()));
                }
            }
            (KeyCode::Backspace, _) if chat.composing => {
                chat.input.pop();
            }
            (KeyCode::Up, _) if chat.composing => {
                chat.scroll_up(1, size.width, size.height);
            }
            (KeyCode::Down, _) if chat.composing => {
                chat.scroll_down(1, size.width, size.height);
            }
            (KeyCode::PageUp, _) => chat.scroll_up(10, size.width, size.height),
            (KeyCode::PageDown, _) => chat.scroll_down(10, size.width, size.height),
            (KeyCode::Home, _) if !chat.composing => {
                chat.follow = false;
                chat.scroll = 0;
            }
            (KeyCode::End, _) if !chat.composing => chat.follow = true,
            (KeyCode::Down | KeyCode::Tab, _) if !chat.composing => {
                chat.move_selection(1);
            }
            (KeyCode::Up | KeyCode::BackTab, _) if !chat.composing => {
                chat.move_selection(-1);
            }
            (KeyCode::Char(ch), _)
                if chat.composing && !key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                chat.input.push(ch);
            }
            _ => {}
        }
    }
}

fn run_chat_turn(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    cwd: &Path,
    argv: &[String],
    chat: &mut ChatView,
) -> Result<()> {
    let mut child = Command::new(std::env::current_exe().context("resolving current executable")?)
        .args(argv)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("sending message to the example agent")?;
    let (tx, rx) = mpsc::channel();
    let stdout = child.stdout.take().context("capturing agent response")?;
    let stderr = child.stderr.take().context("capturing agent diagnostics")?;
    let readers = [stdout_reader(stdout, tx.clone()), stderr_reader(stderr, tx)];
    let mut canceled = false;
    let mut envelope = Vec::new();

    loop {
        while let Ok(line) = rx.try_recv() {
            consume_chat_line(chat, &mut envelope, line);
        }
        let size = terminal.size()?;
        chat.follow_tail(size.width, size.height);
        terminal.draw(|frame| draw_chat_view(frame, chat))?;
        if event::poll(Duration::from_millis(50))? {
            let input_event = event::read()?;
            if let Event::Mouse(mouse) = input_event {
                match mouse.kind {
                    MouseEventKind::ScrollUp => chat.scroll_up(3, size.width, size.height),
                    MouseEventKind::ScrollDown => chat.scroll_down(3, size.width, size.height),
                    _ => {}
                }
            } else if let Event::Key(key) = input_event {
                match (key.code, key.modifiers) {
                    (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                        child.kill().context("canceling agent response")?;
                        canceled = true;
                    }
                    (KeyCode::Up, _) => chat.scroll_up(1, size.width, size.height),
                    (KeyCode::Down, _) => chat.scroll_down(1, size.width, size.height),
                    (KeyCode::PageUp, _) => chat.scroll_up(10, size.width, size.height),
                    (KeyCode::PageDown, _) => chat.scroll_down(10, size.width, size.height),
                    (KeyCode::End, _) => chat.follow = true,
                    _ => {}
                }
            }
        }
        if let Some(status) = child.try_wait().context("waiting for agent response")? {
            for reader in readers {
                let _ = reader.join();
            }
            while let Ok(line) = rx.try_recv() {
                consume_chat_line(chat, &mut envelope, line);
            }
            if canceled {
                chat.lines.push("Response canceled.".to_string());
                return Ok(());
            }
            if !status.success() {
                anyhow::bail!("agent response exited with {status}");
            }
            return Ok(());
        }
    }
}

fn consume_chat_line(chat: &mut ChatView, envelope: &mut Vec<String>, line: String) {
    if !envelope.is_empty() {
        envelope.push(line);
        if reply_envelope_complete(&envelope.join("\n")) {
            let raw = envelope.join("\n");
            if let Some(message) = parse_terminal_message(&raw) {
                chat.lines.extend(message.lines);
                chat.actions = message.actions;
                chat.action_idx = 0;
                chat.action_prompt = message
                    .action_prompt
                    .unwrap_or_else(|| "Choose a response".to_string());
                chat.allow_free_text = message.allow_free_text;
                chat.composing = false;
            } else if let Some((fallback, _)) = raw.split_once(REPLY_FENCE) {
                chat.lines.extend(
                    fallback
                        .trim()
                        .lines()
                        .filter(|line| !line.is_empty())
                        .map(str::to_string),
                );
            }
            envelope.clear();
        }
        return;
    }
    if line.contains(REPLY_FENCE) {
        envelope.push(line);
    } else if !is_internal_chat_status(&line) {
        chat.lines.push(line);
    }
}

fn reply_envelope_complete(text: &str) -> bool {
    text.find(REPLY_FENCE)
        .and_then(|start| text.get(start + REPLY_FENCE.len()..))
        .is_some_and(|rest| rest.contains("```"))
}

fn is_internal_chat_status(line: &str) -> bool {
    line.trim().starts_with("-- final (")
}

fn run_curie_in_tui(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    argv: &[String],
    cwd: &Path,
    view: &mut RunView,
) -> Result<()> {
    run_curie_in_tui_with_env(terminal, app, argv, cwd, view, &[])
}

fn run_curie_in_tui_with_env(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    argv: &[String],
    cwd: &Path,
    view: &mut RunView,
    command_env: &[(String, String)],
) -> Result<()> {
    view.push(format!("$ {}", render_command(argv)));
    view.push("");
    view.running = true;
    let mut child = Command::new(std::env::current_exe().context("resolving current executable")?)
        .args(argv)
        .current_dir(cwd)
        .envs(command_env.iter().cloned())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("running {}", render_command(argv)))?;

    let (tx, rx) = mpsc::channel();
    let stdout = child.stdout.take().context("capturing curie stdout")?;
    let stderr = child.stderr.take().context("capturing curie stderr")?;
    let readers = [stdout_reader(stdout, tx.clone()), stderr_reader(stderr, tx)];
    let mut canceled = false;

    let status_result = (|| -> Result<std::process::ExitStatus> {
        loop {
            while let Ok(line) = rx.try_recv() {
                view.push(line);
            }
            let size = terminal.size()?;
            view.follow_tail(size.width, size.height);
            terminal.draw(|frame| {
                draw(frame, app);
                draw_run_view(frame, view);
            })?;

            if event::poll(Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    match (key.code, key.modifiers) {
                        (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                            child.kill().context("canceling curie command")?;
                            canceled = true;
                        }
                        (KeyCode::Up | KeyCode::Char('k'), _) => {
                            view.scroll_up(1, size.width, size.height);
                        }
                        (KeyCode::Down | KeyCode::Char('j'), _) => {
                            view.scroll_down(1, size.width, size.height);
                        }
                        (KeyCode::End, _) => view.follow = true,
                        _ => {}
                    }
                }
            }
            if let Some(status) = child.try_wait().context("waiting for curie command")? {
                break Ok(status);
            }
        }
    })();

    if status_result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }

    for reader in readers {
        let _ = reader.join();
    }
    while let Ok(line) = rx.try_recv() {
        view.push(line);
    }
    view.running = false;
    view.push("");
    let status = status_result?;
    if canceled {
        view.push("Canceled by user.");
        anyhow::bail!("{} was canceled", render_command(argv));
    }
    if !status.success() {
        view.push(format!("Exited with {status}."));
        anyhow::bail!("{} exited with {status}", render_command(argv));
    }
    view.push("Completed successfully.");
    view.push("");
    Ok(())
}

fn stdout_reader(
    stdout: std::process::ChildStdout,
    tx: mpsc::Sender<String>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || read_output(stdout, tx))
}

fn stderr_reader(
    stderr: std::process::ChildStderr,
    tx: mpsc::Sender<String>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || read_output(stderr, tx))
}

fn read_output(reader: impl io::Read, tx: mpsc::Sender<String>) {
    for line in BufReader::new(reader).lines() {
        match line {
            Ok(line) => {
                if tx.send(line).is_err() {
                    break;
                }
            }
            Err(err) => {
                let _ = tx.send(format!("Unable to read command output: {err}"));
                break;
            }
        }
    }
}

fn show_run_view(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &App,
    view: &mut RunView,
) -> Result<()> {
    view.running = false;
    loop {
        let size = terminal.size()?;
        view.follow_tail(size.width, size.height);
        terminal.draw(|frame| {
            draw(frame, app);
            draw_run_view(frame, view);
        })?;
        let Event::Key(key) = event::read()? else {
            continue;
        };
        match (key.code, key.modifiers) {
            (KeyCode::Enter | KeyCode::Esc | KeyCode::Char('q'), _) => return Ok(()),
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(()),
            (KeyCode::Up | KeyCode::Char('k'), _) => {
                view.scroll_up(1, size.width, size.height);
            }
            (KeyCode::Down | KeyCode::Char('j'), _) => {
                view.scroll_down(1, size.width, size.height);
            }
            (KeyCode::PageUp, _) => {
                view.scroll_up(10, size.width, size.height);
            }
            (KeyCode::PageDown, _) => {
                view.scroll_down(10, size.width, size.height);
            }
            (KeyCode::Home, _) => {
                view.follow = false;
                view.scroll = 0;
            }
            (KeyCode::End, _) => view.follow = true,
            _ => {}
        }
    }
}

fn render_command(argv: &[String]) -> String {
    std::iter::once("curie".to_string())
        .chain(argv.iter().map(|arg| crate::ops::shell_quote(arg)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn draw(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(12),
            Constraint::Length(3),
        ])
        .split(area);

    draw_header(frame, layout[0], app);
    draw_body(frame, layout[1], app);
    draw_footer(frame, layout[2], app);
}

fn draw_header(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let title = Line::from(vec![
        Span::styled("Curie", Style::default().fg(Color::Cyan).bold()),
        Span::raw(" interactive  "),
        Span::styled(
            format!("target: {}", app.targets[app.target_idx]),
            Style::default().fg(Color::Yellow),
        ),
    ]);
    frame.render_widget(
        Paragraph::new(title)
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn draw_body(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(18),
            Constraint::Percentage(36),
            Constraint::Percentage(64),
        ])
        .split(area);
    draw_targets(frame, chunks[0], app);
    draw_actions(frame, chunks[1], app);
    draw_detail(frame, chunks[2], app);
}

fn draw_targets(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let items: Vec<ListItem<'_>> = app
        .targets
        .iter()
        .map(|target| ListItem::new(Line::from(*target)))
        .collect();
    let mut state = ListState::default();
    state.select(Some(app.target_idx));
    frame.render_stateful_widget(
        List::new(items)
            .block(Block::default().title("Targets").borders(Borders::ALL))
            .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan)),
        area,
        &mut state,
    );
}

fn draw_actions(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let visible = app.visible_indices();
    let items: Vec<ListItem<'_>> = visible
        .iter()
        .filter_map(|idx| app.recipes.get(*idx))
        .map(|recipe| {
            ListItem::new(vec![
                Line::from(Span::styled(recipe.title, Style::default().bold())),
                Line::from(Span::styled(
                    recipe.description,
                    Style::default().fg(Color::Gray),
                )),
            ])
        })
        .collect();
    let mut state = ListState::default();
    if !items.is_empty() {
        state.select(Some(app.selected.min(items.len() - 1)));
    }
    frame.render_stateful_widget(
        List::new(items)
            .block(Block::default().title("Actions").borders(Borders::ALL))
            .highlight_style(Style::default().fg(Color::Black).bg(Color::Green)),
        area,
        &mut state,
    );
}

fn draw_detail(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(recipe) = app.selected_recipe() else {
        frame.render_widget(Clear, area);
        return;
    };
    let values: BTreeMap<String, String> = recipe
        .fields
        .iter()
        .filter_map(|field| {
            field
                .default
                .map(|value| (field.key.to_string(), value.to_string()))
        })
        .collect();
    let mut lines = vec![
        Line::from(Span::styled(recipe.title, Style::default().bold())),
        Line::from(""),
        Line::from(recipe.description),
        Line::from(""),
    ];
    match &recipe.kind {
        RecipeKind::Command => {
            let preview = build_argv(recipe, None, &values);
            lines.push(Line::from(Span::styled(
                "Command preview",
                Style::default().fg(Color::Yellow).bold(),
            )));
            lines.push(Line::from(render_command(&preview)));
            lines.push(Line::from(""));
        }
        RecipeKind::Tui(TuiAction::SaveSecret) => {
            lines.push(Line::from(Span::styled(
                "TUI prompts",
                Style::default().fg(Color::Yellow).bold(),
            )));
            lines.push(Line::from("1. Choose a common secret name or Custom"));
            lines.push(Line::from("2. Secret value, hidden while typing"));
            lines.push(Line::from("3. Save to Curie private storage"));
            lines.push(Line::from(""));
        }
        RecipeKind::Tui(TuiAction::ListSecrets) => {
            lines.push(Line::from(Span::styled(
                "Saved secrets",
                Style::default().fg(Color::Yellow).bold(),
            )));
            match crate::secrets::list_names() {
                Ok(names) if names.is_empty() => {
                    lines.push(Line::from("No Curie secrets saved."));
                }
                Ok(names) => {
                    for name in names {
                        lines.push(Line::from(format!("{name}  (value hidden)")));
                    }
                }
                Err(err) => {
                    lines.push(Line::from(format!("Unable to read secret index: {err:#}")));
                }
            }
            lines.push(Line::from(""));
        }
        RecipeKind::Tui(TuiAction::RemoveSecret) => {
            lines.push(Line::from(Span::styled(
                "TUI prompts",
                Style::default().fg(Color::Yellow).bold(),
            )));
            lines.push(Line::from("1. Choose a saved secret name or Custom"));
            lines.push(Line::from("2. Type the same name to confirm removal"));
            lines.push(Line::from("3. Remove from Curie private storage"));
            lines.push(Line::from(""));
        }
        RecipeKind::Workflow(Workflow::ExploreExamples) => {
            lines.push(Line::from(Span::styled(
                "Example browser",
                Style::default().fg(Color::Yellow).bold(),
            )));
            lines.push(Line::from("1. Choose an example agent"));
            lines.push(Line::from("2. Start its runner and required tools"));
            lines.push(Line::from("3. Chat for as many turns as needed"));
            lines.push(Line::from("4. Stop the runner when you leave chat"));
            lines.push(Line::from(""));
            maybe_add_secret_status(&mut lines, Workflow::ExploreExamples);
        }
        RecipeKind::Workflow(Workflow::ParityLadder) => {
            lines.push(Line::from(Span::styled(
                "The parity ladder",
                Style::default().fg(Color::Yellow).bold(),
            )));
            lines.push(Line::from("What curie is: one immutable bundle + one"));
            lines.push(Line::from(
                "evals/cases.json, run identically across tiers.",
            ));
            lines.push(Line::from(
                "skill (runner only) -> local (full platform) ->",
            ));
            lines.push(Line::from("cluster (Kubernetes). A tier-to-tier eval"));
            lines.push(Line::from(
                "divergence is the harness catching a deploy bug.",
            ));
            lines.push(Line::from(""));
        }
        RecipeKind::Workflow(Workflow::DeployToSlack) => {
            lines.push(Line::from(Span::styled(
                "How to deploy to Slack",
                Style::default().fg(Color::Yellow).bold(),
            )));
            lines.push(Line::from("1. Choose the tier (local or cluster)"));
            lines.push(Line::from(
                "2. Create a Slack app from the repo manifest (one time)",
            ));
            lines.push(Line::from(
                "3. Save your app + bot tokens and the channel ID",
            ));
            lines.push(Line::from("4. <tier> deploy -> <tier> comms --slack"));
            lines.push(Line::from("5. @mention the bot in Slack to test"));
            lines.push(Line::from(""));
            maybe_add_secret_status(&mut lines, Workflow::DeployToSlack);
        }
    }
    if !recipe.fields.is_empty() {
        lines.push(Line::from(Span::styled(
            "Prompts before run",
            Style::default().fg(Color::Yellow).bold(),
        )));
        for field in &recipe.fields {
            let default = field.default.unwrap_or(if field.required {
                "required"
            } else {
                "optional"
            });
            lines.push(Line::from(format!("{}: {default}", field.label)));
        }
        lines.push(Line::from(""));
    }
    if !recipe.notes.is_empty() {
        lines.push(Line::from(Span::styled(
            "Notes",
            Style::default().fg(Color::Yellow).bold(),
        )));
        for note in recipe.notes {
            lines.push(Line::from(format!("- {note}")));
        }
    }
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false })
            .block(Block::default().title("Details").borders(Borders::ALL)),
        area,
    );
}

fn draw_footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let text = Line::from(vec![
        Span::styled("Up/Down", Style::default().bold()),
        Span::raw(" action  "),
        Span::styled("Tab/Left/Right", Style::default().bold()),
        Span::raw(" target  "),
        Span::styled("Enter/r", Style::default().bold()),
        Span::raw(" run  "),
        Span::styled("q/Esc", Style::default().bold()),
        Span::raw(" quit    "),
        Span::styled(&app.message, Style::default().fg(Color::Gray)),
    ]);
    frame.render_widget(
        Paragraph::new(text)
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn draw_run_view(frame: &mut Frame<'_>, view: &RunView) {
    let frame_area = frame.area();
    let area = Rect {
        x: frame_area.x.saturating_add(2),
        y: frame_area.y.saturating_add(1),
        width: frame_area.width.saturating_sub(4).max(20),
        height: frame_area.height.saturating_sub(2).max(7),
    };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(2)])
        .split(area);
    let status = if view.running { "running" } else { "finished" };
    let output_width = chunks[0].width.saturating_sub(2).max(1) as usize;
    let wrapped_lines = wrap_output_lines(&view.lines, output_width);
    let lines = wrapped_lines
        .iter()
        .map(|line| Line::from(line.as_str()))
        .collect::<Vec<_>>();
    let viewport = chunks[0].height.saturating_sub(2) as usize;
    let max_scroll = wrapped_lines
        .len()
        .saturating_sub(viewport)
        .min(u16::MAX as usize) as u16;

    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .scroll((view.scroll.min(max_scroll), 0))
            .block(
                Block::default()
                    .title(format!("{} · {status}", view.title))
                    .borders(Borders::ALL),
            ),
        chunks[0],
    );
    let help = if view.running {
        "Up/Down scroll    End follow output    Ctrl-C cancel"
    } else {
        "Up/Down or PgUp/PgDn scroll    Home/End jump    Enter return"
    };
    frame.render_widget(
        Paragraph::new(Span::styled(help, Style::default().fg(Color::Gray)))
            .alignment(Alignment::Center),
        chunks[1],
    );
}

fn draw_chat_view(frame: &mut Frame<'_>, chat: &ChatView) {
    let area = frame.area();
    let (action_height, input_height) = chat_panel_heights(chat);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(action_height),
            Constraint::Length(input_height),
            Constraint::Length(2),
        ])
        .split(area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Curie", Style::default().fg(Color::Cyan).bold()),
            Span::raw(format!("  {}", chat.agent_name)),
            if chat.thinking {
                Span::styled("  thinking...", Style::default().fg(Color::Yellow))
            } else {
                Span::raw("")
            },
        ]))
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL)),
        chunks[0],
    );

    let output_width = chunks[1].width.saturating_sub(2).max(1) as usize;
    let wrapped = wrap_output_lines(&chat.lines, output_width);
    let transcript = wrapped
        .iter()
        .map(|line| {
            let style = match line.as_str() {
                "You" => Style::default().fg(Color::Cyan).bold(),
                "Agent" => Style::default().fg(Color::Green).bold(),
                _ => Style::default(),
            };
            Line::from(Span::styled(line.as_str(), style))
        })
        .collect::<Vec<_>>();
    let viewport = chunks[1].height.saturating_sub(2) as usize;
    let max_scroll = wrapped
        .len()
        .saturating_sub(viewport)
        .min(u16::MAX as usize) as u16;
    frame.render_widget(
        Paragraph::new(Text::from(transcript))
            .scroll((chat.scroll.min(max_scroll), 0))
            .block(Block::default().title("Conversation").borders(Borders::ALL)),
        chunks[1],
    );

    let actions = chat
        .actions
        .iter()
        .map(|action| action.label.as_str())
        .chain(chat.allow_free_text.then_some("Type a message..."))
        .enumerate()
        .map(|(idx, label)| {
            let prefix = if idx == chat.action_idx { "> " } else { "  " };
            ListItem::new(format!("{prefix}{}. {label}", idx + 1))
        })
        .collect::<Vec<_>>();
    let mut action_state = ListState::default();
    action_state.select((chat.choice_count() > 0).then_some(chat.action_idx));
    if action_height > 0 {
        frame.render_stateful_widget(
            List::new(actions)
                .block(
                    Block::default()
                        .title(chat.action_prompt.as_str())
                        .borders(Borders::ALL),
                )
                .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan)),
            chunks[2],
            &mut action_state,
        );
    }

    let input_width = chunks[3].width.saturating_sub(4).max(1) as usize;
    let shown_input = input_window(&chat.input, false, input_width);
    if input_height > 0 {
        frame.render_widget(
            Paragraph::new(Span::raw(shown_input.as_str()))
                .block(Block::default().title("Message").borders(Borders::ALL)),
            chunks[3],
        );
        frame.set_cursor_position((
            chunks[3].x + 1 + UnicodeWidthStr::width(shown_input.as_str()) as u16,
            chunks[3].y + 1,
        ));
    }
    let help = if chat.thinking {
        "Up/Down or wheel scroll    PgUp/PgDn page    End latest    Ctrl-C cancel"
    } else if chat.composing {
        "Type message    Enter send    Up/Down or wheel scroll    Esc responses"
    } else {
        "Up/Down choose    Enter select    PgUp/PgDn or wheel scroll    Esc leave"
    };
    frame.render_widget(
        Paragraph::new(Span::styled(help, Style::default().fg(Color::Gray)))
            .alignment(Alignment::Center),
        chunks[4],
    );
}

fn chat_panel_heights(chat: &ChatView) -> (u16, u16) {
    if chat.thinking {
        return (0, 0);
    }
    let choices = chat.choice_count().min(usize::from(u16::MAX)) as u16;
    let action_height = if choices > 0 {
        choices.saturating_add(2)
    } else {
        0
    };
    let input_height = if chat.composing { 3 } else { 0 };
    (action_height, input_height)
}

fn chat_transcript_dimensions(
    chat: &ChatView,
    terminal_width: u16,
    terminal_height: u16,
) -> (usize, usize) {
    let (action_height, input_height) = chat_panel_heights(chat);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(action_height),
            Constraint::Length(input_height),
            Constraint::Length(2),
        ])
        .split(Rect::new(0, 0, terminal_width, terminal_height));
    (
        chunks[1].width.saturating_sub(2).max(1) as usize,
        chunks[1].height.saturating_sub(2) as usize,
    )
}

fn run_view_dimensions(terminal_width: u16, terminal_height: u16) -> (usize, usize) {
    let area_width = terminal_width.saturating_sub(4).max(20);
    let area_height = terminal_height.saturating_sub(2).max(7);
    let body_height = area_height.saturating_sub(2);
    (
        area_width.saturating_sub(2).max(1) as usize,
        body_height.saturating_sub(2) as usize,
    )
}

fn wrap_output_lines(lines: &[String], max_width: usize) -> Vec<String> {
    let mut wrapped = Vec::new();
    for line in lines {
        if line.is_empty() {
            wrapped.push(String::new());
            continue;
        }
        let mut current = String::new();
        let mut width = 0;
        for ch in line.chars() {
            let char_width = UnicodeWidthChar::width(ch).unwrap_or(0);
            if !current.is_empty() && width + char_width > max_width {
                wrapped.push(current);
                current = String::new();
                width = 0;
            }
            current.push(ch);
            width += char_width;
        }
        wrapped.push(current);
    }
    wrapped
}

fn draw_prompt(
    frame: &mut Frame<'_>,
    title: &str,
    label: &str,
    default: Option<&str>,
    value: &str,
    secret: bool,
    allow_empty: bool,
) {
    let area = centered_rect(64, 9, frame.area());
    let input_width = area.width.saturating_sub(3) as usize;
    let shown_value = input_window(value, secret, input_width);
    let guidance = match default {
        Some(default) => format!("Default: {default}"),
        None if secret => "Input is hidden while typing".to_string(),
        None if allow_empty => "Optional: press Enter to skip".to_string(),
        None => "Type a value".to_string(),
    };
    let submit_help = if default.is_some() {
        "Enter use default    Esc cancel"
    } else if allow_empty {
        "Enter accept or skip    Esc cancel"
    } else {
        "Enter accept    Esc cancel"
    };
    let body = Text::from(vec![
        Line::from(Span::styled(label, Style::default().bold())),
        Line::from(Span::styled(guidance, Style::default().fg(Color::Gray))),
        Line::from(if shown_value.is_empty() {
            Span::styled(" ", Style::default().fg(Color::Gray))
        } else {
            Span::raw(&shown_value)
        }),
        Line::from(""),
        Line::from(Span::styled(submit_help, Style::default().fg(Color::Gray))),
    ]);
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(body)
            .wrap(Wrap { trim: false })
            .block(Block::default().title(title).borders(Borders::ALL)),
        area,
    );
    frame.set_cursor_position((
        area.x + 1 + UnicodeWidthStr::width(shown_value.as_str()) as u16,
        area.y + 3,
    ));
}

fn input_window(value: &str, secret: bool, max_width: usize) -> String {
    if secret {
        return "*".repeat(value.chars().count().min(max_width));
    }

    let mut width = 0;
    let mut chars = Vec::new();
    for ch in value.chars().rev() {
        let char_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if width + char_width > max_width {
            break;
        }
        width += char_width;
        chars.push(ch);
    }
    chars.into_iter().rev().collect()
}

fn draw_select_prompt<T>(
    frame: &mut Frame<'_>,
    title: &str,
    label: &str,
    choices: &[SelectChoice<T>],
    selected: usize,
) {
    let height = (choices.len() as u16)
        .saturating_mul(2)
        .saturating_add(6)
        .min(18);
    let area = centered_rect(74, height, frame.area());
    let mut lines = vec![
        Line::from(label.to_string()),
        Line::from(Span::styled(
            "Up/Down move    Enter choose    Esc cancel",
            Style::default().fg(Color::Gray),
        )),
        Line::from(""),
    ];
    for (idx, choice) in choices.iter().enumerate() {
        let focused = idx == selected;
        let marker = if focused { ">" } else { " " };
        let style = if focused {
            Style::default().fg(Color::Black).bg(Color::Green)
        } else {
            Style::default()
        };
        lines.push(Line::from(Span::styled(
            format!("{marker} {}. {}", idx + 1, choice.label),
            style,
        )));
        lines.push(Line::from(Span::styled(
            format!("     {}", choice.description),
            Style::default().fg(Color::Gray),
        )));
    }
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false })
            .block(Block::default().title(title).borders(Borders::ALL)),
        area,
    );
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width.saturating_sub(2)).max(20);
    let height = height.min(area.height.saturating_sub(2)).max(7);
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_command_quotes_shell_specials() {
        let argv = vec!["skill".into(), "message".into(), "hello world".into()];
        assert_eq!(render_command(&argv), "curie skill message 'hello world'");
    }

    #[test]
    fn render_command_shows_an_empty_field_as_a_quoted_empty_string() {
        // #497: the old interactive shell_quote let an empty arg vanish (vacuous
        // all() over an empty iterator). The canonical ops::shell_quote renders it
        // as '' so the copy-pasteable command keeps the empty positional visible.
        let argv = vec!["skill".into(), "message".into(), "".into()];
        assert_eq!(render_command(&argv), "curie skill message ''");
    }

    #[test]
    fn platform_tab_leads_and_carries_the_primary_functions() {
        let app = App::new();
        // "platform" is the first tab, so it is the default landing view.
        assert_eq!(
            app.targets.first().copied(),
            Some("platform"),
            "platform must lead the tabs"
        );
        // The primary functions are present under it.
        let platform_titles: Vec<&str> = app
            .recipes
            .iter()
            .filter(|r| r.tabs.contains(&"platform"))
            .map(|r| r.title)
            .collect();
        for expected in [
            "Parity ladder (what curie is)",
            "Run evals (parity gate)",
            "Open observability (Console + Langfuse)",
            "List versions",
            "Set budget",
            "Gate a tool (approvals)",
            "Inspect memory",
            "Kill an agent",
            "Resume an agent",
        ] {
            assert!(
                platform_titles.contains(&expected),
                "missing platform recipe: {expected}"
            );
        }
        // And they lead the recipe list (first recipe is on the platform tab).
        assert!(
            app.recipes.first().unwrap().tabs.contains(&"platform"),
            "first recipe must be on the platform tab"
        );
    }

    /// Titles visible when a given tab is selected (drives the real filter).
    fn visible_titles_on(tab: &str) -> Vec<&'static str> {
        let mut app = App::new();
        app.target_idx = app
            .targets
            .iter()
            .position(|t| *t == tab)
            .unwrap_or_else(|| panic!("no such tab: {tab}"));
        app.visible_indices()
            .iter()
            .map(|idx| app.recipes[*idx].title)
            .collect()
    }

    /// A fully-unconfigured operator is told all five, so they know to have the
    /// Slack page open before they start.
    #[test]
    fn a_cold_start_lists_every_prompt() {
        let asks = planned_prompts(true, false, false, false);
        assert_eq!(asks.len(), 5, "{asks:?}");
        assert!(asks.iter().any(|a| a.contains("xapp-")), "{asks:?}");
        assert!(asks.iter().any(|a| a.contains("xoxb-")), "{asks:?}");
    }

    /// The point of the preview: what is already set up is not asked for, and
    /// the operator can see that BEFORE deciding whether they need a browser.
    #[test]
    fn what_is_already_available_is_not_listed() {
        let asks = planned_prompts(true, true, true, true);
        assert_eq!(asks, vec!["the Slack channel id", "the bundle directory"]);
    }

    /// The cluster tier configures the model credential on the release at
    /// `cluster up` time, so this workflow must not ask for it there.
    #[test]
    fn the_cluster_tier_does_not_ask_for_a_model_credential() {
        let asks = planned_prompts(false, false, false, false);
        assert!(
            !asks.iter().any(|a| a.contains("model credential")),
            "{asks:?}"
        );
    }

    /// The closing view has to cover the ways it silently does NOT work, or it
    /// is only useful when nothing went wrong.
    #[test]
    fn the_cluster_close_targets_the_rendered_dispatcher_in_the_chosen_namespace() {
        let namespace = "acme-system";
        let release = "acme-production";
        let chart_name = include_str!("../../charts/curie/Chart.yaml")
            .lines()
            .find_map(|line| line.strip_prefix("name: "))
            .expect("chart name");
        let chart_helpers = include_str!("../../charts/curie/templates/_helpers.tpl");
        assert!(
            chart_helpers.contains("{{- if contains $name .Release.Name -}}"),
            "the expected deployment must follow curie.fullname"
        );
        assert!(
            chart_helpers.contains(
                "{{- printf \"%s-%s\" .Release.Name $name | trunc 63 | trimSuffix \"-\" -}}"
            ),
            "the expected deployment must follow curie.fullname"
        );
        let rendered_dispatcher = format!("{release}-{chart_name}-dispatcher");
        let lines = troubleshooting_lines(Tier::Cluster, Some(namespace), Some(release)).join("\n");
        assert!(
            lines.contains(&format!(
                "kubectl -n {namespace} logs deploy/{rendered_dispatcher}"
            )),
            "{lines}"
        );
        assert!(
            lines.contains(&format!(
                "curie cluster status --namespace {namespace} --release {release}"
            )),
            "{lines}"
        );
        assert!(lines.contains("curie doctor"), "{lines}");
    }

    /// And it must not overclaim. The commands succeeding is not a round trip.
    #[test]
    fn the_close_does_not_claim_a_round_trip_was_proven() {
        let lines = troubleshooting_lines(Tier::Local, None, None).join("\n");
        assert!(
            lines.contains("not the same as a round trip"),
            "must say what was NOT verified: {lines}"
        );
    }

    /// The cluster hint needs a release name; without one it must still render
    /// a usable command rather than an empty deployment reference.
    #[test]
    fn a_missing_release_name_still_renders_a_usable_hint() {
        let lines = troubleshooting_lines(Tier::Cluster, None, None).join("\n");
        assert!(lines.contains("<release>"), "{lines}");
    }

    #[test]
    fn deploy_to_slack_is_a_single_tier_prompting_recipe() {
        let app = App::new();
        // Exactly ONE Deploy-to-Slack recipe (it asks local-vs-cluster at runtime
        // instead of a near-duplicate recipe per tier).
        let matches: Vec<&Recipe> = app
            .recipes
            .iter()
            .filter(|r| r.title == "How to deploy to Slack")
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "there should be exactly one Deploy-to-Slack recipe"
        );
        assert!(matches!(
            matches[0].kind,
            RecipeKind::Workflow(Workflow::DeployToSlack)
        ));
        // Regression (#458): it must be REACHABLE from the operator tier tabs, not
        // hidden on the All tab. Assert reachability through the real tab filter,
        // never a field's string value.
        for tab in ["local", "cluster"] {
            assert!(
                visible_titles_on(tab).contains(&"How to deploy to Slack"),
                "Deploy-to-Slack must be reachable from the {tab} tab"
            );
        }
    }

    #[test]
    fn explore_examples_is_an_action_not_a_target_tab() {
        let app = App::new();
        assert!(!app.targets.contains(&"examples"));
        assert!(app
            .recipes
            .iter()
            .any(|recipe| recipe.title == "Explore examples"));
        let examples = example_choices();
        assert_eq!(examples.len(), 3);
        assert!(examples.iter().all(|example| {
            !example.id.is_empty()
                && example
                    .id
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        }));
    }

    #[test]
    fn chat_hides_internal_final_status() {
        assert!(is_internal_chat_status("-- final (done)"));
        assert!(is_internal_chat_status("  -- final (failed)"));
        assert!(!is_internal_chat_status("The final answer is done."));
    }

    #[test]
    fn chat_appends_free_text_as_the_final_choice() {
        let mut chat = ChatView::new("demo", &["First".to_string(), "Second".to_string()]);
        assert_eq!(chat.choice_count(), 3);
        assert_eq!(chat_panel_heights(&chat), (5, 0));
        assert!(!chat.free_text_selected());

        chat.move_selection(-1);
        assert_eq!(chat.action_idx, 2);
        assert!(chat.free_text_selected());
        chat.composing = true;
        assert_eq!(chat_panel_heights(&chat), (5, 3));

        chat.allow_free_text = false;
        chat.composing = false;
        chat.action_idx = 0;
        assert_eq!(chat.choice_count(), 2);
        assert_eq!(chat_panel_heights(&chat), (4, 0));
        assert!(!chat.free_text_selected());

        chat.thinking = true;
        assert_eq!(chat_panel_heights(&chat), (0, 0));
    }

    #[test]
    fn chat_scroll_up_moves_immediately_from_following_tail() {
        let mut chat = ChatView::new("demo", &[]);
        chat.lines = (0..80).map(|index| format!("line {index}")).collect();
        let max_scroll = chat.max_scroll(80, 24);
        assert!(max_scroll > 0);

        chat.follow = true;
        chat.scroll = 0;
        chat.scroll_up(1, 80, 24);

        assert!(!chat.follow);
        assert_eq!(chat.scroll, max_scroll - 1);
    }

    #[test]
    fn chat_consumes_semantic_choices_without_printing_the_envelope() {
        let mut chat = ChatView::new("demo", &[]);
        let mut envelope = Vec::new();
        consume_chat_line(&mut chat, &mut envelope, "```curie-reply".to_string());
        consume_chat_line(
            &mut chat,
            &mut envelope,
            "{\"version\":\"1.0\",\"text\":\"Pick one\",\"interaction\":{\"kind\":\"choice\",\"id\":\"pick\",\"options\":[{\"label\":\"First\",\"value\":\"first-value\"}]}}".to_string(),
        );
        consume_chat_line(&mut chat, &mut envelope, "```".to_string());

        assert!(envelope.is_empty());
        assert!(chat.lines.iter().any(|line| line == "Pick one"));
        assert!(!chat.lines.iter().any(|line| line.contains("curie-reply")));
        assert_eq!(chat.actions[0].label, "First");
        assert_eq!(chat.actions[0].value, "first-value");
        assert!(chat.allow_free_text);
    }

    #[test]
    fn malformed_envelope_keeps_only_ordinary_text_fallback() {
        let mut chat = ChatView::new("demo", &[]);
        let mut envelope = Vec::new();
        consume_chat_line(
            &mut chat,
            &mut envelope,
            "Still useful\n```curie-reply".to_string(),
        );
        consume_chat_line(&mut chat, &mut envelope, "{broken".to_string());
        consume_chat_line(&mut chat, &mut envelope, "```".to_string());
        assert!(chat.lines.iter().any(|line| line == "Still useful"));
        assert!(!chat.lines.iter().any(|line| line.contains("{broken")));
    }

    #[test]
    fn secret_actions_stay_inside_tui() {
        let app = App::new();
        for (title, action) in [
            ("Save secret", TuiAction::SaveSecret),
            ("List saved secrets", TuiAction::ListSecrets),
            ("Remove secret", TuiAction::RemoveSecret),
        ] {
            let recipe = app
                .recipes
                .iter()
                .find(|recipe| recipe.title == title)
                .unwrap_or_else(|| panic!("{title} recipe exists"));
            assert!(matches!(&recipe.kind, RecipeKind::Tui(actual) if *actual == action));
            assert!(recipe.args.is_empty());
        }
    }

    #[test]
    fn secret_name_choices_include_custom_and_do_not_default_to_github() {
        let choices = secret_name_choices_for(&BTreeSet::new());
        assert!(matches!(
            choices.first().map(|choice| &choice.value),
            Some(SecretNameChoice::Name(name)) if name == "ANTHROPIC_API_KEY"
        ));
        assert!(choices
            .iter()
            .any(|choice| choice.value == SecretNameChoice::Custom));
        assert!(choices
            .iter()
            .any(|choice| matches!(&choice.value, SecretNameChoice::Name(name) if name == "OPENAI_API_KEY")));
        assert!(choices.iter().any(
            |choice| matches!(&choice.value, SecretNameChoice::Name(name) if name == "GITHUB_PERSONAL_ACCESS_TOKEN")
        ));
    }

    #[test]
    fn secret_name_choices_mark_saved_common_keys() {
        let saved = BTreeSet::from(["OPENAI_API_KEY".to_string()]);
        let choices = secret_name_choices_for(&saved);
        let openai = choices
            .iter()
            .find(|choice| {
                matches!(&choice.value, SecretNameChoice::Name(name) if name == "OPENAI_API_KEY")
            })
            .expect("OpenAI choice exists");
        let anthropic = choices
            .iter()
            .find(|choice| {
                matches!(&choice.value, SecretNameChoice::Name(name) if name == "ANTHROPIC_API_KEY")
            })
            .expect("Anthropic choice exists");

        assert_eq!(openai.label, "OPENAI_API_KEY ✓");
        assert!(openai.description.ends_with("(saved)"));
        assert_eq!(anthropic.label, "ANTHROPIC_API_KEY");
    }

    #[test]
    fn input_window_keeps_the_caret_end_visible() {
        assert_eq!(input_window("abcdefgh", false, 5), "defgh");
        assert_eq!(input_window("ab界", false, 3), "b界");
        assert_eq!(input_window("secret", true, 4), "****");
    }

    #[test]
    fn command_output_wraps_without_losing_wide_characters() {
        let lines = vec!["abcdef".to_string(), "ab界cd".to_string(), String::new()];
        assert_eq!(
            wrap_output_lines(&lines, 4),
            vec!["abcd", "ef", "ab界", "cd", ""]
        );
    }

    #[test]
    fn model_status_uses_saved_names_without_secret_reads() {
        let saved = BTreeSet::from([
            "ANTHROPIC_API_KEY".to_string(),
            "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
        ]);
        let legacy = BTreeSet::new();
        assert_eq!(model_credential_status(&saved, &legacy), "available");
    }

    #[test]
    fn first_scroll_up_moves_off_the_output_tail() {
        let mut view = RunView::new("test");
        for idx in 0..30 {
            view.push(format!("line {idx}"));
        }
        view.follow_tail(80, 24);
        let bottom = view.scroll;

        view.scroll_up(1, 80, 24);

        assert!(!view.follow);
        assert_eq!(view.scroll, bottom - 1);
    }

    #[test]
    fn interactive_routes_do_not_restore_the_regular_terminal_mid_session() {
        let source = include_str!("interactive.rs");
        let old_suspend_path = ["suspend", "_terminal"].concat();
        let old_line_prompt = ["prompt", "_field"].concat();
        assert!(!source.contains(&old_suspend_path));
        assert!(!source.contains(&old_line_prompt));
    }

    #[test]
    fn find_repo_root_detects_stable_sentinel_and_fails_when_absent() {
        use std::fs;
        let root = tempfile::tempdir().unwrap();
        // A repo root is runner/Dockerfile + pyproject.toml (stable markers), not
        // a specific example directory (#458 fix 3).
        fs::create_dir_all(root.path().join("runner")).unwrap();
        fs::write(root.path().join("runner/Dockerfile"), b"FROM scratch\n").unwrap();
        fs::write(root.path().join("pyproject.toml"), b"[project]\n").unwrap();
        let nested = root.path().join("cli").join("src");
        fs::create_dir_all(&nested).unwrap();
        assert_eq!(
            find_repo_root(nested),
            Some(root.path().to_path_buf()),
            "should walk up to the repo root"
        );

        // No sentinel: fail (None) rather than silently returning the start dir.
        let bare = tempfile::tempdir().unwrap();
        let bare_nested = bare.path().join("a").join("b");
        fs::create_dir_all(&bare_nested).unwrap();
        assert_eq!(find_repo_root(bare_nested), None);
    }
}

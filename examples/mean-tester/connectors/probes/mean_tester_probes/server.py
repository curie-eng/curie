"""The mean tester's tools (ADR 0169). Guardrails live here, not in the prompt."""

# No `from __future__ import annotations`: MCPServer introspects signatures.
import json
import logging
import os
import sys
import threading
import time

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from mean_tester_probes.config import Config, ConfigError, RepoRef
from mean_tester_probes.guard import GuardRefusal, ProbeGuard, refuse_unlisted
from mean_tester_probes.observe import observe
from mean_tester_probes.rounds import RoundLimiter, RoundRefusal

log = logging.getLogger("mean-tester-probes")
READ = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=True)
POST = ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=True)
POLL_INTERVAL_S = 5
REPLAY_REFUSAL = (
    "issue filing is disabled in this connector (replay mode); report the FAIL without filing"
)


def build(config: Config, slack, sources, issues=None, now=time.monotonic) -> MCPServer:
    """Wire the tools. `now` is the clock the round caps count time with.

    This process is one hosted connector shared by every thread, so nothing
    here remembers a "current" target: every tool names its channel, and the
    issue tools name their repository, which the approval card then shows.
    What is remembered is what this process itself did, and only as a check:
    the repositories `read_target` returned, and the probes `send_probes`
    posted.
    """
    mcp = MCPServer("probes")
    guard = ProbeGuard(config)
    limiter = RoundLimiter(config, now)
    read_repos: set[str] = set()
    posted: dict[tuple[str, str], float] = {}  # (channel, ts) -> when, on `now`
    posted_lock = threading.Lock()
    # Past this a probe's replies are no longer waited for, so its ts is dropped.
    retention_s = 2 * config.reply_timeout_s

    def remember(channel: str, ts: str) -> None:
        with posted_lock:
            t = now()
            for key in [k for k, when in posted.items() if t - when > retention_s]:
                del posted[key]
            posted[(channel, ts)] = t

    def foreign_probes(channel: str, probe_ts: list[str]) -> list[str]:
        with posted_lock:
            return [ts for ts in probe_ts if (channel, ts) not in posted]

    def listed_channel(channel: str) -> str:
        """The channel a tool works in. The runner gives the agent no Slack
        channel id, so an omitted one means the only operator-listed channel."""
        if not channel:
            if len(config.channels) != 1:
                listed = ", ".join(sorted(config.channels))
                raise ToolError(f"several channels are listed ({listed}); pass channel")
            (channel,) = config.channels
        try:
            refuse_unlisted(config, channel)
        except GuardRefusal as exc:
            raise ToolError(str(exc)) from exc
        return channel

    def issue_repo(repository: str) -> RepoRef:
        if repository not in read_repos:
            raise ToolError(
                f"read_target has not returned {repository} in this session; issues are "
                "searched and filed only in a repository read_target returned"
            )
        if issues is None:
            raise ToolError(REPLAY_REFUSAL)
        listed = next((r for r in config.repos if r.full_name == repository), None)
        if listed is None:
            raise ToolError(f"{repository} is not listed in MEAN_TESTER_REPOS")
        return listed

    @mcp.tool(annotations=READ)
    def read_target(target_user: str, channel: str = "", bundle_name: str = "") -> dict:
        """Find the target's bundle in the listed repositories and read it."""
        channel = listed_channel(channel)
        log.info("read_target channel=%s target_user=%r bundle_name=%r",
                 channel, target_user, bundle_name)
        found = sources.find(channel, bundle_name or None)
        if not found:
            raise ToolError(
                "no bundle in the listed repositories deploys to this channel or has that name"
            )
        if len(found) > 1:
            names = ", ".join(sorted(b.name for b in found))
            raise ToolError(f"several bundles match ({names}); ask which one and pass bundle_name")
        b = found[0]
        if hasattr(slack, "use"):
            slack.use(b.name)
        read_repos.add(b.repo.full_name)
        return {
            "bundle": b.name, "repository": b.repo.full_name, "commit": b.commit,
            "path": b.path, "files": b.files,
            "spec": b.spec, "spec_omitted": list(b.spec_omitted),
            "target_in_channel": target_user in slack.members(channel),
        }

    @mcp.tool(annotations=READ)
    def find_open_issue(repository: str, query: str) -> dict:
        """Search `repository` (owner/name, as read_target returned it) for an open
        issue about this failure."""
        repo = issue_repo(repository)
        found = issues.find_open(repo, query)
        return {
            "repository": repo.full_name,
            "matches": [
                {"number": i["number"], "title": i["title"], "url": i["html_url"]} for i in found
            ],
        }

    @mcp.tool(annotations=POST)
    def file_issue(repository: str, title: str, body: str) -> dict:
        """File one confirmed failure in `repository` (owner/name, as read_target
        returned it). Gated: a person approves the card first."""
        repo = issue_repo(repository)  # before `issues` is touched: None in replay mode
        return {"url": issues.create(repo, title, body)}

    @mcp.tool(annotations=POST)
    def send_probes(target_user: str, probes: list[str], channel: str = "") -> dict:
        """Post each probe as a new root message mentioning the target."""
        channel = listed_channel(channel)  # before Slack is asked anything about it
        log.info("send_probes channel=%s target_user=%r probes=%d",
                 channel, target_user, len(probes))
        try:
            texts = guard.check(channel, slack.channel_info(channel), probes, target_user)
            if target_user not in slack.members(channel):
                raise GuardRefusal(f"<@{target_user}> is not a member of {channel}")
            limiter.reserve(channel, target_user, len(texts))
        except (GuardRefusal, RoundRefusal) as exc:
            raise ToolError(str(exc)) from exc
        sent: list[dict] = []
        for k, text in enumerate(texts, start=1):
            try:
                ts = slack.post(channel, text)
            except Exception as exc:
                limiter.release(channel, target_user, len(texts) - len(sent))
                raise ToolError(
                    f"posting probe {k} of {len(texts)} failed ({exc}). These probes were "
                    f"already posted; do not send them again: {json.dumps(sent)}"
                ) from exc
            remember(channel, ts)
            sent.append({"text": text, "ts": ts})
        return {"probes": sent}

    @mcp.tool(annotations=READ)
    def collect_replies(target_user: str, probe_ts: list[str], channel: str = "") -> dict:
        """Wait for each probe's final reply and report what a person would see."""
        channel = listed_channel(channel)
        log.info("collect_replies channel=%s target_user=%r probe_ts=%s",
                 channel, target_user, probe_ts)
        foreign = foreign_probes(channel, probe_ts)
        if foreign:
            raise ToolError(
                f"{', '.join(foreign)} in {channel} was not posted by send_probes in this "
                "session; replies are collected only for this connector's own probes"
            )
        deadline = time.monotonic() + config.reply_timeout_s
        pending, done = list(probe_ts), {}
        while pending and time.monotonic() < deadline:
            for ts in list(pending):
                o = observe(
                    slack.replies(channel, ts), target_user, ts, time.time(), config.settle_s
                )
                if o.final:
                    done[ts] = o
                    pending.remove(ts)
            if pending:
                time.sleep(POLL_INTERVAL_S)
        observations = [{"probe_ts": ts, **done[ts].as_dict()} for ts in probe_ts if ts in done]
        return {"observations": observations, "timed_out": pending}

    return mcp


def main() -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), stream=sys.stderr)
    try:
        config = Config.from_env(os.environ)
    except ConfigError as exc:
        log.error("refusing to start: %s", exc)
        return 1
    replay = os.environ.get("MEAN_TESTER_REPLAY_DIR")
    if replay:
        from mean_tester_probes.replay import ReplaySlack, ReplaySources
        slack, sources, issues = ReplaySlack(replay), ReplaySources(replay), None
    else:
        from mean_tester_probes.issues import GitHubIssues
        from mean_tester_probes.slack import SlackApi
        from mean_tester_probes.sources import GitHubSources
        slack = SlackApi(config.slack_token)
        sources = GitHubSources(config.github_token, config.repos, spec_paths=config.spec_paths)
        issues = GitHubIssues(config.github_token)
    build(config, slack, sources, issues=issues).run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

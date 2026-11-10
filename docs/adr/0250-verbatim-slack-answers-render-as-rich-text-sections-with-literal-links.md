# 250. Verbatim Slack answers render as rich text sections with literal links

Date: 2026-10-26

Status: Accepted

Accepted alongside implementation with explicit maintainer approval from Junwon Jung on 2026-10-26 under ADR 0102. Approval covers the rendering decision below and its realizing paths.

## Context

[ADR 0227](0227-opt-in-final-answer-publication-validation.md) lets a bundle
require that a validated final answer is published exactly. Its clause 8 has the
Slack adapter emit "literal plain-text blocks" and partition oversized text
losslessly. A `plain_text` section showed line breaks as spaces in the Slack
client, so the adapter switched to one `rich_text` block holding a single
`rich_text_preformatted` element per partition. That keeps every character and
line break, but the client draws it as a grey monospace code box. A business
reader sees what looks like program output, URLs are not clickable, and a
bundle whose whole purpose is a readable report cannot adopt exact publication.

The exactness ADR 0227 guarantees is about the validated logical text and a
lossless transport payload, not one particular visual style. The defect is the
visual style chosen to carry that text.

## Decision

1. A verbatim answer (`ReplyUpdate.text_mode == "verbatim"`) is delivered to
   Slack as one `rich_text` block per partition. The block holds one
   `rich_text_section` whose elements are `text` and `link` elements only. No
   `style`, emoji, mention, list, quote or preformatted element is emitted, and
   no characters are added, removed or normalized.
2. A `link` element is emitted for each `http://` or `https://` URL found in the
   text. Its `url` and its visible `text` are both the exact original substring.
   Trailing sentence punctuation and an unbalanced closing parenthesis or
   bracket are left outside the URL as ordinary text; balanced parentheses and
   percent escapes stay inside it. Everything else is emitted as `text`
   elements carrying the original characters, line breaks and leading spaces.
   The text is never interpreted as mrkdwn or Markdown.
3. Concatenating the visible text of every element across all partitions
   reconstructs the approved text exactly. Each partition's plain `text`
   fallback is exactly that partition's content.
4. Partitions keep the existing UTF-8 byte budget. Within that budget a split
   prefers a line break, then a space, in the later half of the partition, and
   never splits inside a URL that fits in one partition. A URL longer than the
   budget is emitted as text, because a fragment of it is not the URL.
5. Rejected blocks still surface as delivery errors with no fallback to the
   legacy formatter. Ordinary (non-verbatim) replies are unchanged.

This supersedes, in part, the rendering sentence of ADR 0227 clause 8 ("emits
literal plain-text blocks"). The rest of ADR 0227 stands.

Realizing paths: `curie_worker.slack_text` (URL segmentation, boundary-aware
lossless partitioning and the rich text block) and `curie_worker.slack_sink`
(verbatim delivery branch).

## Consequences

- A verbatim answer reads as an ordinary Slack message: proportional font, no
  code box, clickable links.
- Slack draws the `rich_text_section` text itself. Whitespace and blank-line
  display remain client behavior outside byte-level platform control and still
  require live integration qualification, as ADR 0227 states.
- Formatting a bundle writes as Markdown or mrkdwn (for example `*bold*`)
  appears as its literal characters, as before. A bundle that wants emphasis
  must use an ordinary, unconstrained answer.
- Link detection is a fixed syntactic rule, not interpretation: it changes
  which element carries a substring, never the substring.

## Alternatives considered

- Keep `rich_text_preformatted`: exact, but unreadable for the intended reader.
- `section` with `mrkdwn` text: Slack would interpret `*`, `_`, `~`, `<...>` and
  `&` sequences, altering the approved text.
- `section` with `plain_text`: line breaks render as spaces.
- Plain `text` with no blocks: Slack parses it as mrkdwn.
- Detect bare domains or `mailto:` links: wider heuristics risk wrapping
  non-URL text; the exact substring stays visible either way.

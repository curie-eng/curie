# 192. The model sees the platform sender

Date: 2026-10-03

Status: Accepted

Accepted with the implementation of [issue 3818](https://github.com/curie-eng/curie/issues/3818) under [ADR 0102](0102-accepted-alongside-implementation-with-explicit-approval.md).

## Context

The model has to know who sent a turn, and that fact has to come from the platform rather than from text the sender can forge. A line inside the user message that copies a sender header is untrusted. The system prompt is the wrong place for a per turn fact, because [ADR 0119](0119-a-resumed-thread-rebuilds-its-prefix-so-the-prompt-cache-still-hits.md) keeps that prefix stable so the prompt cache still hits.

## Decision

On turn start and on steer, the runner queries the model with a platform sender frame prepended to the user text. The frame is built only from the event type, the event user, and the boot env channel kind. It is not part of the system prompt. A boundary token that does not occur in the user text, the user id, or the channel kind fences the user text, so a forged sender line inside the message cannot occupy the platform line. The same framed string is what the transcript stores as the user message, so replay matches the queried turn.

An eval case may name that user with an optional sender. When the sender is non-empty, the eval drivers use it as the turn author. When it is omitted, existing suites keep today's author.

The realizing paths are `curie_runner.sender_frame.frame_user_turn`, `SessionRunner._drive_turn` and `SessionRunner.steer`, `BootEnv.channel_kind`, `BindingResolver.boot_env`, `Kernel._to_event`, and `EvalCase.sender` in the worker model and the Rust CLI.

## Consequences

A forged sender inside the user text does not replace the platform person. A scheduled run names no person. Eval suites that omit sender keep decoding. The channel kind is a declared optional boot env field.

## Alternatives considered

1. Put the sender in the system prompt. Rejected because ADR 0119 keeps that prefix cacheable, and a per turn sender would bust it.
2. Parse the user id to decide the channel. Rejected because [ADR 0012](0012-substrate-and-channel-agnostic-core.md) keeps the core channel agnostic. A user id that looks like one channel's id is not that channel.
3. Add a new TurnSource enum value. Rejected. Cron and webhook stay jobs, and eval stays the eval case type the drivers already construct.
4. Read the channel kind out of the memory URL. Rejected. The kind is an input to boot, not a string decoded from a credential URL.
5. Pass the channel kind as an undeclared environment variable. Rejected. An undeclared variable is invisible to the boot contract and to older readers. The kind is an optional declared boot env field instead.

//! Exercise worker input ownership through the real local teardown command.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Command, Output};

const DOCKER: &str = r#"#!/usr/bin/env python3
import json
import os
import re
import sqlite3
import sys

args = sys.argv[1:]
db = sqlite3.connect(os.environ['CURIE_TEST_REAP_DB'])
db.execute('CREATE TABLE IF NOT EXISTS resources (kind TEXT, name TEXT, body TEXT, PRIMARY KEY(kind, name))')
db.execute('CREATE TABLE IF NOT EXISTS calls (argv TEXT)')
if not db.execute('SELECT 1 FROM calls').fetchone():
    base = {'curietech.ai/managed-by': 'curie-sandbox-substrate',
            'curietech.ai/claim': 'acme-claim',
            'curietech.ai/claim-attempt': '00000000-0000-4000-8000-000000000001'}
    case = os.environ.get('CURIE_TEST_REAP_CASE', '')
    if case == 'malformed-attempt':
        base['curietech.ai/claim-attempt'] = 'not-a-uuid'
    if case == 'empty-claim':
        base['curietech.ai/claim'] = ''
    volumes = {}
    for component in ['bundle', 'workspace', 'attachments']:
        volumes['acme-' + component] = dict(base, **{'curietech.ai/component': 'claim-' + component})
    volumes['other-attempt'] = dict(volumes['acme-bundle'], **{
        'curietech.ai/claim-attempt': '00000000-0000-4000-8000-000000000002'})
    volumes['other-claim'] = dict(volumes['acme-bundle'], **{'curietech.ai/claim': 'acme-claim-neighbor'})
    volumes['foreign-volume'] = dict(volumes['acme-bundle'], **{'curietech.ai/managed-by': 'foreign'})
    volumes['wrong-component'] = dict(volumes['acme-bundle'], **{'curietech.ai/component': 'claim-bundle-extra'})
    volumes['missing-owner'] = {key: value for key, value in volumes['acme-bundle'].items()
                                if key != 'curietech.ai/managed-by'}
    for field in ['claim', 'claim-attempt', 'component']:
        volumes['missing-' + field] = {key: value for key, value in volumes['acme-bundle'].items()
                                      if key != 'curietech.ai/' + field}
    for name, labels in volumes.items():
        db.execute('INSERT INTO resources VALUES (?, ?, ?)', ('volume', name, json.dumps({'Name': name, 'Labels': labels})))
    runner = {'Id': 'acme-runner', 'Name': '/acme-claim',
              'Config': {'Labels': dict(base, **{'curietech.ai/component': 'claim-runner'})},
              'NetworkSettings': {'Networks': {'curie_runner': {}}},
              'Mounts': [{'Type': 'volume', 'Name': name, 'Destination': '/inputs/' + name}
                         for name in ['acme-bundle', 'acme-workspace', 'acme-attachments']]}
    if case.startswith('runner-missing-'):
        runner['Config']['Labels'].pop('curietech.ai/' + case.removeprefix('runner-missing-'))
    if case == 'runner-wrong-component':
        runner['Config']['Labels']['curietech.ai/component'] = 'claim-runner-extra'
    if case == 'untrusted-mounts':
        runner['Mounts'] = [{'Type': 'volume', 'Name': name, 'Destination': '/inputs/' + name}
                            for name in volumes]
    db.execute('INSERT INTO resources VALUES (?, ?, ?)', ('container', 'acme-runner', json.dumps(runner)))
db.execute('INSERT INTO calls VALUES (?)', (json.dumps(args),))
db.commit()

def fail(message):
    print(message, file=sys.stderr)
    sys.exit(1)

def resources(kind):
    return [(name, json.loads(body)) for name, body in
            db.execute('SELECT name, body FROM resources WHERE kind = ? ORDER BY name', (kind,))]

def matching(body):
    labels = body.get('Labels', body.get('Config', {}).get('Labels', {}))
    for index, arg in enumerate(args):
        if arg != '--filter':
            continue
        key, _, value = args[index + 1].partition('=')
        if key == 'label':
            label, equal, expected = value.partition('=')
            if label not in labels or (equal and labels[label] != expected):
                return False
        elif key == 'network' and value not in body.get('NetworkSettings', {}).get('Networks', {}):
            return False
    return True

def inspect(kind):
    name = args[-1]
    row = db.execute('SELECT body FROM resources WHERE kind = ? AND name = ?', (kind, name)).fetchone()
    if not row:
        fail('No such ' + kind + ': ' + name)
    body = json.loads(row[0])
    if '--format' not in args and '-f' not in args:
        print(json.dumps([body]))
        return
    flag = '--format' if '--format' in args else '-f'
    template = args[args.index(flag) + 1]
    labels = body.get('Labels', body.get('Config', {}).get('Labels', {}))
    if '{{if eq ' in template:
        if labels.get('curietech.ai/managed-by') == 'curie-cli':
            fail('unexpected CLI-owned fixture')
        return
    def render(match):
        expression = match.group(1).strip()
        if expression.startswith('json '):
            value = body
            for key in expression[5:].strip().strip('.').split('.'):
                if key:
                    value = value[key]
            return json.dumps(value)
        key = re.fullmatch(r'index \.(?:Config\.)?Labels "([^"]+)"', expression)
        if key:
            return labels.get(key.group(1), '<no value>')
        fail('unsupported inspect format: ' + expression)
    print(re.sub(r'\{\{(.*?)\}\}', render, template))

if args[0] == 'compose' and 'down' in args:
    pass
elif args[0] == 'ps':
    print('\n'.join(name for name, body in resources('container') if matching(body)))
elif args[:2] == ['volume', 'ls']:
    print('\n'.join(name for name, body in resources('volume') if matching(body)))
elif args[:2] in [['container', 'inspect'], ['volume', 'inspect']]:
    inspect(args[0])
elif args[0] == 'inspect':
    inspect('container')
elif args[0] == 'rm' or args[:2] == ['container', 'rm']:
    start = 1 if args[0] == 'rm' else 2
    for name in (arg for arg in args[start:] if not arg.startswith('-')):
        db.execute('DELETE FROM resources WHERE kind = ? AND name = ?', ('container', name))
        print(name)
    db.commit()
elif args[:2] == ['volume', 'rm']:
    for name in (arg for arg in args[2:] if not arg.startswith('-')):
        if any(mount.get('Name') == name for _, body in resources('container') for mount in body.get('Mounts', [])):
            fail('volume is in use: ' + name)
        db.execute('DELETE FROM resources WHERE kind = ? AND name = ?', ('volume', name))
        print(name)
    db.commit()
else:
    fail('unexpected Docker operation: ' + repr(args))
"#;

fn docker(tools: &Path, database: &Path, args: &[&str]) -> Output {
    Command::new(tools.join("docker"))
        .args(args)
        .env("CURIE_TEST_REAP_DB", database)
        .output()
        .expect("run recording Docker daemon")
}

fn local_down(dir: &Path, case: &str) -> Output {
    let database = dir.join("docker.sqlite");
    crate::support::executable::install_in(dir, "docker", DOCKER);
    let path = std::env::join_paths(std::iter::once(dir.to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))
    .expect("stub PATH");
    let compose = dir.join("compose.yaml");
    std::fs::write(&compose, "services: {}\n").expect("fixture compose file");
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["--json", "local", "down", "--wipe", "--yes", "-f"])
        .arg(&compose)
        .current_dir(dir)
        .env("PATH", path)
        .env("CURIE_TEST_REAP_DB", &database)
        .env("CURIE_TEST_REAP_CASE", case)
        .env_remove("COMPOSE_FILE")
        .env_remove("COMPOSE_PROJECT_NAME")
        .output()
        .expect("run real local down");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("unexpected Docker operation")
            && !stderr.contains("unsupported inspect format"),
        "recording daemon does not support this production operation: {stderr}"
    );
    output
}

fn volume_names(dir: &Path) -> BTreeSet<String> {
    let volumes = docker(dir, &dir.join("docker.sqlite"), &["volume", "ls", "-q"]);
    assert!(volumes.status.success());
    String::from_utf8_lossy(&volumes.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

fn preserved_volumes() -> BTreeSet<String> {
    [
        "other-attempt",
        "other-claim",
        "foreign-volume",
        "wrong-component",
        "missing-owner",
        "missing-claim",
        "missing-claim-attempt",
        "missing-component",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[test]
fn local_down_wipe_reaps_only_exact_worker_claim_attempt_inputs() {
    let dir = tempfile::tempdir().expect("temporary fixture");
    let output = local_down(dir.path(), "");
    let diagnostic = format!(
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{diagnostic}");
    let containers = docker(
        dir.path(),
        &dir.path().join("docker.sqlite"),
        &["ps", "-a", "-q"],
    );
    assert!(containers.status.success());
    assert!(containers.stdout.is_empty() || containers.stdout == b"\n");
    assert_eq!(
        volume_names(dir.path()),
        preserved_volumes(),
        "{diagnostic}"
    );
}

#[test]
fn local_down_preserves_mounted_volumes_without_exact_input_ownership() {
    let dir = tempfile::tempdir().expect("temporary fixture");
    let output = local_down(dir.path(), "untrusted-mounts");
    let remaining = volume_names(dir.path());
    assert!(
        preserved_volumes().is_subset(&remaining),
        "foreign, incomplete, and neighboring ownership must survive even when mounted; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn local_down_preserves_inputs_without_valid_runner_ownership() {
    for case in [
        "malformed-attempt",
        "empty-claim",
        "runner-missing-claim",
        "runner-missing-claim-attempt",
        "runner-missing-component",
        "runner-missing-managed-by",
        "runner-wrong-component",
    ] {
        let dir = tempfile::tempdir().expect("temporary fixture");
        let output = local_down(dir.path(), case);
        let mut expected = preserved_volumes();
        expected.extend(["acme-bundle", "acme-workspace", "acme-attachments"].map(str::to_owned));
        assert_eq!(
            volume_names(dir.path()),
            expected,
            "{case}: invalid runner ownership must not authorize volume removal; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

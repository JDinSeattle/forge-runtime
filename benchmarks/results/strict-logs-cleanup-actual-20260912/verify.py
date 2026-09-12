"""Offline archive verification. No original checkout, network, DB, Docker, keys or subprocesses."""
from pathlib import Path
import base64
import collections
import hashlib
import json
import tarfile

ROOT = Path(__file__).resolve().parent
sha = lambda value: hashlib.sha256(value).hexdigest()
checks = []
def check(ok, name):
    if not ok:
        raise AssertionError(name)
    checks.append(name)

index = json.loads((ROOT / 'archive-manifest.json').read_bytes())
archive = ROOT / 'raw-evidence.tar.gz'
check(sha(archive.read_bytes()) == index['tar_sha256'], 'tar hash')
with tarfile.open(archive, 'r:gz') as tf:
    members = {m.name: m for m in tf.getmembers()}
    indexed = {x['archived_member'] for x in index['files'] if 'archived_member' in x}
    check(set(members) == indexed and all(m.isfile() for m in members.values()), 'exact regular tar inventory')
    raw = {name: tf.extractfile(member).read() for name, member in members.items()}
for item in index['files']:
    value = raw[item['archived_member']] if 'archived_member' in item else (ROOT / item['archived_file']).read_bytes()
    check(sha(value) == item['archived_sha256'] == item['source_sha256'] and len(value) == item['bytes'], 'unchanged original ' + item.get('archived_member', item.get('archived_file')))

def j(name):
    return json.loads(raw[name])
C = 'raw/logs-01-cleanup/'
manifest = j(C + 'manifest.json')
check(set(manifest) | {'manifest.json'} == {name[len(C):] for name in raw if name.startswith(C)}, 'original cleanup manifest exact inventory')
for name, digest in manifest.items():
    check(sha(raw[C + name]) == digest, 'original cleanup hash ' + name)
before = j(C + 'invocation-1075405454/journal-before.json')
after = j(C + 'released-journal.json')
check(before == j('raw/historical/L3-bytes-op-03-journal.json'), 'original failed authority exactly retained')
check(before['identity'] == after['identity'] == 'b010dd7440a54c58601967342f397faf686b7e306061af9f49809ee342b0ec8e' and before['version'] == after['version'] == 5, 'unchanged UUID and version')
b, a = before['tables'], after['tables']
check(len(a['operations']) == 44 and b['operations'] == a['operations'], 'all44 full operation rows unchanged')
check(all(row['status'] in ('succeeded', 'failed', 'cancelled') for row in a['operations']), 'all44 terminal')
for table in ('log_runs', 'volume_slots'):
    check(b[table] == a[table], 'unchanged ' + table)
run = 'sl-L3-bytes-46djebzw3ifph2ui7nesemhkz6'
for table, identity, columns, total in [('workspaces', 'id', {'stopped', 'adopting', 'released'}, 1), ('volume_leases', 'workspace_id', {'released'}, 1), ('operation_logs', 'operation_id', {'cleanup_state'}, 4)]:
    old, new = ({x[identity]: x for x in t[table]} for t in (b, a))
    check(old.keys() == new.keys(), table + ' row identities')
    changed = [key for key in old if old[key] != new[key]]
    check(len(changed) == total, table + ' changed count')
    for key in changed:
        check(key == run or (table == 'operation_logs' and key in {run + '-op-%02d' % i for i in range(4)}), 'own identity ' + key)
        diff = {k: (old[key].get(k), new[key].get(k)) for k in old[key].keys() | new[key].keys() if old[key].get(k) != new[key].get(k)}
        check(set(diff) <= columns and all(v == (('retained', 'removed') if table == 'operation_logs' else (0, 1)) for v in diff.values()), 'allowed changes ' + key)
check(len(a['operation_logs']) == 42 and all(x['cleanup_state'] == 'removed' for x in a['operation_logs']), 'all42 log removal acknowledgements')
check(len(a['volume_slots']) == len(a['volume_leases']) == len(a['workspaces']) == 4 and all(x['released'] == 1 and x['active_operation'] == '' for x in a['workspaces']) and all(x['released'] == 1 for x in a['volume_leases']), 'all4 release/no active')
oldpins = {x['object_key']: x for x in b['runner_artifacts']}
pins = {x['object_key']: x for x in a['runner_artifacts']}
check(len(oldpins) == 95 and len(pins) == 97 and all(pins[k] == v for k, v in oldpins.items()), '95 unchanged pins plus2')
stop, snap = j(C + 'stop.json'), j(C + 'snapshot.json')
for label, ref in [('stop', stop['ref']), ('snapshot', snap['artifact'])]:
    data = raw[C + label + '.bytes']
    check(sha(data) == ref['sha256'] and len(data) == ref['size'] and ref['run_id'] == run and ref['tenant_id'] == 'strict-log-fixture' and ref['object_key'] == 'strict-log-fixture/' + run + '/' + ref['sha256'], label + ' content address and authority')
    check(json.loads(pins[ref['object_key']]['ref_json']) == ref, label + ' durable pin')
check(stop['no_active_operations'] is True and stop['workspace'] == snap['workspace'], 'no-active stopped workspace bound to snapshot')
files = j(C + 'snapshot.bytes')['files']
check(set(files) == {'app.py'}, 'snapshot exact inventory')
content = base64.b64decode(files['app.py']['content'], validate=True)
check(content == b'def clamp(v, lo, hi):\n    return v\n' and sha(content) == files['app.py']['sha256'] and files['app.py']['executable'] is False, 'snapshot source bytes/mode')
check(j(C + 'release-intent.json') == {'workspace_id': run, 'epoch': 1, 'stop_sha256': stop['ref']['sha256'], 'snapshot_sha256': snap['artifact']['sha256'], 'snapshot_verified': True}, 'release intent exact proofs')
check(j(C + 'release.json') == {'workspace_id': run, 'released': True}, 'typed release acknowledgement')
peer, signal = j(C + 'invocation-1075405454/runner-01-peer.json'), j(C + 'invocation-1075405454/runner-01-stop.json')
acceptance = j(C + 'acceptance-input.json')
check(peer['peer_pid'] == peer['spawned_runner_pid'] == signal['pid'] and peer['sha256'] == signal['exe_sha256'] == acceptance['runner_sha256'] and signal['exit_code'] == 0 and str(signal['proc_start_ticks']).isdigit(), 'own runner peer identity and exit0')
host = j('raw/host-logs-cleanup-02/result.json')
check(host['exit_code'] == 1 and host['after']['MainPID'] == '0' and host['after']['LoadState'] == 'not-found', 'original overall exit1 preserved')
check('required existing stage differs:' in raw['raw/host-logs-cleanup-02/execution.log'].decode(), 'original failure text')
review = json.loads((ROOT / 'canonical-review.json').read_bytes())
check(review['validation']['pass'] == 41 and review['validation']['skip'] == 2 and review['validation']['fail'] == 0 and review['validation']['vet_exit_code'] == 0, 'bounded independent canonical regression')
old = j('raw/logs-02-preparation-revision/original-acceptance-logs-02.json')
intent = j('raw/logs-02-preparation-revision/intent.json')
result = j('raw/logs-02-preparation-revision/result.json')
check(sha(raw['raw/logs-02-preparation-revision/original-acceptance-logs-02.json']) == intent['old_sha256'] == result['original_copy_sha256'] and old == acceptance, 'pending preparation original preserved')
check(result['prepared'] is True and result['executed'] is False and result['historical_inputs_unchanged'] is True and result['new_sha256'] == intent['new_sha256'], 'preparation revision outcome only')
print(json.dumps({'archive_audit_passed': True, 'checks': len(checks), 'original_cleanup_test_passed': False, 'material_release_confirmed': True, 'direct_post_rm_daemon_probe': False, 'raw_members': len(raw)}, sort_keys=True))

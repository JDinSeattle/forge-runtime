"""Verify only the checked-in archive; never call network, subprocesses, credentials or runtime resources."""
from pathlib import Path
import hashlib
import json
import tarfile

root = Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
checks = []
def check(v, label):
    if not v:
        raise AssertionError(label)
    checks.append(label)
index = json.loads((root/'archive-manifest.json').read_bytes())
archive = root/'raw-evidence.tar.gz'
check(sha(archive.read_bytes()) == index['tar_sha256'], 'archive hash')
with tarfile.open(archive, 'r:gz') as tf:
    check(all(x.isfile() for x in tf.getmembers()), 'regular evidence only')
    raw = {m.name: tf.extractfile(m).read() for m in tf.getmembers()}
check(set(raw) == {x['archived_member'] for x in index['files'] if 'archived_member' in x}, 'exact tar inventory')
by_member = {}
for row in index['files']:
    data = raw[row['archived_member']] if 'archived_member' in row else (root/row['archived_file']).read_bytes()
    check(sha(data) == row['archived_sha256'], 'archived hash '+row.get('archived_member',row.get('archived_file')))
    if row['projection'] is None:
        check(row['source_sha256'] == row['archived_sha256'], 'byte-identical source '+row.get('archived_member',row.get('archived_file')))
    else:
        v = json.loads(data)
        check(row['archived_member'] == 'raw/logs-02/L1/fixture.json' and 'worker_config_sha256' not in v and v['_archive_projection'] == row['projection'] and row['projection']['original_file_sha256'] == row['source_sha256'], 'explicit one-field private digest projection')
    if 'archived_member' in row:
        by_member[row['archived_member']] = row
j = lambda name: json.loads(raw[name])
for prefix, size in [('raw/logs-02/',15),('raw/logs-02-before-runner-observation/',4)]:
    manifest = j(prefix+'manifest.json')
    check(len(manifest) == size and set(manifest)|{'manifest.json'} == {x[len(prefix):] for x in raw if x.startswith(prefix)}, prefix+' original exact inventory')
    for name, expected in manifest.items():
        check(by_member[prefix+name]['source_sha256'] == expected, 'original manifest source binding '+prefix+name)
        if by_member[prefix+name]['projection'] is None:
            check(sha(raw[prefix+name]) == expected, 'raw original manifest hash '+prefix+name)
C='raw/logs-02/'
D='raw/logs-02-before-runner-observation/'
acceptance, pre, fixture = j(C+'acceptance.json'), j(C+'preflight.json'), j(C+'L1/fixture.json')
check(acceptance['passed'] is False and set(acceptance['cases']) == {'L5'} and acceptance['input_sha256_before'] == acceptance['input_sha256_after'], 'failed outcome onlyL5 and unchanged inputs')
check(pre['journal'] == j(C+'L5-live-authority.json') == j(D+'journal.json') == j('raw/historical-cleanup/released-journal.json'), 'all stored journal representations agree')
host = j('raw/host-logs-02-retry/result.json')
check(host['exit_code'] == 1 and host['after']['LoadState']=='not-found' and host['after']['MainPID']=='0', 'original host failure and exit')
check('strict-log-publication.json: file exists' in raw['raw/host-logs-02-retry/execution.log'].decode(), 'original precise failure')
p, h, r = j(D+'postgres.json'), j(D+'history.json'), j(D+'report.json')
check(p['schema']==r['schema']==j(C+'private-schema.json')['schema'] and p['schema_oid']=='852430', 'exact observed namespace/OID')
check(p['database']==h['database']=='forge' and p['current_user']==h['current_user']=='forge_admin' and p['superuser'] is True and h['superuser'] is True, 'read-only administrator sample identity')
check(len(p['runs'])==len(h['runs'])==1, 'single submitted run')
run, full = p['runs'][0], h['runs'][0]
for a,b in [('id','id'),('tenant','tenant_id'),('state','state'),('version','version'),('epoch','lease_epoch'),('owner','lease_owner'),('workspace','workspace_id'),('snapshot','snapshot')]:
    check(run[a]==full[b], 'two SQL transactions agree '+a)
check((run['id'],run['tenant'],run['state'],run['version'],run['epoch'],run['owner'],run['workspace'])==(fixture['run_id'],fixture['tenant'],'queued',1,0,'',''), 'recorded fixture exact unstarted run')
check(full['lease_until'] is None and full['runner_id'] is None and full['pending_commands']==[] and full['next_event_seq']==2, 'no claim assignment pendingcommands')
check(len(h['events'])==len(h['snapshots'])==1 and h['steps'] is None, 'only initial history')
e,s=h['events'][0],h['snapshots'][0]
check(e['type']=='run.created' and e['seq']==1 and e['payload']==run['snapshot'] and e['run_id']==run['id'] and e['tenant_id']==run['tenant'], 'only creationevent')
check(s['version']==1 and s['body']==run['snapshot'] and s['input_event'] is None and s['input_state'] is None and s['run_id']==run['id'] and s['tenant_id']==run['tenant'], 'only initial snapshot')
for key in ('effects','unsettled_effects','attempts','artifacts','active','runner_reserved','allocations','requests','unsettled_reservations','cleanup'):
    check(type(p[key]) is int and p[key]==0, 'zero '+key)
check(h['quota_reservations']==h['runner_allocations']==0, 'no reservation/allocation rows')
check(len(p['quotas'])==1 and all(p['quotas'][0][k]==0 for k in ('active_requests','reserved_tokens','committed_tokens','reserved_microusd','committed_microusd')), 'no synthetic quota use')
check(r['unit']==host['unit'] and r['unit_state']['LoadState']=='not-found' and r['unit_state']['MainPID']=='0', 'later exact unit absent')
for suffix, archived in [('forge-observe-logs02-before-runner.py','observer.py.txt'),('isolated_state.py','observer-isolated-state.py.txt')]:
    expected=next(v for k,v in r['source_hashes'].items() if k.endswith('/'+suffix))
    check(sha((root/archived).read_bytes())==expected, 'observer source '+suffix)
print(json.dumps({'archive_audit_passed':True,'checks':len(checks),'original_logs02_passed':False,'actual_readonly_sample_queued_no_execution':True,'raw_members':len(raw),'projected_members':1},sort_keys=True))

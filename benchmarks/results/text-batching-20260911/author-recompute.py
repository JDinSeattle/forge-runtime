#!/usr/bin/env python3
"""Author recheck of retained batching records; no DB, service or raw mutation."""
import collections
import hashlib
import importlib.util
import importlib.machinery
import json
from pathlib import Path
import re
import subprocess
import sys
sys.dont_write_bytecode = True

BASE = Path(__file__).resolve().parent
ROOT = BASE.parents[2]

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def read(path): return json.loads(path.read_text())
def require(value, text):
    if not value: raise AssertionError(text)
def go_json(value):
    text = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    for char, escaped in [('<', '\\u003c'), ('>', '\\u003e'), ('&', '\\u0026'), ('\u2028', '\\u2028'), ('\u2029', '\\u2029')]:
        text = text.replace(char, escaped)
    return text.encode('utf-8')
def instant_ns(value):
    import datetime
    second, dot, fraction = value.removesuffix('Z').partition('.')
    parsed = datetime.datetime.fromisoformat(second).replace(tzinfo=datetime.timezone.utc)
    return int(parsed.timestamp()) * 1_000_000_000 + int((fraction if dot else '').ljust(9, '0'))

build = read(BASE/'build-02-isolated/identity.json')
require(build['whole_source_unchanged'] and build['source_before'] == build['source_after'], 'unstable Go build')
owned = {str(p.relative_to(BASE/'build-02-isolated/source')).removesuffix('.txt') for p in (BASE/'build-02-isolated/source').rglob('*.go.txt')}
require(len(owned) == 8, 'expected eight frozen Go files')
for name, expected in build['source_after'].items():
    raw = (BASE/'build-02-isolated/source'/(name+'.txt')).read_bytes() if name in owned else subprocess.check_output(['git', 'show', build['commit']+':'+name], cwd=ROOT)
    require(hashlib.sha256(raw).hexdigest() == expected, 'not a9208e7 plus only batching: '+name)
for entry in build['binaries'].values(): require(sha(Path(entry['path'])) == entry['sha256'], 'fixed binary changed')

summaries = {}
for run in ['actual-01-isolated', 'actual-02-isolated']:
    folder = BASE/run
    execution = read(folder/'execution.json')
    require([row['exit_code'] for row in execution] == ([0,0,1] if run.startswith('actual-01') else [0,0,0]), 'execution classification changed')
    require(read(folder/'build-record/identity.json') == build, 'different build used')
    require(read(folder/'source-after-execution.json') == build['source_after'], 'source drift during execution')
    records = []
    for path in sorted((folder/'postgres').glob('*.json')):
        report = read(path)
        require(report['failed'] is False, 'PG case failed')
        events = report.get('events_after_close', report.get('events_while_text_paused', report.get('events')))
        require([e['seq'] for e in events] == list(range(1,len(events)+1)), 'event gap')
        require(all(e['run_id'] == report['run_id'] for e in events), 'event run mismatch')
        text = [e for e in events if e['type'] == 'text.delta']
        require(all(e['payload']['provisional'] is True for e in text), 'nonprovisional text')
        sizes = [len(go_json(e['payload'])) for e in text]
        require(all(n <= 32768 for n in sizes), 'encoded payload escaped cap')
        joined = ''.join(e['payload']['delta'] for e in text)
        expected = report.get('input', report.get('expected_text', ''))
        require(joined == expected, 'text changed during delivery')
        record = {'test':report['test'],'run_id':report['run_id'],'event_count':len(events),'text_events':len(text),'decoded_bytes':len(joined.encode()),'max_encoded_payload_bytes':max(sizes,default=0)}
        if 'events_before_close' in report:
            before = report['events_before_close']
            require(events[:len(before)] == before, 'close rewrote event prefix')
            require(any(e['type']=='text.delta' for e in before), 'no text persisted before close')
            require(report['decoded_bytes'] == len(joined.encode()) and report['text_event_count']==len(text), 'summary differs')
            record['first_observed_after_emit_ns'] = instant_ns(report['first_observed_at']) - instant_ns(report['emit_started_at'])
            record['emit_elapsed_ns'] = instant_ns(report['emit_returned_at']) - instant_ns(report['emit_started_at'])
        if 'consumer_cancelled' in report:
            require(report['consumer_cancelled'] and report['pending_text_fenced'], 'cancel not fenced')
            counts = collections.Counter(e['type'] for e in events)
            require(counts == {'run.created':1,'run.claimed':1,'run.message_added':1,'run.cancel_requested':1}, 'control event loss/duplication')
            record['control_commit_interval_ns'] = instant_ns(report['controls_returned_at']) - instant_ns(report['controls_started_at'])
        if 'full_model_turn' in report:
            turn, attempt = report['full_model_turn'], report['model_attempt']
            require(turn['text']==expected and turn['run_id']==attempt['RunID']==report['run_id'], 'model turn identity/content changed')
            require(turn['attempt_id']==attempt['ID'] and attempt['Status']=='completed' and bool(attempt['RawRef']), 'no complete artifact binding')
            require(all(e['payload']['attempt_id']==attempt['ID'] for e in text), 'delta attempt mismatch')
            require(hashlib.sha256(joined.encode()).hexdigest()==report['full_text_sha256'], 'full-text digest mismatch')
            record['decoded_model_text_sha256']=report['full_text_sha256']
            record['raw_artifact_independently_available']=False
        records.append(record)
    require(len(records)==4, 'missing PG cases')
    summaries[run]={'exit_codes':[r['exit_code'] for r in execution],'postgres':records}

auditor_path=BASE/'f03-auditor.py.txt'
loader=importlib.machinery.SourceFileLoader('f03_audit',str(auditor_path))
spec=importlib.util.spec_from_loader('f03_audit',loader)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
f03=[]
for path in sorted((BASE/'actual-02-isolated/native-f03').glob('*/report.json')):
    report=read(path);result=m.audit(path.parent,report)
    require(report['manifest']['executing_binary_sha256']==build['binaries']['benchmarks.test']['sha256'],'F03 wrong binary')
    require(all(build['source_after'][name]==value for name,value in report['manifest']['source_sha256'].items()),'F03 snapshots wrong build')
    f03.append(result)
require({x['native'] for x in f03}=={'openai','anthropic'},'missing native replay')
for path in (BASE/'actual-01-isolated/native-f03').glob('*/report.json'):
    report=read(path);require(not report['passed'] and report['test_failed'] and 'snapshots' not in report and 'wire_requests' not in report,'initial preflight failure relabelled')

launcher=BASE/'actual-02-isolated/launcher.py'
require(sha(launcher)==read(BASE/'actual-02-isolated/execution-start.json')['launcher_sha256']==sha(ROOT/'internal/application/text_batch_acceptance.py'),'launcher identity mismatch')
require(read(BASE/'launcher-go-env-preflight/before.json')['exit_code']==1 and read(BASE/'launcher-go-env-preflight/after.json')['exit_code']==0,'Go env preflight classification')
result={'passed':True,'role':'author recheck of retained records, not another OS experiment or independent implementation review','base_commit':build['commit'],'source_hashes_checked':len(build['source_after']),'eight_owned_go_sources_and_other_sources_reconstructed_from_base':True,'fixed_binaries_verified':{k:v['sha256'] for k,v in build['binaries'].items()},'launcher_sha256':sha(launcher),'runs':summaries,'f03':f03,'limits':['PG event exports are phase-specific retained Store results, not a full historical schema dump.','The complete model turn was loaded from actual local storage in the test; only its decoded content is retained, not the original transient object bytes.','F03 uses actual local native HTTP/Driver/PG with synthetic prices and a recording runner; no paid inference or Docker.']}
result['f03_auditor_sha256']=sha(auditor_path)
result['f03_auditor_equals_a9208e7']=subprocess.check_output(['git','show',build['commit']+':benchmarks/audit_model_stream_fault.py'],cwd=ROOT)==auditor_path.read_bytes()
require(result['f03_auditor_equals_a9208e7'],'different F03 auditor')
if (BASE/'author-audit.json').exists():
    require(read(BASE/'author-audit.json')==result,'retained author summary differs from recomputation')
else:
    with (BASE/'author-audit.json').open('x') as f: json.dump(result,f,indent=2);f.write('\n')
files={str(p.relative_to(BASE)):sha(p) for p in BASE.rglob('*') if p.is_file() and p.name!='author-manifest.json' and '__pycache__' not in p.parts}
if (BASE/'author-manifest.json').exists():
    require(read(BASE/'author-manifest.json')['files']==files,'manifest differs from retained files')
else:
    with (BASE/'author-manifest.json').open('x') as f: json.dump({'algorithm':'sha256','scope':'before/build/actual/preflight records plus author audit and scripts; no credentials','files':files},f,indent=2);f.write('\n')
print(json.dumps({'passed':True,'source_count':len(build['source_after']),'manifest_files':len(files),'runs':summaries,'f03':f03},indent=2))

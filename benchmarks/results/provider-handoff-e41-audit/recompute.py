"""Recompute E41 claims from retained ledger, wire and exact artifact bytes."""
import argparse, base64, copy, datetime as dt, hashlib, json, pathlib

def instant(value):
    return dt.datetime.fromisoformat(value.replace('Z','+00:00'))

def verify_record(r):
    assert r['failed'] is False, 'test reported failure'
    run=r['run']; tenant,run_id=run['tenant_id'],run['id']
    assert [e['seq'] for e in r['events']]==list(range(1,len(r['events'])+1)), 'event sequence gap'
    assert {a['id'] for a in r['artifacts']}==set(r['object_bytes_base64']), 'artifact export incomplete'
    objects={}
    for a in r['artifacts']:
        raw=base64.b64decode(r['object_bytes_base64'][a['id']],validate=True)
        assert a['tenant_id']==tenant and a['run_id']==run_id and a['state']=='ready'
        assert len(raw)==a['byte_size'] and hashlib.sha256(raw).hexdigest()==a['sha256'], 'artifact bytes mismatch'
        try: objects[a['id']]=json.loads(raw)
        except (ValueError,UnicodeDecodeError): pass
    assert objects==r['objects'], 'parsed artifacts disagree with original bytes'
    attempts=r['attempts']; reservations={q['id']:q for q in r['reservations']}
    assert len(reservations)==len(attempts), 'attempt/obligation mismatch'
    exposure=0
    for a in attempts:
        assert a['tenant_id']==tenant and a['run_id']==run_id and a['request_ref'] in objects
        q=reservations[a['attempt_id']]
        assert q['tenant_id']==tenant and q['run_id']==run_id
        assert a['pricing']['provider']==a['provider'] and a['pricing']['model']==a['model_id']
        assert a['pricing']['price_version']==a['price_version']
        exposure+=q['actual_microusd'] if q['status']=='settled' else q['microusd']
        if a['status']=='failed': assert q['status']=='unknown' and q['actual_microusd'] is None, 'failed obligation refunded'
        if a['status']=='completed': assert a['raw_ref'] in objects
    assert exposure<=run['config_snapshot']['max_cost_microusd'], 'budget exceeded'
    handoffs=[e for e in r['events'] if e['type']=='model.handoff_prepared']
    if 'primary_wire' in r:
        primary,target=r['primary'],r['target']
        assert run['state']=='completed' and run['snapshot']['model_rounds']==3
        assert [(a['step_seq'],a['attempt'],a['provider'],a['status']) for a in attempts]==[(1,1,primary,'completed'),(2,1,primary,'failed'),(2,2,target,'completed'),(3,1,target,'completed')]
        assert len(handoffs)==1
        source,failed,switched,last=attempts
        assert failed['raw_ref'] is None and failed['error_code']=='stream_interrupted'
        prepared=r['prepared_before_crash']
        assert prepared['ID']==switched['attempt_id'] and prepared['RequestRef']==switched['request_ref']
        assert prepared['PriceVersion']==switched['price_version'] and instant(prepared['Deadline'])==instant(switched['deadline'])
        assert switched['price_version']=='local-exact-fixture-v1' and last['price_version']=='changed-after-preparation'
        assert handoffs[0]['payload']['from_attempt_id']==failed['attempt_id'] and handoffs[0]['payload']['to_attempt_id']==switched['attempt_id']
        old,portable=objects[failed['request_ref']],objects[switched['request_ref']]
        assert old['native_state']['provider']==primary and not portable.get('native_state')
        assert old['applied_message_seq']==portable['applied_message_seq']
        expected=copy.deepcopy(old['portable_messages']); pending={}; seq=0
        for message in expected:
            if message['role']!='tool': assert not pending, 'migrated open batch'
            for call in message.get('tool_calls',[]):
                seq+=1; assert call['id'] not in pending; pending[call['id']]='handoff_'+str(seq); call['id']=pending[call['id']]
            if message['role']=='tool': message['tool_call_id']=pending.pop(message['tool_call_id'])
        assert not pending and expected==portable['messages'], 'portable history or tool pairing changed'
        pw,tw=r['primary_wire'],r['target_wire']
        assert len(pw)==len(tw)==2
        assert [x['attempt_id'] for x in pw+tw]==[a['attempt_id'] for a in attempts]
        assert instant(tw[0]['at'])>=instant(failed['failure_policy']['not_before']), 'backoff bypassed'
        assert 'SOURCE_OPAQUE_SENTINEL' in json.dumps(pw[1]['body']) and 'SOURCE_OPAQUE_SENTINEL' not in json.dumps(tw[0]['body'])
        assert 'TARGET_OPAQUE_SENTINEL' in json.dumps(tw[1]['body'])
        assert r['failed_obligation_before_handoff']['microusd']==r['failed_obligation_after_handoff']['microusd']==reservations[failed['attempt_id']]['microusd']
        tools=[e for e in r['effects'] if e['ordinal']>=0]
        assert [(e['step_seq'],e['kind'],e['status']) for e in tools]==[(1,'read_file','succeeded'),(2,'apply_patch','succeeded')], 'provisional tool execution or missing committed tool'
        for effect in r['effects']: assert effect['receipt_ref'] in objects
    else:
        scenario=r['scenario']; primary,backup,attempt_count,handoff_count=3,0,3,0
        status='failed'
        if scenario=='compatible_fallback_fails': primary,backup,handoff_count=1,2,1
        if scenario=='unknown_cost_exhausts_budget':primary,attempt_count,status=1,1,'budget_exhausted'
        assert run['state']==status and run['snapshot']['model_rounds']==1
        assert r['primary_calls']==primary and len(r['backup_requests'] or [])==backup
        assert len(attempts)==attempt_count and len(handoffs)==handoff_count
        assert [a['attempt'] for a in attempts]==list(range(1,attempt_count+1)), 'retry budget reset'
    return {'run_id':run_id,'artifacts':len(objects),'all_artifact_bytes':len(r['artifacts']),'attempts':len(attempts),'handoffs':len(handoffs),'exposure_microusd':exposure,'state':run['state']}

def rejected(mutated):
    try: verify_record(mutated)
    except (AssertionError,KeyError,ValueError): return True
    return False

def main():
    p=argparse.ArgumentParser();p.add_argument('evidence',type=pathlib.Path);p.add_argument('--self-test',action='store_true');args=p.parse_args()
    root=args.evidence
    manifest=json.loads((root/'manifest.json').read_text())
    for name,sha in manifest.items(): assert hashlib.sha256((root/name).read_bytes()).hexdigest()==sha, name
    assert json.loads((root/'source-before.json').read_text())==json.loads((root/'source-after.json').read_text())
    assert all(r['exit_code']==0 for r in json.loads((root/'results.json').read_text()))
    records=[json.loads(path.read_text()) for path in sorted((root/'records').glob('*.json'))]
    assert len(records)==7
    results=[verify_record(r) for r in records]
    negatives={}
    if args.self_test:
        native=next(r for r in records if 'primary_wire' in r)
        changed=copy.deepcopy(native);changed['target_wire'][0]['body']['untrusted_extra']='SOURCE_OPAQUE_SENTINEL';negatives['foreign_native_leak']=rejected(changed)
        changed=copy.deepcopy(native);changed['attempts'][2]['attempt']=1;negatives['retry_count_reset']=rejected(changed)
        # Select the failed obligation explicitly, independent of SQL row order.
        changed=copy.deepcopy(native);failed_id=changed['attempts'][1]['attempt_id'];q=next(q for q in changed['reservations'] if q['id']==failed_id);q['status']='settled';q['actual_microusd']=0;negatives['unknown_fee_refund']=rejected(changed)
        changed=copy.deepcopy(native);first=next(iter(changed['object_bytes_base64']));changed['object_bytes_base64'][first]=base64.b64encode(b'tampered').decode();negatives['artifact_corruption']=rejected(changed)
        assert all(negatives.values())
    print(json.dumps({'passed':True,'original_manifest_files':len(manifest),'source_inputs':len(json.loads((root/'source-before.json').read_text())),'records':results,'negative_oracles':negatives},indent=2))

if __name__=='__main__':main()

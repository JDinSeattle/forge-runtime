"""Recompute E41 identities, portable wire, and fixture billing from raw bytes.

This is a bounded oracle for the retained zero-cache protocol fixtures, not a
general-purpose native protocol or invoice validator. The original v1 audit and
its reports remain immutable under benchmarks/results/provider-handoff-e41-audit.
"""
import argparse, base64, copy, datetime as dt, hashlib, json, pathlib

def instant(value):
    return dt.datetime.fromisoformat(value.replace('Z','+00:00'))

def verify_bindings(r, objects):
    for effect in r['effects']:
        receipt=objects[effect['receipt_ref']]; request=receipt['request']
        for key in ('tenant_id','run_id','operation_id','kind','args_hash','policy_version','epoch','expected_revision'):
            assert request[key]==effect[key], 'receipt identity mismatch: '+key
        assert request['workspace_id']==r['run']['id'], 'wrong receipt workspace'
        assert request['args']==effect['args'] and receipt['status']==effect['status'], 'receipt result or arguments mismatch'
        assert effect['tenant_id']==r['run']['tenant_id'] and effect['run_id']==r['run']['id']
        assert effect['canonical_args'].startswith('\\x'), 'missing canonical PG bytes'
        raw=bytes.fromhex(effect['canonical_args'][2:])
        assert hashlib.sha256(raw).hexdigest()==effect['args_hash'] and json.loads(raw)==request['args'], 'canonical argument hash mismatch'
    reservations={q['id']:q for q in r['reservations']}
    for attempt in r['attempts']:
        q=reservations[attempt['attempt_id']]; p=attempt['pricing']
        rate=max(p.get(k) or 0 for k in ('input_price','cache_read_price','cache_write_5m_price','cache_write_1h_price'))
        ceiling=(p['context_tokens']*rate+p['max_output_tokens']*p['output_price']+999999)//1000000
        assert q['microusd']==ceiling and q['tokens']==p['context_tokens']+p['max_output_tokens'], 'reservation differs from frozen quote'
        assert q['credential_group']==p['credential_group'] and instant(q['request_deadline'])==instant(attempt['deadline']), 'reservation identity differs'
        if attempt['status']=='completed':
            turn=objects[attempt['raw_ref']]; usage=turn['usage']
            assert turn['attempt_id']==attempt['attempt_id'] and turn['run_id']==attempt['run_id'] and turn['step_id']==str(attempt['step_seq']), 'model turn identity differs'
            assert usage==attempt['usage'], 'SQL usage differs from exact completed turn'
            assert p['exact_pricing'] and usage['final'] and usage['input']['known'] and usage['output']['known']
            # Fail closed outside this evidence set: these fixtures have no
            # cache usage; OpenAI has no cache-write dimension in this adapter.
            assert usage['cache_read']['known'] and usage['cache_read']['value']==0 and usage['cache_write']['value']==0, 'nonzero/unknown cache outside fixture oracle'
            if p['provider']=='anthropic': assert usage['cache_write']['known']
            assert p['provider'] in ('openai','anthropic')
            assert usage['input']['value']>=0 and usage['output']['value']>=0
            actual=(usage['input']['value']*p['input_price']+usage['output']['value']*p['output_price']+999999)//1000000
            assert q['status']=='settled' and q['actual_microusd']==actual, 'settled cost differs from frozen price and actual usage'
            assert q['actual_tokens']==usage['input']['value']+usage['output']['value'] and q['settlement_kind']=='actual_usage', 'settled usage differs'
    if 'primary_wire' not in r: return
    attempt=r['attempts'][2]; pricing=attempt['pricing']; body=r['target_wire'][0]['body']; envelope=objects[attempt['request_ref']]
    assert body['model']==attempt['model_id'] and body['stream'] is True, 'target wire model/stream mismatch'
    expected=[]; systems=[]
    if r['target']=='openai':
        assert set(body)=={'model','max_output_tokens','store','include','input','tools','stream'}, 'unexpected target wire fields'
        assert body['store'] is False and body['include']==['reasoning.encrypted_content']
        assert body['max_output_tokens']==pricing['max_output_tokens'], 'target output cap differs'
        for message in envelope['messages']:
            if message['role']=='tool':
                expected.append({'type':'function_call_output','call_id':message['tool_call_id'],'output':message.get('text','')})
            else:
                if message.get('text') or not message.get('tool_calls'):
                    expected.append({'role':message['role'],'content':message.get('text','')})
                for call in message.get('tool_calls',[]):
                    expected.append({'type':'function_call','call_id':call['id'],'name':call['name'],'arguments':call['arguments']})
        actual=copy.deepcopy(body['input'])
        for item in actual:
            if item.get('type')=='function_call': item['arguments']=json.loads(item['arguments'])
        assert actual==expected, 'actual target input differs from portable history'
        toolkey='parameters'
        for tool in body['tools']:
            assert set(tool)=={'name','description','parameters','type','strict'} and tool['type']=='function' and tool['strict'] is False
    else:
        assert r['target']=='anthropic'
        assert set(body)=={'model','max_tokens','messages','system','tools','stream'}, 'unexpected target wire fields'
        assert body['max_tokens']==pricing['max_output_tokens'], 'target output cap differs'
        for message in envelope['messages']:
            if message['role']=='system':
                systems.append({'type':'text','text':message.get('text','')}); continue
            blocks=[]
            if message['role']=='tool':
                blocks=[{'type':'tool_result','tool_use_id':message['tool_call_id'],'content':message.get('text',''),'is_error':message.get('is_error',False)}]
            else:
                if message.get('text'): blocks.append({'type':'text','text':message['text']})
                for call in message.get('tool_calls',[]):
                    blocks.append({'type':'tool_use','id':call['id'],'name':call['name'],'input':call['arguments']})
            expected.append({'role':'user' if message['role']=='tool' else message['role'],'content':blocks})
        assert body['system']==systems and body['messages']==expected, 'actual target input differs from portable history'
        toolkey='input_schema'
        for tool in body['tools']: assert set(tool)=={'name','description','input_schema'}
    assert len(body['tools'])==len(envelope['tools']), 'target tool catalog differs'
    for actual,want in zip(body['tools'],envelope['tools']):
        assert actual['name']==want['name'] and actual['description']==want['description'] and actual[toolkey]==want['schema'], 'target tool definition differs'

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
    verify_bindings(r,objects)
    return {'run_id':run_id,'artifacts':len(objects),'all_artifact_bytes':len(r['artifacts']),'attempts':len(attempts),'handoffs':len(handoffs),'exposure_microusd':exposure,'state':run['state'],'bound_receipts':len(r['effects']),'quotes_recomputed':len(attempts),'portable_target_wire_bound':'primary_wire' in r}

def rejected(mutated):
    try: verify_record(mutated)
    except (AssertionError,KeyError,ValueError,TypeError): return True
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
        for original in (r for r in records if 'primary_wire' in r):
            target=original['target']
            changed=copy.deepcopy(original);changed['target_wire'][0]['body']['model']='wrong-model';negatives[target+'_wrong_wire_model']=rejected(changed)
            changed=copy.deepcopy(original);changed['target_wire'][0]['body']['input' if target=='openai' else 'messages']=[];negatives[target+'_missing_wire_history']=rejected(changed)
            changed=copy.deepcopy(original);changed['target_wire'][0]['body']['tools']=[];negatives[target+'_missing_wire_tools']=rejected(changed)
            changed=copy.deepcopy(original);q=next(q for q in changed['reservations'] if q['id']==changed['attempts'][2]['attempt_id']);q['actual_microusd']=0;negatives[target+'_settled_fee_changed']=rejected(changed)
            changed=copy.deepcopy(original);q=next(q for q in changed['reservations'] if q['id']==changed['attempts'][2]['attempt_id']);q['actual_tokens']=0;negatives[target+'_settled_tokens_changed']=rejected(changed)
            changed=copy.deepcopy(original);q=next(q for q in changed['reservations'] if q['id']==changed['attempts'][1]['attempt_id']);q['microusd']-=1;negatives[target+'_unknown_quote_changed']=rejected(changed)
            changed=copy.deepcopy(original);effect=next(e for e in changed['effects'] if e['kind']=='read_file');effect['receipt_ref']=next(e['receipt_ref'] for e in changed['effects'] if e['kind']=='verify');negatives[target+'_receipt_substitution']=rejected(changed)
            changed=copy.deepcopy(original);effect=next(e for e in changed['effects'] if e['kind']=='read_file');effect['epoch']+=1;negatives[target+'_receipt_epoch_changed']=rejected(changed)
            changed=copy.deepcopy(original);effect=next(e for e in changed['effects'] if e['kind']=='read_file');effect['canonical_args']='\\x7b7d';negatives[target+'_canonical_args_changed']=rejected(changed)
        assert all(negatives.values())
    print(json.dumps({'passed':True,'audit_version':2,'cost_oracle_scope':'retained zero-cache fixtures only; other cache usage fails closed','original_manifest_files':len(manifest),'source_inputs':len(json.loads((root/'source-before.json').read_text())),'records':results,'negative_oracles':negatives},indent=2))

if __name__=='__main__':main()

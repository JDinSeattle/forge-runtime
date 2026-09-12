"""Offline counterexamples for the explicit evidence consumer. No live services."""
import copy, hashlib, importlib.util, json, os, sqlite3, struct, tempfile, unittest, zlib
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
SPECS={}
for name in ('isolated_checks','isolated_state','isolated_composite'):
 spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(name+'.py'));mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);SPECS[name]=mod
c=SPECS['isolated_checks'];s=SPECS['isolated_state'];m=SPECS['isolated_composite']

def digest(raw):return hashlib.sha256(raw).hexdigest()
def frame(stream,seq,body):return b'FLG1'+bytes([stream,0,0,0])+struct.pack('>QII',seq,len(body),zlib.crc32(body))+b'\0'*8+body
RAW=frame(1,0,b'out\x00\xff')+frame(2,1,b'err\x00\xfe')

def write(p,value):
 p.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 p.write_bytes(value if isinstance(value,bytes) else json.dumps(value).encode());p.chmod(0o600);return p

def journal():
 return {'identity':m.UUID,'version':5,'tables':{k:([{'id':'slot-'+str(i)} for i in range(4)] if k=='volume_slots' else []) for k in m.TABLES}}

class CompositeTests(unittest.TestCase):
 def test_frames_recompute_binary_crc_and_retain_only_valid_prefix(self):
  self.assertEqual(m.frames(RAW),RAW)
  self.assertEqual(m.frames(RAW+b'FLG1\x02'),RAW)
  for bad in (RAW[:37],RAW.replace(b'out',b'bad'),frame(1,0,b'only'),frame(1,1,b'bad')+frame(2,2,b'bad')):
   with self.subTest(raw_hash=digest(bad)),self.assertRaises(ValueError):m.frames(bad)
 def test_manifest_refuses_credentials_before_member_digest(self):
  with tempfile.TemporaryDirectory() as tmp:
   base=Path(tmp);path=write(base/'manifest.json',{'database.json':'a'*64});write(base/'database.json',{'not_a_real_secret':True})
   seen=[]
   with patch.object(c,'sha',side_effect=lambda p:seen.append(p) or digest(p.read_bytes())),self.assertRaises(ValueError):m.archive(c,base,kind='recovery')
   self.assertEqual(seen,[path])
 def test_manifest_requires_exact_members_and_immutable_bytes(self):
  with tempfile.TemporaryDirectory() as tmp:
   base=Path(tmp);p=write(base/'report.json',{'passed':False});write(base/'manifest.json',{'report.json':c.sha(p)})
   self.assertEqual(len(m.archive(c,base,kind='recovery')),2)
   write(base/'surprise.json',{});
   with self.assertRaises(ValueError):m.archive(c,base,kind='recovery')
   (base/'surprise.json').unlink();write(p,{'passed':True})
   with self.assertRaises(ValueError):m.archive(c,base,kind='recovery')
 def test_explicit_generated_path_grammar(self):
  for kind,paths in [('recovery',['runner-01-default.log','runner-01-stop.json','artifacts/art_a.bytes','pressure.gz']),('target',['L4/worker-finished-signal.json','L4/artifacts/art_a.json','L4-physical-full.meta.tmp','L4-health-log.flg','config-default.json','config-bytes.json','config-count.json'])]:
   for path in paths:self.assertTrue(m.generated_member(path,kind),path)
  for path in ('../runner.key','L4/database.json','runner.env','/etc/passwd','arbitrary.json','L5/fixture.json','L4/artifacts/../runner.key'):
   self.assertFalse(m.generated_member(path,'target'),path)
 def test_closed_input_path_does_not_read_private_keys(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);secret=scope/'runtime/runner.key';before={str(secret):'a'*64}
   with patch.object(c,'sha') as call,self.assertRaises(ValueError):m.closed_inputs(c,scope,{'input_sha256_before':before,'input_sha256_after':before},{})
   call.assert_not_called()
 def test_journal_ignores_only_row_order_and_json_serialization(self):
  a=journal();a['tables']['operations']=[{'id':'a','status':'failed','request_json':'{"grant":"","args":{"b":2,"a":1}}'}]
  b=copy.deepcopy(a);b['tables']['volume_slots'].reverse();b['tables']['operations'][0]['request_json']='{"args": {"a": 1, "b": 2}}'
  self.assertEqual(m.canonical_journal(a),m.canonical_journal(b))
  for field,value in [('status','unknown'),('request_json','{"grant":"capability-must-not-be-hashed"}')]:
   x=copy.deepcopy(a);x['tables']['operations'][0][field]=value
   if field=='status':
    with self.assertRaises(ValueError):m.idle(x)
   else:
    with self.assertRaises(ValueError):m.canonical_journal(x)
 def test_idle_refuses_each_unsettled_resource(self):
  good=journal();m.idle(good)
  for table,row in [('operations',{'status':'planned'}),('operations',{'status':'unknown'}),('workspaces',{'released':0,'active_operation':''}),('workspaces',{'released':1,'active_operation':'op'}),('volume_leases',{'released':0}),('operation_logs',{'cleanup_state':'retained'})]:
   j=copy.deepcopy(good);j['tables'][table].append(row)
   with self.subTest(table=table,row=row),self.assertRaises(ValueError):m.idle(j)
  for field,value in [('identity','a'*64),('version',4)]:
   with self.assertRaises(ValueError):m.idle(dict(good,**{field:value}))
 def test_live_baseline_readonly_exact_rows_and_missing_file(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);path=scope/'runtime/journal.sqlite';path.parent.mkdir();con=sqlite3.connect(path)
   con.execute('PRAGMA user_version=5');con.execute('CREATE TABLE journal_identity(singleton INTEGER,id TEXT)');con.execute('INSERT INTO journal_identity VALUES (1,?)',(m.UUID,))
   j=journal()
   for table in sorted(m.TABLES):
    con.execute('CREATE TABLE '+table+'(id TEXT)')
    if table=='volume_slots':con.executemany('INSERT INTO volume_slots VALUES (?)',[(x['id'],) for x in j['tables'][table]])
   con.commit();write(scope/'runtime/runner.json',{'journal_path':str(path)});write(scope/'evidence/logs-l4-01/final-journal.json',j)
   before=path.read_bytes();self.assertTrue(m.live_baseline(c,scope)['same_rows']);self.assertEqual(path.read_bytes(),before)
   con.execute('UPDATE volume_slots SET id=? WHERE id=?',('other','slot-0'));con.commit();con.close()
   with self.assertRaises(ValueError):m.live_baseline(c,scope)
   path.unlink()
   with self.assertRaises(ValueError):m.live_baseline(c,scope)
   self.assertFalse(path.exists())
 def test_compatibility_includes_proto_and_binary_hashes(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);rev='a'*40
   def acceptance(revision):
    a={'purpose':'worker-runner-sigterm-v1','runner_config':str(scope/'runtime/runner.json')}
    for key,name in [('runner','forge-runner'),('worker','forge-worker'),('test','application-faults.test')]:
     path=write(scope/'bin'/revision/name,b'fixture-executable');a[key+'_binary']=str(path);a[key+'_sha256']=c.sha(path)
    return a
   old=acceptance(m.REVISION);new=acceptance(rev)
   tree='\0'.join('100644 blob '+('a'*40)+'\t'+p+('/file.go' if '.' not in p else '') for p in m.PRODUCTION)+'\0'
   calls=[]
   with patch.object(c,'output',side_effect=lambda argv:calls.append(argv) or tree),patch.object(m,'binary_build',return_value={}):proof=m.compatibility(c,scope,old,[('targeted-L4',new)])
   self.assertFalse(proof['original_aggregate_passed']);self.assertTrue(all('proto' in argv for argv in calls));self.assertEqual(proof['phases'][1]['revision'],rev)
   def changed(argv):return tree.replace('a'*40,'b'*40,1) if rev in argv else tree
   with patch.object(c,'output',side_effect=changed),patch.object(m,'binary_build',return_value={}),self.assertRaises(ValueError):m.compatibility(c,scope,old,[('targeted-L4',new)])
   new['worker_sha256']='f'*64
   with patch.object(c,'output',return_value=tree),patch.object(m,'binary_build',return_value={}),self.assertRaises(ValueError):m.compatibility(c,scope,old,[('targeted-L4',new)])
 def test_missing_actual_composite_cannot_use_old_summary_as_fallback(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);write(scope/'logs-l4-continuation.json',{'purpose':'strict-logs-targeted-l4-v1','recovered_run_id':m.RUN,'failed_manifest_sha256':m.FAILED_SHA,'observation_manifest_sha256':m.OBS_SHA})
   with patch.object(c,'historical_inputs',return_value={}),self.assertRaises(FileNotFoundError):m.prior(c,scope)
   with patch.object(c,'composite_evidence',side_effect=ValueError('incomplete')) as call,self.assertRaises(ValueError):c.prior_evidence(scope)
   call.assert_called_once_with(scope)
 def operation_fixture(self,base,label,healthy=False):
  request={'tenant_id':'tenant','run_id':'run','workspace_id':'workspace','operation_id':'op','epoch':2,'grant':'','deadline':'2026-09-12T01:01:00Z'}
  def ref(kind,raw):return {'tenant_id':'tenant','run_id':'run','kind':kind,'sha256':digest(raw),'size':len(raw),'object_key':'tenant/run/'+digest(raw)}
  log={'complete':healthy,'retained_bytes':len(RAW),'dropped_known':healthy,'dropped_bytes':0,'artifact':ref('operation_log',RAW)}
  job={'id':'forge-original','log':log};op={'request':request,'status':'succeeded' if healthy else 'failed','job_id':'forge-original','result':job}
  receipt=json.dumps(op).encode();op['receipt']=ref('operation_receipt',receipt)
  j=journal();j['tables']['operations']=[{'id':'op','request_json':json.dumps(request),'status':op['status'],'job_id':'forge-original'}];j['tables']['operation_logs']=[{'operation_id':'op','container_id':'c'*64}]
  docker=[{'Id':'c'*64,'Name':'/forge-original','LogPath':'','State':{'Running':False,'OOMKilled':False},'HostConfig':{'LogConfig':{'Type':'none'},'NetworkMode':'none','ReadonlyRootfs':True},'Config':{'Labels':{'forge.operation_id':'op','forge.runtime':'1'}}}]
  for suffix,value in [('request.json',request),('operation.json',op),('receipt.json',receipt),('log.flg',RAW),('journal.json',j),('docker.json',docker),('disk-identity.json',{'spool_device':1,'checkout_device':1,'spool_inode':7})]:write(base/(label+'-'+suffix),value)
  return request,op,log,j,docker
 def test_receipt_not_rpc_metadata_is_authority_and_health_is_actual(self):
  with tempfile.TemporaryDirectory() as tmp:
   base=Path(tmp);req,op,log,j,dc=self.operation_fixture(base,'L4-recovered')
   self.assertEqual(m.operation(c,base,'L4-recovered',success=False)[0],req)
   for mutate in (lambda o:o['request'].update(operation_id='different'),lambda o:o['result']['log'].update(complete=True),lambda o:o['result']['log']['artifact'].update(run_id='foreign'),lambda o:o['receipt'].update(size=1)):
    bad=copy.deepcopy(op);mutate(bad);write(base/'L4-recovered-operation.json',bad)
    with self.assertRaises(ValueError):m.operation(c,base,'L4-recovered',success=False)
   write(base/'L4-recovered-operation.json',op)
   with self.assertRaises(ValueError):m.operation(c,base,'L4-recovered',success=True)
   dc[0]['Id']='d'*64;write(base/'L4-recovered-docker.json',dc)
   with self.assertRaises(ValueError):m.operation(c,base,'L4-recovered',success=False)
 def test_targeted_summary_without_physical_health_publication_cannot_pass(self):
  with tempfile.TemporaryDirectory() as tmp:
   base=Path(tmp)
   for body in ({'passed':False,'cases':{'L4':{'passed':True}}},{'passed':True,'cases':{}},{'passed':True,'cases':{'L4':{'passed':True},'L1':{'passed':True}}}):
    write(base/'acceptance.json',body)
    with self.assertRaises(ValueError):m.targeted(c,base,journal())
   write(base/'acceptance.json',{'passed':True,'cases':{'L4':dict(passed=True,physical_errno='ENOSPC',actual_spool_failure_observed=True,pressure_removed_after_actual_stop=True,same_id_inspection_only=True,all_four_volume_identities_reverified=True,fresh_real_health_operation=True)}})
   with self.assertRaises(FileNotFoundError):m.targeted(c,base,journal())
 def test_complete_targeted_material_and_cross_layer_counterexamples(self):
  # Synthetic public records exercise the consumer's cross-layer connections;
  # operation() separately verifies real encoded receipt/ref/frame contracts.
  with tempfile.TemporaryDirectory() as tmp:
   base=Path(tmp);req,op,log,j,dc=self.operation_fixture(base,'L4-recovered');op['job_id']='forge-original'
   j['tables']['operations'][0].update(dispatch_started=1,docker_start_intent=1)
   slot={'id':'slot-0','spec_json':json.dumps({'mount_path':'/fixture-slot'})};j['tables']['volume_slots'][0]=slot
   j['tables']['volume_leases']=[{'workspace_id':'workspace','slot_id':'slot-0','released':0}]
   healthy_req=dict(req,operation_id='health',workspace_id='health-workspace',run_id='health-run')
   final=copy.deepcopy(j);final['tables']['operations'].append(dict(j['tables']['operations'][0],id='health',status='succeeded'))
   final['tables']['operation_logs'].append({'operation_id':'health','container_id':'h'*64})
   dc[0]['State'].update(StartedAt='2026-09-12T01:00:00Z',FinishedAt='2026-09-12T01:00:05Z',ExitCode=137)
   art={k:v for k,v in log['artifact'].items() if k!='size'};art.update(id='art_log',state='ready',byte_size=len(RAW),created_at='2026-09-12T01:00:06Z')
   public={
    'acceptance.json':{'passed':True,'cases':{'L4':dict(passed=True,physical_errno='ENOSPC',actual_spool_failure_observed=True,pressure_removed_after_actual_stop=True,same_id_inspection_only=True,all_four_volume_identities_reverified=True,fresh_real_health_operation=True)}},
    'L4/fixture.json':{'tenant':'tenant','run_id':'run','target_operation':'op'},
    'L4-pressure.json':dict(stages=[dict(chunk_bytes=n,written_bytes=1,error='no space left on device',synced=True) for n in (1<<20,4096,1)],fill_error='<nil>',owner_uid=0,effective_uid=0,written_bytes=3,allocated_bytes=4096,after={'Bavail':0},device=1,inode=77,path='/fixture-slot/workspace-workspace/strict-logs-enospc-pressure'),
    'L4-pressure-removed.json':dict(device=1,inode=77,only_after_confirmed_job_stop=True,at='2026-09-12T01:00:06Z'),
    'L4-stopped-before-pressure-removal.json':dc,
    'L4-worker-pause.json':dict(stopped_confirmed_at='2026-09-12T01:00:01Z',continued_at='2026-09-12T01:00:07Z',watchdog=False,lease_epoch=2),
    'L4-before-pressure-journal.json':j,
    'L4/physical-full-postgres.json':{'runs':[{'id':'run','state':'running','lease_epoch':2}]},
    'L4-http-proof.json':dict(artifact=art,status=200,list_status=200,listed_matches=1,cross_tenant_download=404,cross_tenant_list=404,body_sha256=digest(RAW),body_bytes=len(RAW)),
    'L4/cleanup-postgres.json':{'artifacts':[art,{'id':'receipt','state':'ready','sha256':op['receipt']['sha256']}],'effects':[{'operation_id':'op','status':'failed','epoch':2,'receipt_ref':'receipt'}]},
    'L4/closure-oracle.json':dict(ready_receipt_joined_effects=1,durable_effect_confirmations=1,tenant_active=0,runner_reserved=0,unreleased_allocations=0,provider_active_requests=0),
    'L4/cleanup.json':dict(tenant_id='tenant',run_id='run',phase='released',snapshot_ref='code'),
    'L4-health-stop.json':{'no_active_operations':True},'L4-health-snapshot.json':{'artifact':{'kind':'workspace_snapshot'}},'L4-health-release.json':{'released':True}}
   failure=copy.deepcopy(j);failure['tables']['operations'][0]['error']='no space left on device';public['L4-spool-failure-journal.json']=failure
   for name,body in public.items():write(base/name,body)
   for name in ('L4-before-pressure.spool','L4-physical-full.spool','L4-http.flg'):write(base/name,RAW)
   def calls(c,b,label,success):return (healthy_req,{}, {},{}, {}) if success else (req,op,log,j,dc[0])
   with patch.object(m,'operation',side_effect=calls):m.targeted(c,base,final)
   changes=[
    ('L4-pressure.json',lambda x:x['after'].update(Bavail=1)),
    ('L4-pressure.json',lambda x:x['stages'][-1].update(chunk_bytes=4096)),
    ('L4-pressure.json',lambda x:x.update(path='/other-tenant/pressure')),
    ('L4-worker-pause.json',lambda x:x.update(watchdog=True)),
    ('L4-stopped-before-pressure-removal.json',lambda x:x[0]['State'].update(OOMKilled=True)),
    ('L4-stopped-before-pressure-removal.json',lambda x:x[0]['State'].update(ExitCode=0)),
    ('L4-stopped-before-pressure-removal.json',lambda x:x[0]['State'].update(FinishedAt='2026-09-12T01:00:24Z')),
    ('L4/physical-full-postgres.json',lambda x:x['runs'][0].update(state='cancel_requested')),
    ('L4-spool-failure-journal.json',lambda x:x['tables']['operation_logs'][0].update(container_id='r'*64)),
    ('L4-spool-failure-journal.json',lambda x:x['tables']['operations'][0].update(error='unrelated failure')),
    ('L4-http-proof.json',lambda x:x['artifact'].update(run_id='foreign')),
    ('L4/cleanup-postgres.json',lambda x:x['effects'][0].update(receipt_ref='missing')),
    ('L4/closure-oracle.json',lambda x:x.update(tenant_active=1)),
    ('L4-health-release.json',lambda x:x.update(released=False))]
   for name,change in changes:
    bad=copy.deepcopy(public[name]);change(bad);write(base/name,bad)
    with self.subTest(path=name),patch.object(m,'operation',side_effect=calls),self.assertRaises(ValueError):m.targeted(c,base,final)
    write(base/name,public[name])
 def test_build_info_requires_actual_clean_revision_and_package(self):
  p=Path('/frozen/forge-runner');rev='a'*40
  raw=str(p)+': go1.26.8\n\tpath\tgithub.com/JDinSeattle/forge-runtime/cmd/forge-runner\n'+''.join('\tbuild\t'+k+'='+v+'\n' for k,v in [('GOOS','linux'),('GOARCH','amd64'),('vcs.revision',rev),('vcs.modified','false')])
  with patch.object(c,'output',return_value=raw):self.assertEqual(m.binary_build(c,p,rev)['vcs.revision'],rev)
  for value in (raw.replace(rev,'b'*40),raw.replace('modified=false','modified=true'),raw.replace('cmd/forge-runner','cmd/forge-worker'),raw.replace('go1.26.8','go1.26.7')):
   with patch.object(c,'output',return_value=value),self.assertRaises(ValueError):m.binary_build(c,p,rev)
 def test_original_schema_oid_snapshot_and_history_are_retained(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);raw={'id':m.RUN,'tenant_id':m.TENANT,'state':'cancelled','version':9,'lease_epoch':3,'lease_owner':'','workspace_id':m.RUN,'snapshot':{'limits':{'deadline':'2026-09-12T00:00:00Z'}}}
   saved={'runs':[raw],'effects':[{'status':'failed'}],'model_attempts':[],'artifacts':[]}
   write(scope/'evidence/logs-03-recovery/postgres-after.json',saved);write(scope/'evidence/logs-03-retained-observation/postgres.json',{'runs':[raw]})
   row={'schema':m.SCHEMA,'schema_oid':m.OID,'runs':[dict(id=m.RUN,tenant=m.TENANT,state='cancelled',version=9,epoch=3,owner='',workspace=m.RUN,snapshot=raw['snapshot'])],'effects':1,'attempts':0,'artifacts':0}
   m.database_closure(c,scope,row,'strict-logs-private-03')
   for mutate in (lambda r:r.update(schema_oid=m.OID+1),lambda r:r['runs'][0].update(state='completed'),lambda r:r.update(effects=2),lambda r:r['runs'][0]['snapshot']['limits'].update(deadline='2030-01-01T00:00:00Z')):
    bad=copy.deepcopy(row);mutate(bad)
    with self.assertRaises(ValueError):m.database_closure(c,scope,bad,'strict-logs-private-03')
   saved['effects'][0]['status']='planned';write(scope/'evidence/logs-03-recovery/postgres-after.json',saved)
   with self.assertRaises(ValueError):m.database_closure(c,scope,row,'strict-logs-private-03')
 def test_sixth_schema_uses_only_targeted_public_role_and_fixture(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);write(scope/'logs-l4-continuation.json',{});rows={}
   names=['sigterm-private','sigterm-private-02','strict-logs-private','strict-logs-private-02','strict-logs-private-03','strict-logs-l4-private-01']
   for idx,name in enumerate(names):
    schema=('appfault_lifecycle_' if name.startswith('sigterm') else 'appfault_strictlogs_')+str(idx);role='fixture_role_'+str(idx)
    write(scope/'runtime'/name/'database.json',{'schema':schema,'worker_dsn':'postgres://'+role+':synthetic@127.0.0.1:32773/forge?sslmode=disable&search_path='+schema})
    attempt='03' if name.endswith('-03') else '02' if name.endswith('-02') else '01'
    base=scope/'evidence'/('sigterm-'+attempt if name.startswith('sigterm') else 'logs-'+attempt)
    if name.startswith('sigterm'):base=base/'worker-runner-sigterm'
    if idx==5:base=scope/'evidence/logs-l4-01'
    write(base/'worker-role-preflight.json',dict(schema=schema,current_user=role,session_user=role,production_CheckWorkerRole='passed'))
    row={k:0 for k in ('unsettled_effects','active','runner_reserved','allocations','requests','unsettled_reservations','cleanup','effects','attempts','artifacts')};row.update(schema=schema,runs=[dict(id='run_'+str(idx),tenant='tenant_'+str(idx),state='completed')]);rows[schema]=row
    if idx==0:
     state={'status':'cancel_requested'};row['runs'][0].update(state='cancel_requested',version=2,epoch=0,owner='',workspace='',snapshot=state)
     write(scope/'evidence/sigterm-01/abort-before-runner/report.json',dict(passed=True,terminal=False,status='cancel_requested',schema=schema,run_id='run_0',after={'state':state}))
    else:
     write(base/('fixture.json' if idx==1 else ('L4/fixture.json' if idx==5 else 'L1/fixture.json')),{'run_id':'run_'+str(idx),'tenant':'tenant_'+str(idx)})
     if idx>1:write(base/'private-schema.json',{'schema':schema})
   recorded=[]
   with patch.object(s,'second_unstarted'),patch.object(s,'sql_json',side_effect=lambda q,e,sch:rows[sch]),patch.object(s.c,'composite_module',return_value=SimpleNamespace(database_closure=lambda c,sc,r,n:recorded.append(n))):
    self.assertEqual(len(s.previous_databases(scope,{})),6)
   self.assertEqual(recorded,['strict-logs-private-03','strict-logs-l4-private-01'])
   write(scope/'evidence/logs-l4-01/worker-role-preflight.json',dict(schema='appfault_strictlogs_5',current_user='foreign_role',session_user='foreign_role',production_CheckWorkerRole='passed'))
   with patch.object(s,'second_unstarted'),patch.object(s,'sql_json',side_effect=lambda q,e,sch:rows[sch]),patch.object(s.c,'composite_module',return_value=SimpleNamespace(database_closure=lambda *args:None)),self.assertRaises(ValueError):s.previous_databases(scope,{})
 def test_live_pg_query_counts_planned_as_unsettled(self):
  self.assertIn("status NOT IN ('succeeded','failed','cancelled')",s.QUERIES)
 def test_process_identity_records_include_runner_exit_proofs(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);write(scope/'logs-l4-continuation.json',{})
   write(scope/'evidence/logs-03-recovery/runner-01-stop.json',{'pid':22222,'proc_start_ticks':'111'})
   with patch.object(s,'process_identity',return_value={'proc_start_ticks':'111'}),self.assertRaises(ValueError):s.prior_processes(scope)
   with patch.object(s,'process_identity',side_effect=FileNotFoundError),patch.object(s.c,'composite_evidence',return_value=(m.UUID,{},{})),patch.object(s.c,'composite_module',return_value=SimpleNamespace(live_baseline=lambda c,s:{})):
    self.assertEqual(len(s.prior_processes(scope)['processes']),1)

if __name__=='__main__':unittest.main()

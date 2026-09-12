"""Offline launcher guards. All service/PG/Docker/provider boundaries are mocked."""
import contextlib, copy, errno, importlib.util, json, os, subprocess, tempfile, types, unittest
from pathlib import Path
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('isolated',Path(__file__).with_name('isolated.py'));i=importlib.util.module_from_spec(spec);spec.loader.exec_module(i);s=i.s;c=i.c

class IsolatedTests(unittest.TestCase):
 def write(self,p,v):
  p.parent.mkdir(parents=True,exist_ok=True,mode=0o700);p.write_text(json.dumps(v) if not isinstance(v,str) else v);p.chmod(0o600);return p
 def idle(self):
  return dict(schema='eval_ds_'+'a'*32,schema_oid=17,now='2026-09-12T00:00:00Z',runs=[],effects=0,attempts=0,artifacts=0,projects=0,unsettled_effects=0,active=0,runner_reserved=0,allocations=0,requests=0,unsettled_reservations=0,cleanup=0,quotas=[])
 def history_files(self,scope):
  for name in ('sigterm-01','sigterm-02','logs-01','logs-02','logs-03'):
   base=scope/'evidence'/name
   if name.startswith('sigterm'):base=base/'worker-runner-sigterm'
   self.write(base/'worker-role-preflight.json',{'schema':'synthetic'})
   if name=='sigterm-02':self.write(base/'fixture.json',{'run_id':'synthetic'})
   if name.startswith('logs'):
    self.write(base/'private-schema.json',{'schema':'synthetic'});self.write(base/'L1/fixture.json',{'run_id':'synthetic'})
    if name=='logs-03':self.write(base/'L4/fixture.json',{'run_id':'synthetic'})
 def test_private_environment_plain_client_and_shlex_dsn(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'env';self.write(p,"FORGE_DATABASE_URL='postgres://u:p@127.0.0.1:32773/forge?sslmode=disable&search_path=exact'\n")
   got=c.literal_env(p,{'FORGE_DATABASE_URL'},quoted=True);self.assertEqual(got['FORGE_DATABASE_URL'].split('?')[1],'sslmode=disable&search_path=exact')
   self.write(p,'FORGE_TENANT=eval_fixture\n');self.assertEqual(c.literal_env(p,{'FORGE_TENANT'},quoted=False),{'FORGE_TENANT':'eval_fixture'})
   for body in ('FORGE_TENANT=one\nFORGE_TENANT=two\n','DEEPSEEK_API_KEY=unexpected\n','FORGE_TENANT=\n','FORGE_TENANT="embedded\nnewline"\n'):
    self.write(p,body)
    with self.assertRaises(ValueError):c.literal_env(p,{'FORGE_TENANT'},quoted=True)
   p.chmod(0o644)
   with self.assertRaises(ValueError):c.literal_env(p,{'FORGE_TENANT'},quoted=False)
 def test_audit_dsn_refuses_every_routing_override_and_leaks_no_environment(self):
  role='eval_ds_'+'a'*32+'_audit';dsn='postgres://'+role+':synthetic@127.0.0.1:32773/forge?sslmode=disable'
  with patch.dict(os.environ,{'DEEPSEEK_API_KEY':'never-forward','PGSERVICE':'other','PGOPTIONS':'other'},clear=True):env=c.dsn_env(dsn,role)
  self.assertEqual(set(env),{'PATH','LC_ALL','TMPDIR','PGHOST','PGPORT','PGDATABASE','PGUSER','PGPASSWORD','PGSSLMODE','PGCONNECT_TIMEOUT','PGOPTIONS'})
  for bad in (dsn+'&sslmode=disable',dsn+'&search_path=public',dsn.replace('sslmode=disable','%73slmode=disable'),dsn+'#frag',dsn.replace(':32773',':5432'),dsn.replace('/forge?','/postgres?'),dsn.replace(role+':','other:'),dsn.replace('127.0.0.1','localhost')):
   with self.subTest(dsn_shape=bad.split('@')[-1]),self.assertRaises(ValueError):c.dsn_env(bad,role)
 def test_runtime_dsn_matches_actual_provisioner_shape(self):
  schema='eval_ds_'+'a'*32;role=schema+'_worker';dsn='postgres://'+role+':synthetic@127.0.0.1:32773/forge?application_name=forge-eval-provision&connect_timeout=5&options=&search_path='+schema+'&sslmode=disable'
  self.assertEqual(c.dsn_env(dsn,role,schema=schema)['PGUSER'],role)
  for bad in (dsn.replace('options=','options=--search-path=public'),dsn.replace('search_path='+schema,'search_path=public'),dsn+'&host=localhost'):
   with self.assertRaises(ValueError):c.dsn_env(bad,role,schema=schema)
 def test_exact_profile_reconstruction_and_authority(self):
  scope=Path('/dedicated/var/lifecycle-rehearsals/lr20260912_a');runner={'sources':{},'profiles':{},'allow_test_backend':False,'logs':{'entry_bytes':16384,'operation_bytes':524288,'run_bytes':16777216,'max_operations':32,'preview_bytes':65536},'server':{'unix_socket':'/tmp/dedicated.sock'},'artifact_root':str(scope/'runtime/artifacts'),'signing_key_file':str(scope/'runtime/runner.key')}
  registration={'provider':'deepseek','model_id':'deepseek-v4-flash','total_budget_microusd':2000000,'task_budgets_microusd':{x:500000 for x in c.common.CASES},'max_model_rounds':20,'max_tool_calls':64,'max_runtime_seconds':600,'model_spec':{'exact_pricing':False,'credential_group':'deepseek-flash-evalv2','price_version':'deepseek-flash-peak-ceiling-20260912'},'capabilities':{'tool_calling':True},'config_id':'eval-config'}
  platform={'sources':{},'configs':{'eval-config':{'provider':'deepseek','model':'deepseek-v4-flash','max_model_rounds':20,'max_tool_calls':64,'max_cost_microusd':500000,'max_runtime_seconds':600}},'models':{'deepseek/deepseek-v4-flash':registration['model_spec']},'providers':{'deepseek':{'deepseek-v4-flash':registration['capabilities']}},'worker_slots':1,'runner_id':'application-fault-runner','runner':{'unix_socket':'/tmp/dedicated.sock','tls':{'cert_file':'','key_file':'','ca_file':'','expected_peer_uri':''}},'artifact_root':runner['artifact_root'],'signing_key_file':runner['signing_key_file']}
  for case in c.common.CASES:
   identity=c.common.SOURCE_PREFIX+case;source=str(c.common.CORPUS/case/'source');runner['sources'][identity]=source;platform['sources'][identity]={'path':source,'hash':'a'*64,'profile_id':identity,'has_target':True}
   runner['profiles'][identity]={'id':identity,'image':c.PYTHON_IMAGE if case.startswith('py-') else c.GO_IMAGE,'verify_command':c.common.grade_command(case,'regression'),'target_command':c.common.grade_command(case,'target'),'tmpfs_executable':case.startswith('go-'),'trusted_tests_dir':str(c.common.CORPUS/'graders'),'memory_bytes':256<<20,'workspace_quota_bytes':256<<20,'cpus':1,'pids':64,'user':'1000:1000'}
  original=dict(runner,sources={'lifecycle':'original'},profiles={'lifecycle':'original'})
  with patch.object(c,'read_json',return_value=original):
   c.policy(platform,runner,registration,scope)
   first=next(iter(runner['profiles']))
   mutations=[lambda p,r:g_set(r['profiles'][first],'verify_command',['sh','-c','untrusted']),lambda p,r:g_set(r['profiles'][first],'trusted_tests_dir','/foreign'),lambda p,r:g_set(r['profiles'][first],'image','python:latest'),lambda p,r:g_set(r,'signing_key_file','/new-key'),lambda p,r:g_set(r['logs'],'run_bytes',1<<30),lambda p,r:g_set(p,'worker_slots',2),lambda p,r:g_set(p['configs']['eval-config'],'fallback',{'provider':'openai'}),lambda p,r:g_set(p['runner']['tls'],'key_file','/foreign-key')]
   for mutate in mutations:
    p,r=copy.deepcopy(platform),copy.deepcopy(runner);mutate(p,r)
    with self.assertRaises(ValueError):c.policy(p,r,registration,scope)
 def test_existing_socket_is_never_unlinked(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'stale.sock';p.write_bytes(b'retained')
   with self.assertRaises(ValueError):s.socket_absent(p)
   self.assertEqual(p.read_bytes(),b'retained')
 @patch.object(c,'historical_inputs',return_value={})
 def test_historical_cleanup_and_six_case_input_hash_gates(self,_history):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);authority=self.write(scope/'runtime/runner.json',{});uuid='a'*64
   self.history_files(scope)
   sig=self.write(scope/'evidence/sigterm-02/worker-runner-sigterm/acceptance.json',{'passed':True,'journal_uuid':uuid})
   self.write(scope/'evidence/logs-03/final-journal.json',{'identity':uuid,'version':5})
   cleanup=self.write(scope/'evidence/logs-01-cleanup/report.json',{'passed':True,'released':True,'snapshot_verified':True})
   body={'passed':True,'cases':{k:{'passed':True} for k in c.CASE_KEYS},'input_sha256_before':{str(authority):c.sha(authority)},'input_sha256_after':{str(authority):c.sha(authority)}};body['cases']['L5']['same_journal_uuid']=uuid
   path=self.write(scope/'evidence/logs-03/acceptance.json',body)
   self.assertEqual(c.prior_evidence(scope)[0],uuid)
   for change in (lambda v:g_set(v,'passed',False),lambda v:v['cases'].pop('L4'),lambda v:g_set(v['cases']['L4'],'passed',False),lambda v:g_set(v,'input_sha256_after',{}),lambda v:g_set(v['cases']['L5'],'same_journal_uuid','b'*64)):
    bad=copy.deepcopy(body);change(bad);self.write(path,bad)
    with self.assertRaises(ValueError):c.prior_evidence(scope)
   self.write(path,body);self.write(cleanup,{'passed':True,'released':False,'snapshot_verified':True})
   with self.assertRaises(ValueError):c.prior_evidence(scope)
   self.write(cleanup,{'passed':True,'released':True,'snapshot_verified':True});self.write(authority,{'rebound':True})
   with self.assertRaises(ValueError):c.prior_evidence(scope)
 @patch.object(c,'historical_inputs',return_value={})
 def test_historical_manifest_cannot_request_credential_hash(self,_history):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);authority=self.write(scope/'runtime/runner.json',{});uuid='a'*64;secret=scope/'runtime/runner.key'
   self.write(scope/'evidence/sigterm-02/worker-runner-sigterm/acceptance.json',{'passed':True,'journal_uuid':uuid});self.write(scope/'evidence/logs-03/final-journal.json',{'identity':uuid,'version':5});self.write(scope/'evidence/logs-01-cleanup/report.json',{'passed':True,'released':True,'snapshot_verified':True})
   hashes={str(authority):c.sha(authority),str(secret):'b'*64};body={'passed':True,'cases':{k:{'passed':True} for k in c.CASE_KEYS},'input_sha256_before':hashes,'input_sha256_after':hashes};body['cases']['L5']['same_journal_uuid']=uuid;self.write(scope/'evidence/logs-03/acceptance.json',body)
   real=c.sha;seen=[]
   def read(path):seen.append(str(path));return real(path)
   with patch.object(c,'sha',side_effect=read),self.assertRaises(ValueError):c.prior_evidence(scope)
   self.assertNotIn(str(secret),seen)
 def test_existing_control_lock_is_exclusive_without_recreation(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);p=self.write(scope/'runtime/.sigterm-control.lock','')
   inode=p.stat().st_ino
   with s.control_lock(scope):
    with self.assertRaises(BlockingIOError):
     with s.control_lock(scope):pass
   self.assertEqual(p.stat().st_ino,inode)
 @patch.object(s,'second_unstarted',return_value={})
 def test_prior_private_dsn_is_bound_to_public_fixture_roles_and_run_ids(self,_second):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);rows={};paths=[]
   old_state={'status':'cancel_requested','version':2}
   for index,name in enumerate(('sigterm-private','sigterm-private-02','strict-logs-private','strict-logs-private-02','strict-logs-private-03')):
    schema=('appfault_lifecycle_' if name.startswith('sigterm') else 'appfault_strictlogs_')+str(index);role='appfault_worker_'+str(index)
    path=self.write(scope/'runtime'/name/'database.json',{'schema':schema,'worker_dsn':'postgres://'+role+':synthetic@127.0.0.1:32773/forge?sslmode=disable&search_path='+schema});paths.append(path)
    attempt='03' if name.endswith('-03') else '02' if name.endswith('-02') else '01';base=scope/'evidence'/('sigterm-'+attempt if name.startswith('sigterm') else 'logs-'+attempt)
    if name.startswith('sigterm'):base=base/'worker-runner-sigterm'
    self.write(base/'worker-role-preflight.json',{'schema':schema,'current_user':role,'session_user':role,'production_CheckWorkerRole':'passed'})
    row=dict(self.idle(),schema=schema,runs=[{'id':'run_'+str(index),'tenant':'tenant_'+str(index),'state':'completed'}])
    if index==0:
     row['runs'][0].update(state='cancel_requested',version=2,epoch=0,owner='',workspace='',snapshot=old_state)
     self.write(scope/'evidence/sigterm-01/abort-before-runner/report.json',{'passed':True,'terminal':False,'status':'cancel_requested','schema':schema,'run_id':'run_0','after':{'state':old_state}})
    elif index==1:self.write(base/'fixture.json',{'run_id':'run_1','tenant':'tenant_1'})
    else:self.write(base/'private-schema.json',{'schema':schema});self.write(base/'L1/fixture.json',{'run_id':'run_'+str(index),'tenant':'tenant_'+str(index)})
    rows[schema]=row
   with patch.object(s,'sql_json',side_effect=lambda query,env,schema:rows[schema]):self.assertEqual(len(s.previous_databases(scope,{})),5)
   original=json.loads(paths[0].read_text());changed=dict(original,schema='appfault_lifecycle_foreign',worker_dsn=original['worker_dsn'].replace(original['schema'],'appfault_lifecycle_foreign'));self.write(paths[0],changed)
   with patch.object(s,'sql_json') as query,self.assertRaises(ValueError):s.previous_databases(scope,{})
   query.assert_not_called()
   self.write(paths[0],original);rows['appfault_lifecycle_1']['runs'][0]['id']='substituted_run'
   with patch.object(s,'sql_json',side_effect=lambda query,env,schema:rows[schema]),self.assertRaises(ValueError):s.previous_databases(scope,{})
 def test_nonterminal_unknown_capacity_and_original_cancel_intent(self):
  good=self.idle();s.idle_pg(good)
  for field in ('unsettled_effects','active','runner_reserved','allocations','requests','unsettled_reservations','cleanup'):
   with self.subTest(field=field),self.assertRaises(ValueError):s.idle_pg(dict(good,**{field:1}))
  for status in ('queued','running','needs_reconciliation','waiting_approval','cancel_requested'):
   with self.assertRaises(ValueError):s.idle_pg(dict(good,runs=[{'state':status}]))
  state={'run_id':'old','status':'cancel_requested','limits':{'deadline':'2026-01-01T00:00:00Z'}};old={'schema':good['schema'],'run_id':'old','after':{'state':state}}
  row=dict(good,runs=[{'id':'old','state':'cancel_requested','version':2,'epoch':0,'owner':'','workspace':'','snapshot':state}]);s.idle_pg(row,old)
  for key,value in (('epoch',1),('state','cancelled'),('owner','new-worker'),('workspace','new-workspace'),('snapshot',{})):
   bad=copy.deepcopy(row);bad['runs'][0][key]=value
   with self.assertRaises(ValueError):s.idle_pg(bad,old)
 def test_fresh_quota_never_renews_window_or_hides_previous_spend(self):
  q={'credential_group':'deepseek-flash-evalv2','max_concurrent':1,'max_tokens':10000000,'max_microusd':2000000,'window_generation':1,'window_ends_at':'2099-01-01T00:00:00Z','active_requests':0,'reserved_tokens':0,'committed_tokens':0,'reserved_microusd':0,'committed_microusd':0}
  report={'schema':'eval_ds_'+'a'*32,'schema_oid':17,'initial_quota':q,'launch_not_after':'2099-01-01T00:00:00Z','token_expires_at':'2099-01-01T00:00:00Z'};good=dict(self.idle(),quotas=[q])
  with patch.object(s,'sql_json',return_value=good):s.fresh_evaluation(Path('/not-read'),report,{})
  for mutate in (lambda x:g_set(x,'projects',1),lambda x:g_set(x,'schema_oid',18),lambda x:g_set(x,'now','2100-01-01T00:00:00Z'),lambda x:g_set(x['quotas'][0],'committed_microusd',1),lambda x:g_set(x['quotas'][0],'window_generation',2),lambda x:g_set(x['quotas'][0],'window_ends_at','2099-02-01T00:00:00Z')):
   bad=copy.deepcopy(good);mutate(bad)
   with patch.object(s,'sql_json',return_value=bad),self.assertRaises(ValueError):s.fresh_evaluation(Path('/not-read'),report,{})
 def test_scope_query_is_exact_readonly_and_wrong_role_rejected(self):
  schema='eval_ds_'+'a'*32;env={'PGUSER':'operator'}
  value={'database':'forge','current_user':'operator','superuser':True}
  with patch.object(s.subprocess,'run',return_value=subprocess.CompletedProcess([],0,json.dumps(value),'')) as call:
   s.sql_json(s.QUERIES,env,schema)
   query=call.call_args.kwargs['input'];self.assertTrue(query.startswith('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;'));self.assertTrue(query.endswith('ROLLBACK;'));self.assertNotIn('public.',query);self.assertIn('schema='+schema,call.call_args.args[0])
  for bad in ({'database':'other','current_user':'operator','superuser':True},{'database':'forge','current_user':'api','superuser':False}):
   with patch.object(s.subprocess,'run',return_value=subprocess.CompletedProcess([],0,json.dumps(bad),'')),self.assertRaises(ValueError):s.sql_json(s.QUERIES,env,schema)
 def test_own_container_inventory_does_not_remove_or_inspect_others(self):
  calls=[]
  with patch.object(s,'docker_read',side_effect=lambda runner,*args:calls.append(args) or ''):
   self.assertEqual(s.containers_absent({},['op_one','op_two'])['remaining'],0)
  self.assertEqual(calls,[('ps','--all','--no-trunc','--filter','label=forge.operation_id=op_one','--format','{{.ID}}'),('ps','--all','--no-trunc','--filter','label=forge.operation_id=op_two','--format','{{.ID}}')])
  with patch.object(s,'docker_read',return_value='retained-id'),self.assertRaises(ValueError):s.containers_absent({},['op_one'])
 def test_unit_command_clears_ambient_env_and_only_creates_own_service(self):
  root=Path('/dedicated E');argv=i.host_argv(root,'forge-eval-deepseek-0123456789abcdef.service',Path('/run/user/1000/unique'))
  self.assertEqual(argv[0],'/usr/bin/systemd-run');self.assertIn('--property=Delegate=yes',argv);self.assertIn('--property=KillMode=control-group',argv)
  self.assertEqual(argv[argv.index('/usr/bin/env')+1],'-i');self.assertIn('HOME=/dedicated E/private',argv)
  self.assertNotIn('DEEPSEEK_API_KEY',' '.join(argv));self.assertNotIn('FORGE_DATABASE_URL',' '.join(argv));self.assertNotIn('stop',argv);self.assertNotIn('mount',argv)
 def test_entitlement_is_exclusive_and_wait_readiness_stops_at_early_exit(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'entitlement.json';c.save(p,{'fixed':'original'})
   with self.assertRaises(FileExistsError):c.save(p,{'fixed':'replacement'})
   self.assertEqual(json.loads(p.read_text()),{'fixed':'original'})
  child=types.SimpleNamespace(healthy=lambda:(_ for _ in ()).throw(ValueError('worker exited')))
  with patch.object(i.time,'sleep') as sleep,self.assertRaisesRegex(ValueError,'worker exited'):i.wait_ready(lambda:True,[child])
  sleep.assert_not_called()
 def test_identity_uses_pid_ticks_exe_and_full_args(self):
  identity={'pid':234,'proc_start_ticks':'100','exe_path':'/own/forge','exe_sha256':'a'*64,'args':['/own/forge','-config','/own/config']}
  self.assertTrue(s.same_process(identity,dict(identity)))
  for key,value in (('pid',235),('proc_start_ticks','101'),('exe_path','/other'),('exe_sha256','b'*64),('args',['/own/forge','-config','/other'])):
   self.assertFalse(s.same_process(identity,dict(identity,**{key:value})))
 def test_api_after_exit_requires_connection_refused_not_http_503_or_timeout(self):
  for code in (0,errno.ETIMEDOUT,errno.EACCES):
   sock=types.SimpleNamespace(settimeout=lambda _:None,connect_ex=lambda _:code,close=lambda:None)
   with patch.object(i.socket,'socket',return_value=sock),self.assertRaises(ValueError):i.api_absent('http://127.0.0.1:18098')
  sock=types.SimpleNamespace(settimeout=lambda _:None,connect_ex=lambda _:errno.ECONNREFUSED,close=lambda:None)
  with patch.object(i.socket,'socket',return_value=sock):self.assertEqual(i.api_absent('http://127.0.0.1:18098')['new_tcp_connection'],'refused')
 def test_closure_requires_four_bound_terminal_runs_and_retains_unknown_cost(self):
  row=dict(self.idle(),runs=[{'id':'run_'+str(n),'tenant':'tenant','state':'completed'} for n in range(4)])
  report={'complete':True,'tasks':[{'run_id':r['id'],'budget_violation':False} for r in row['runs']],'conservative_accounted_exposure_microusd':2000000,'unknown_or_unsettled_reserved_microusd':0}
  self.assertEqual(len(i.closure(row,report,{'tenant_id':'tenant'})),4)
  for mutate in (lambda r:g_set(r,'unknown_or_unsettled_reserved_microusd',1),lambda r:g_set(r,'conservative_accounted_exposure_microusd',2000001),lambda r:g_set(r,'complete',False),lambda r:r['tasks'].pop()):
   bad=copy.deepcopy(report);mutate(bad)
   with self.assertRaises(ValueError):i.closure(row,bad,{'tenant_id':'tenant'})
 def test_mapped_orchestration_idle_order_and_failure_does_not_cleanup_unknown(self):
  for complete in (True,False):
   with self.subTest(complete=complete),tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp)/'evaluation/eval';out=root/'evidence/launch';out.mkdir(parents=True);scope=root.parent.parent;repo=scope.parent
    key=root/'synthetic-provider.env';m={'journal_uuid':'a'*64,'revision':'b'*40,'provider_key_path':str(key)};p={'schema':'eval_ds_'+'c'*32,'tenant_id':'tenant','api_url':'http://127.0.0.1:18098'};platform={'worker_id':'eval-one'};runner={'server':{'unix_socket':'/not-used.sock'}}
    report={'complete':complete,'tasks':[{'run_id':'run_'+str(n),'budget_violation':False} for n in range(4)],'conservative_accounted_exposure_microusd':4,'unknown_or_unsettled_reserved_microusd':0 if complete else 1};row=dict(self.idle(),runs=[{'id':'run_'+str(n),'tenant':'tenant','state':'completed'} for n in range(4)])
    saved={};events=[];environments={};unit='forge-eval-deepseek-0123456789abcdef.service'
    def read(path):
     path=Path(path)
     if path.name=='deepseek-flash-2usd.entitlement.json':return {'evaluation_root':str(root),'launch_sha256':'a'*64,'total_budget_microusd':2000000,'never_automatically_renew':True}
     if path.name=='host-intent.json':return {'unit':unit}
     if path.name=='report.json':return report
     return saved[str(path)]
    class Owned:
     def __init__(self,name,args,env,out):self.name=name;environments[name]=dict(env);events.append('start:'+name)
     def healthy(self):pass
     def stop(self):events.append('stop:'+self.name);return {'name':self.name,'exited':True,'exit_code':0}
    class Evaluator:
     def __init__(self,*args,**kwargs):events.append('collector');environments['collector']=kwargs['env']
     def wait(self,timeout):return 0
    def execute(args,**kw):self.assertIn('workspace-gc',args);events.append('cleanup');return subprocess.CompletedProcess(args,0)
    real_read_text=Path.read_text
    def kernel_read(path,*args,**kw):return '0::/user.slice/'+unit+'\n' if str(path)=='/proc/self/cgroup' else real_read_text(path,*args,**kw)
    ticks=iter(n*.5 for n in range(100))
    with contextlib.ExitStack() as stack:
     for target,name,value in ((i,'mapped_identity',lambda:{}),(i,'load',lambda _: (root,scope,repo,m,p,{},platform,runner)),(i,'preflight',lambda *a,**k:{}),(i,'envs',lambda *a:({'FORGE_DATABASE_URL':'synthetic-api'},{'FORGE_DATABASE_URL':'synthetic-worker'},{'FORGE_EVAL_AUDIT_DSN':'synthetic-audit'})),(i,'OwnedProcess',Owned),(i,'wait_ready',lambda *a,**k:None),(i,'api_absent',lambda _: {'new_tcp_connection':'refused'}),(i,'api_authentication',lambda *a:{'mutations':0}),(i,'static_helper',lambda *a:{'journal':{'operation_ids':[]}}),(s,'control_lock',lambda _:contextlib.nullcontext()),(s,'admin_environment',lambda _: ({'PGUSER':'operator'},'postgres://operator:synthetic@127.0.0.1:32773/forge?sslmode=disable')),(s,'fresh_evaluation',lambda *a:{}),(s,'sql_json',lambda *a,**k:copy.deepcopy(row)),(s,'containers_absent',lambda *a:{}),(c,'read_json',read),(c,'sha',lambda _: 'a'*64),(c,'save',lambda path,value:saved.update({str(path):copy.deepcopy(value)})),(c,'verify_sealed_inputs',lambda *a:None),(c,'literal_env',lambda *a,**k:{'DEEPSEEK_API_KEY':'synthetic-worker-only'}),(c.evaluate,'CLI',lambda *a:types.SimpleNamespace(call=lambda *a:[]))):stack.enter_context(patch.object(target,name,value))
     stack.enter_context(patch.object(Path,'read_text',kernel_read));stack.enter_context(patch.object(i.time,'sleep',lambda _:None));stack.enter_context(patch.object(i.time,'monotonic',lambda:next(ticks)));stack.enter_context(patch.object(i.subprocess,'Popen',Evaluator));stack.enter_context(patch.object(i.subprocess,'run',execute))
     if complete:self.assertEqual(i.mapped_child(root),0)
     else:
      with self.assertRaises(ValueError):i.mapped_child(root)
    self.assertEqual([x for x in events if x.startswith('stop:')],['stop:api','stop:worker','stop:runner'])
    self.assertEqual(events.count('cleanup'),4 if complete else 0);self.assertEqual(events.count('collector'),1)
    self.assertIn('DEEPSEEK_API_KEY',environments['worker'])
    for name in ('runner','api','collector'):self.assertNotIn('DEEPSEEK_API_KEY',environments[name])
    self.assertEqual(saved[str(out/'result.json')]['complete'],complete)

 def test_provision_binds_exact_candidate_bytes_before_seal(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);hashes={}
   for name in ('candidate','platform','runner','registration'):
    p=self.write(root/'candidate'/(name+'.json'),{'synthetic':name});hashes[name+'.json']=c.sha(p)
   reg={'config_id':'eval-config','model_spec':{'credential_group':'deepseek-flash-evalv2','price_version':'fixed'}};platform={'runner_id':'fixture','runner':{'unix_socket':'/retained.sock'},'listen':'127.0.0.1:18098'}
   report={'phase':'ready','purpose':'deepseek-eval-v2-provision','schema_version':1,'candidate_dir':str(root/'candidate'),'candidate_hashes':hashes,'provider':'deepseek','model':'deepseek-v4-flash','config_id':'eval-config','credential_group':'deepseek-flash-evalv2','price_version':'fixed','exact_pricing':False,'batch_id':'deepseek-flash-2usd-'+hashes['registration.json'],'budget':{'total_microusd':2000000,'task_budgets_microusd':{k:500000 for k in c.common.CASES}},'runner_slots':4,'worker_slots':1,'runner_id':'fixture','runner_endpoint':'unix:///retained.sock','api_url':'http://127.0.0.1:18098'}
   c.provision_binding(root,report,reg,platform)
   for key,value in (('batch_id','new-batch'),('worker_slots',2),('exact_pricing',0),('candidate_hashes',{})):
    with self.assertRaises(ValueError):c.provision_binding(root,dict(report,**{key:value}),reg,platform)
   self.write(root/'candidate/registration.json',{'changed':True})
   with self.assertRaises(ValueError):c.provision_binding(root,report,reg,platform)
 @patch.object(c,'failed_second_inputs',return_value={})
 def test_material_history_closed_manifest_and_no_private_hash(self,_second):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);old=scope/'evidence/logs-01';clean=scope/'evidence/logs-01-cleanup'
   oldfile=self.write(old/'original.json',{'failed':True});oldmanifest=self.write(old/'manifest.json',{'original.json':c.sha(oldfile)})
   top=['acceptance-input.json','intent.json','release-intent.json','release.json','released-journal.json','report.json','snapshot.bytes','snapshot.json','stop.bytes','stop.json']
   for name in top:self.write(clean/name,{})
   run='sl-L3-bytes-46djebzw3ifph2ui7nesemhkz6'
   for name,key,kind in (('stop.json','ref','workspace_stop'),('snapshot.json','artifact','workspace_snapshot')):
    raw=b'stop' if key=='ref' else b'snapshot';digest=c.hashlib.sha256(raw).hexdigest();object_key='strict-log-fixture/'+run+'/'+digest
    p=scope/'runtime/artifacts'/object_key;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
    self.write(clean/name,{key:{'tenant_id':'strict-log-fixture','run_id':run,'kind':kind,'object_key':object_key,'sha256':digest,'size':len(raw)}})
   self.write(clean/'invocation-123/report.json',{'passed':False})
   members={str(p.relative_to(clean)):c.sha(p) for p in clean.rglob('*') if p.is_file()};manifest=self.write(clean/'manifest.json',members)
   with patch.object(c,'HISTORICAL_MANIFEST_SHA',c.sha(oldmanifest)):
    proof=c.historical_inputs(scope);self.assertIn(str(clean/'invocation-123/report.json'),proof)
    for bad in ('../runtime/runner.key','private/database.json','runner.key','invocation-123/worker.env','unreviewed.json'):
     self.write(manifest,dict(members,**{bad:'f'*64}));real=c.sha;seen=[]
     def tracked(path):seen.append(str(path));return real(path)
     with patch.object(c,'sha',side_effect=tracked),self.assertRaises(ValueError):c.historical_inputs(scope)
     self.assertNotIn(str(clean/bad),seen)
    self.write(manifest,members);self.write(oldfile,{'changed':True})
    with self.assertRaises(ValueError):c.historical_inputs(scope)
 def test_final_journal_large_json_has_specific_bound_not_credential_expansion(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=self.write(Path(tmp)/'final-journal.json',{'tables':'x'*(2<<20)})
   self.assertEqual(len(c.read_json(p,16<<20)['tables']),2<<20)
   with self.assertRaises(ValueError):c.read_json(p)
   with self.assertRaises(ValueError):c.private(p,16384)
 def test_unit_empty_checks_exact_cgroup_and_refuses_retained_children(self):
  unit='forge-eval-deepseek-0123456789abcdef.service';state={'LoadState':'loaded','ActiveState':'inactive','MainPID':'0','ControlGroup':'/user.slice/user-1000.slice/'+unit}
  with patch.object(Path,'read_text',return_value=''):self.assertTrue(s.unit_empty(unit,state)['empty_or_absent'])
  with patch.object(Path,'read_text',return_value='123\n'),self.assertRaises(ValueError):s.unit_empty(unit,state)
  with self.assertRaises(ValueError):s.unit_empty(unit,dict(state,ControlGroup='/user.slice/foreign.service'))
  with self.assertRaises(ValueError):s.unit_empty(unit,dict(state,MainPID='123'))
 def test_authentication_uses_existing_readonly_run_route_and_exact_response_pair(self):
  import http.client
  statuses=[403,404];codes=['forbidden','not_found'];requests=[]
  class Connection:
   def __init__(self,*a,**k):self.index=len(requests)
   def request(self,method,path,headers):requests.append((method,path,dict(headers)))
   def getresponse(self):return types.SimpleNamespace(status=statuses[self.index],read=lambda _:json.dumps({'code':codes[self.index],'request_id':'req'}).encode())
   def close(self):pass
  values={'FORGE_API_URL':'http://127.0.0.1:18098','FORGE_TENANT':'tenant','FORGE_TOKEN':'synthetic'}
  with patch.object(c,'literal_env',return_value=values),patch.object(http.client,'HTTPConnection',Connection):
   proof=i.api_authentication(Path('/not-read'),{'api_url':values['FORGE_API_URL'],'tenant_id':'tenant'})
   self.assertEqual(proof['mutations'],0);self.assertEqual([r[:2] for r in requests],[('GET','/v1/runs/eval_preflight_absent')]*2)
   self.assertNotIn('Authorization',requests[0][2]);self.assertIn('Authorization',requests[1][2]);self.assertNotIn('synthetic',json.dumps(proof))
   requests.clear();statuses[1]=403;codes[1]='forbidden'
   with self.assertRaises(ValueError):i.api_authentication(Path('/not-read'),{'api_url':values['FORGE_API_URL'],'tenant_id':'tenant'})

 def test_second_unstarted_keeps_full_snapshot_and_history_without_cancel(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);base=scope/'evidence/logs-02-before-runner-observation'
   snapshot={'version':1,'limits':{'deadline':'2026-09-11T23:08:28.321842-07:00'},'lease':{'owner':'','epoch':0},'status':'queued'}
   row=dict(self.idle(),schema=c.SECOND_SCHEMA,schema_oid=852430,projects=1,runs=[{'id':c.SECOND_RUN,'tenant':c.SECOND_TENANT,'state':'queued','version':1,'epoch':0,'owner':'','workspace':'','snapshot':snapshot}])
   self.write(base/'postgres.json',dict(row,schema_oid='852430'))
   history={'runs':[{'id':c.SECOND_RUN,'snapshot':snapshot,'pending_commands':[]}],'events':[{'seq':1,'type':'run.created'}],'snapshots':[{'version':1,'body':snapshot}],'steps':None,'quota_reservations':0,'runner_allocations':0}
   self.write(base/'history.json',history)
   with patch.object(s,'sql_json',return_value=history) as query:
    proof=s.second_unstarted(scope,row,{})
    self.assertEqual(proof['status'],'queued');self.assertIs(proof['worker_routed'],False);self.assertIn(':"schema".runs',query.call_args.args[0]);self.assertNotIn('UPDATE',query.call_args.args[0])
   for mutate in (lambda x:g_set(x,'schema_oid',852431),lambda x:g_set(x,'schema_oid','852430'),lambda x:g_set(x,'effects',1),lambda x:g_set(x['runs'][0],'state','cancel_requested'),lambda x:g_set(x['runs'][0]['snapshot'],'version',True),lambda x:g_set(x['runs'][0],'epoch',1),lambda x:g_set(x['runs'][0],'owner','new-worker'),lambda x:g_set(x['runs'][0],'workspace','new-workspace'),lambda x:g_set(x['runs'][0]['snapshot']['limits'],'deadline','2099-01-01T00:00:00Z')):
    bad=copy.deepcopy(row);mutate(bad)
    with patch.object(s,'sql_json') as query,self.assertRaises(ValueError):s.second_unstarted(scope,bad,{})
    query.assert_not_called()
   for mutate in (lambda x:g_set(x,'steps',[{'seq':1}]),lambda x:g_set(x,'quota_reservations',1),lambda x:g_set(x,'runner_allocations',1),lambda x:g_set(x['runs'][0],'pending_commands',['stop']),lambda x:x['events'].append({'seq':2}),lambda x:g_set(x['snapshots'][0],'version',2)):
    bad=copy.deepcopy(history);mutate(bad)
    with patch.object(s,'sql_json',return_value=bad),self.assertRaises(ValueError):s.second_unstarted(scope,row,{})
 def test_live_schema_oid_is_explicit_numeric_sql_type(self):
  self.assertIn('(SELECT oid::bigint FROM pg_catalog.pg_namespace',s.QUERIES)
  q={'credential_group':'deepseek-flash-evalv2','max_concurrent':1,'max_microusd':2000000,'window_generation':1,'window_ends_at':'2099-01-01T00:00:00Z'}
  row=dict(self.idle(),schema_oid='17',quotas=[q]);report={'schema':row['schema'],'schema_oid':17}
  with patch.object(s,'sql_json',return_value=row),self.assertRaises(ValueError):s.fresh_evaluation(Path('/not-read'),report,{})
 def test_only_final_third_logs_acceptance_can_open_gate(self):
  with tempfile.TemporaryDirectory() as tmp:
   scope=Path(tmp);sig=self.write(scope/'evidence/sigterm-02/worker-runner-sigterm/acceptance.json',{'passed':True,'journal_uuid':'a'*64})
   self.write(scope/'evidence/logs-02/acceptance.json',{'passed':True})
   with self.assertRaises(FileNotFoundError):c.prior_evidence(scope)
   # A previous PASS cannot substitute for missing final logs03 evidence.
 def test_exact_second_manifest_members_cannot_hash_credentials(self):
  scope=Path('/dedicated/scope');read_paths=[];hashed=[]
  original_sha=c.sha
  def hash_path(path):
   hashed.append(str(path))
   if path==scope/'evidence/logs-02/manifest.json':return c.FAILED_SECOND_MANIFEST_SHA
   return 'a'*64
  members={('entry-'+str(n)+'.json'):'a'*64 for n in range(14)};members['../runtime/runner.key']='a'*64
  with patch.object(c,'sha',side_effect=hash_path),patch.object(c,'read_json',return_value=members),self.assertRaises(ValueError):c.failed_second_inputs(scope)
  self.assertFalse(any(x.endswith('runner.key') for x in hashed))
  exact=scope/'evidence/sigterm-02/worker-runner-sigterm/acceptance-input.json'
  self.assertTrue(c.historical_path(scope,exact,{str(exact):'a'*64}))
  self.assertFalse(c.historical_path(scope,exact.with_name('runner.key'),{}))

def g_set(obj,key,value):obj[key]=value
if __name__=='__main__':unittest.main()

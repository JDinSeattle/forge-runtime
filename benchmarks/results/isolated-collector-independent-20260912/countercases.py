#!/usr/bin/python3
"""Independent offline replay/mutations; no real CLI, PG, provider or credential."""
import copy, hashlib, importlib.util, json, os, subprocess, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
REPO=Path('/tmp/forge-collector-review-41d6f51')
OUT=Path('/tmp/forge-collector-delta-independent')
spec=importlib.util.spec_from_file_location('independent_fixtures',REPO/'scripts/evaluation/test_deployment.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
e=m.evaluate;c=e.common
OBS=[]
class Watch:
 def terminate(self): pass
 def wait(self,timeout): return 0
 def kill(self): pass
class Review(unittest.TestCase):
 def test_original_counterexamples_replayed_against_frozen_tip(self):
  old=Path('/tmp/forge-isolated-collector-counterexamples.py').read_text()
  self.assertEqual(hashlib.sha256(old.encode()).hexdigest(),'7654b7dae3425fb76cd033c36d7395920881bef9b6e8bf7e093265b6af631d0d')
  # Reuse exact old statements and inputs, substituting only imported frozen modules.
  first=old[old.index('f=m.DeploymentTests();f.setUp()'):old.index('# Counterexample 2:')]
  with self.assertRaisesRegex(ValueError,'saved batch envelope') as caught:
   exec(compile(first,'old-counterexample-1','exec'),dict(m=m,e=e,c=c,patch=patch,json=json,sys=sys,results=[]))
  OBS.append({'case':'original_batch_envelope_substitution','rejected':True,'error':str(caught.exception)})
  second=old[old.index('class Watch:'):old.index('assert results[0]')]
  with self.assertRaisesRegex(ValueError,'exact sealed source/profile') as caught:
   exec(compile(second,'old-counterexample-2','exec'),dict(m=m,e=e,c=c,patch=patch,json=json,sys=sys,tempfile=tempfile,Path=Path,hashlib=hashlib,results=[]))
  OBS.append({'case':'original_wrong_project_fixture','rejected':True,'error':str(caught.exception),'limit':'Legacy fixture omits newly required sealed source and CLI receipt; next test supplies both to exercise project binding.'})
 def fixture(self,root):
  case=c.CASES[0];directory=root/case;directory.mkdir()
  r=m.fixtures.registration();source={'path':str(c.CORPUS/case/'source'),'hash':'a'*64,'profile_id':c.SOURCE_PREFIX+case};body=e.submit_body(case,source,r)
  tenant='eval_fixture';project='proj_expected';runid='run_original';rev=3
  intent={'project_id':project,'budget_microusd':r['task_budgets_microusd'][case],'idempotency_key':'frozen-key','submitted_at':'2026-09-12T00:00:00Z'}
  submitted={'run_id':runid,'reused':False}
  for name,value in {'project.json':{'id':project,'tenant_id':tenant,'source_id':c.SOURCE_PREFIX+case,'profile_id':source['profile_id']},'submission-intent.json':intent,'submission.json':submitted}.items(): (directory/name).write_text(json.dumps(value))
  scope='http://127.0.0.1:19097\n'+tenant+'\n/v1/projects/'+project+'/runs\n'
  (root/'cli-receipts').mkdir();rp=root/'cli-receipts'/('key-'+hashlib.sha256((scope+intent['idempotency_key']).encode()).hexdigest()+'.json')
  receipt={'key':intent['idempotency_key'],'body_hash':hashlib.sha256(scope.encode()+e.cli_body_bytes(body)).hexdigest(),'created_at':intent['submitted_at'],'response':submitted};rp.write_text(json.dumps(receipt))
  config=dict(body['budget'],provider=r['provider'],model=r['model_id'])
  expected={'id':runid,'tenant_id':tenant,'project_id':project,'task':body['task'],'base_commit':source['hash']}
  state={'status':'completed','verification_status':'verified','verification_report_ref':'verification_evidence','workspace_revision':rev,'verification_revision':rev,'run_id':runid,'tenant_id':tenant}
  run=dict(expected,config=config,state=state)
  admission={'schema_version':1,'project_id':project,'task':body['task'],'base_commit':source['hash'],'source_id':c.SOURCE_PREFIX+case,'profile_id':source['profile_id']}
  quote=dict(r['model_spec'],schema_version=1,provider=r['provider'],model=r['model_id'])
  attempt={'attempt_id':'attempt_one','provider':r['provider'],'model_id':r['model_id'],'pricing':quote,'step_seq':1,'attempt':1,'status':'completed','error_code':None,'request_id':'synthetic-native-request','usage':{'input_tokens':1,'output_tokens':1},'response_persisted_at':'2026-09-12T00:00:01Z'}
  reservation={'id':'attempt_one','credential_group':r['model_spec']['credential_group'],'status':'settled','actual_microusd':1,'microusd':2,'dispatched_at':'2026-09-12T00:00:00Z'}
  ledger={'run':dict(expected,config_snapshot=config,state='completed',input_snapshot=json.dumps(admission),created_at='2026-09-12T00:00:00Z'),'finished_at':'2026-09-12T00:00:02Z','attempts':[attempt],'reservations':[reservation]}
  artifacts=[]
  for aid,kind,data in [('baseline_evidence','baseline',{'target_failed':True}),('verification_evidence','verification_report',{'evidence':{'trusted':True,'baseline_target_failed':True,'target_passed':True,'regression_passed':True,'workspace_revision':rev}})]:
   raw=json.dumps(data).encode();(directory/aid).write_bytes(raw);artifacts.append({'id':aid,'kind':kind,'sha256':hashlib.sha256(raw).hexdigest()})
  class CLI:
   command=['synthetic-cli-never-executed'];env={'FORGE_API_URL':'http://127.0.0.1:19097'};binding={'synthetic':True}
   def check_binary(self): pass
   def call(self,*args):
    if args[:2]==('run','get'): return run
    if args[:2]==('run','artifacts'): return artifacts
    raise AssertionError(args)
  cli=CLI();cli.tenant=tenant
  return case,directory,r,source,cli,run,ledger,rp,receipt
 def test_complete_case_with_real_receipt_shape_rejects_wrong_project_and_receipt(self):
  with tempfile.TemporaryDirectory() as temp:
   case,d,r,source,cli,run,ledger,rp,receipt=self.fixture(Path(temp))
   with patch.object(e.subprocess,'Popen',return_value=Watch()),patch.object(e,'audit_run',return_value=ledger):
    self.assertTrue(e.collect_task(cli,case,d,r,0,source)['verified_repair'])
    for which in ('get','sql','both'):
     original=run['project_id'];run['project_id']='proj_other' if which in ('get','both') else original
     ledger['run']['project_id']='proj_other' if which in ('sql','both') else original
     with self.subTest(which=which),self.assertRaisesRegex(ValueError,'GET/SQL run differs'):
      e.collect_task(cli,case,d,r,0,source)
     run['project_id']=ledger['run']['project_id']=original
    for changes in ({'body_hash':'f'*64},{'response':{'run_id':'other_run'}},{'key':'other-key'}):
     rp.write_text(json.dumps(dict(receipt,**changes)))
     with self.subTest(changes=changes),self.assertRaisesRegex(ValueError,'CLI receipt'):
      e.collect_task(cli,case,d,r,0,source)
   OBS.append({'case':'complete_case_to_project_and_cli_receipt','positive_verified':True,'wrong_get_project_rejected':True,'wrong_sql_project_rejected':True,'both_wrong_projects_rejected':True,'changed_bodyhash_response_key_rejected':True,'scope':'Synthetic API/SQL/artifacts, no spawned process or socket.'})
 def test_saved_duplicate_json_and_duplicate_run_fail(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);p=root/'record.json';p.write_text('{"total_budget_microusd":2000000,"total_budget_microusd":999999999}')
   with self.assertRaisesRegex(ValueError,'duplicate'):e.saved_json(p)
   for case in c.CASES[:2]:
    (root/case).mkdir();(root/case/'submission.json').write_text('{"run_id":"same_run"}')
   with self.assertRaisesRegex(ValueError,'duplicate run'):e.unique_saved_runs(root)
 def test_missing_confirmed_submission_collect_never_calls_cli_submit(self):
  f=m.DeploymentTests();f.setUp()
  try:
   b=f.binding();f.output.mkdir();batch=e.batch_envelope({'sha256':'c'*64},f.seal,f.registration,b,'2026-09-12T00:00:00Z');(f.output/'batch.json').write_text(json.dumps(batch))
   args=['evaluate.py','--bundle',str(f.bundle),'--output',str(f.output),'--client-env',str(f.root/'client.env'),'--deployment',str(f.descriptor),'--collect']
   with patch.object(e,'load_bundle',return_value=({'sha256':'c'*64},f.seal,f.registration,{})),patch.object(e,'audit_environment'),patch.object(e,'audit_preflight'),patch.object(e,'CLI') as cli,patch.object(sys,'argv',args):
    e.main();cli.return_value.call.assert_not_called()
   report=json.loads((f.output/'report.json').read_text());self.assertFalse(report['complete']);self.assertIn('missing confirmed run ID',report['stopped_reason'])
  finally:f.doCleanups()
 def test_provisioned_plain_env_passes_and_shellquoted_original_fails(self):
  f=m.DeploymentTests();f.setUp()
  try:
   b=f.binding();p=f.root/'client.env';values={'FORGE_API_URL':f.value['api_url'],'FORGE_TENANT':f.value['tenant'],'FORGE_TOKEN':'frg_SYNTHETIC_FOR_OFFLINE_ONLY'}
   for quoted in (False,True):
    p.write_text(''.join(k+'='+ ("'"+v+"'" if quoted else v)+'\n' for k,v in values.items()));p.chmod(0o600)
    with patch.object(e.subprocess,'run') as invoked:
     if quoted:
      with self.assertRaisesRegex(ValueError,'incomplete client environment'):e.CLI(p,f.output,b)
     else:self.assertEqual(e.CLI(p,f.output,b).tenant,f.value['tenant'])
     invoked.assert_not_called()
  finally:f.doCleanups()
 def test_actual_go_submit_bridge_all_cases_and_escape_bytes(self):
  patterns=[''.join(chr(i) for i in range(32)), '<>& \\u003c \\u2028 \u2028\u2029', '中文🙂😃𝄞é e\u0301', '\\" / \t\n\r\\\\', 'x'*63990]
  samples=[]
  for case in c.CASES:
   src={'path':str(c.CORPUS/case/'source'),'hash':'a'*64,'profile_id':c.SOURCE_PREFIX+case};samples.append(e.submit_body(case,src,m.fixtures.registration()))
  for pattern in patterns:
   body=copy.deepcopy(samples[0]);body['task']=pattern;samples.append(body)
  for index,body in enumerate(samples):
   raw=c.local_output(['go','run',str(REPO/'scripts/evaluation/hash-sources.go'),'--submit-json'],input=json.dumps(body),cwd=REPO,env=c.go_environment())
   with self.subTest(index=index):self.assertEqual(e.cli_body_bytes(body)+b'\n',raw.encode())
  OBS.append({'case':'actual_go_submit_type_serialization','samples':len(samples),'all_equal':True,'scope':'Local Go bridge only; no CLI or HTTP invocation.'})
if __name__=='__main__':
 suite=unittest.defaultTestLoader.loadTestsFromTestCase(Review);result=unittest.TextTestRunner(verbosity=2).run(suite)
 (OUT/'countercases-result.json').write_text(json.dumps({'commit':'41d6f5184157ee8048647336536205cfefe4cc5f','tests':result.testsRun,'passed':result.wasSuccessful(),'observations':OBS},indent=2)+'\n')
 sys.exit(not result.wasSuccessful())

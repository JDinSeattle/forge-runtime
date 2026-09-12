#!/usr/bin/python3
"""One reviewed DeepSeek $2 batch, in a dedicated transient user service.

seal writes nonsecret identity only; preflight is read-only except its explicit
new evidence file. launch starts only this deployment. No mount, pool rebinding,
provisioning, provider retry, global artifact GC, or existing-service stop exists.
"""
from __future__ import annotations
import argparse, datetime, errno, importlib.util, json, os, re, secrets, signal, socket, struct, subprocess, sys, time
from pathlib import Path
sys.dont_write_bytecode=True
spec=importlib.util.spec_from_file_location('isolated_state',Path(__file__).with_name('isolated_state.py'));s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s);c=s.c

def load(root):
 root,scope,repo=c.authority(root)
 manifest=c.read_json(root/'launch.json');c.verify_sealed_inputs(root,manifest)
 provision=c.read_json(root/'private/provision.json')
 if provision.get('phase')!='ready' or provision.get('purpose')!='deepseek-eval-v2-provision' or provision.get('schema_version')!=1 or provision.get('candidate_dir')!=str(root/'candidate') or provision['launch_not_after']!=manifest['launch_not_after']:raise ValueError('ready provisioned identity required')
 corpus,seal,registration,platform=c.evaluate.load_bundle(root/'candidate');runner=c.read_json(root/'candidate/runner.json');c.policy(platform,runner,registration,scope);c.provision_binding(root,provision,registration,platform)
 binding=c.deployment.load(root/'deployment.json',root/'candidate',root/'evidence/eval-01',seal,registration)
 d=binding['descriptor']
 if d['database']['schema']!=provision['schema'] or d['database']['schema_oid']!=provision['schema_oid'] or d['database']['audit_role']!=provision['roles']['audit'] or d['tenant']!=provision['tenant_id'] or d['api_url']!=provision['api_url'] or d['api_url']!='http://'+platform['listen']:raise ValueError('collector/provision/config identity differs')
 if manifest['provider_key_path']!=str(repo/'var/evaluation/deepseek.env'):raise ValueError('fixed operator-provided key path required')
 return root,scope,repo,manifest,provision,binding,platform,runner

def static_helper(root,revision,mode='static',uuid=''):
 argv=[str(root/'bin/eval-preflight'),'-root',str(root),'-revision',revision,'-mode',mode]
 if mode=='live':argv+=['-journal-uuid',uuid]
 return json.loads(c.output(argv,timeout=40),object_pairs_hook=c.deployment.pairs)

def seal(root):
 root,scope,repo=c.authority(root)
 if (root/'launch.json').exists() or (root/'deployment.json').exists():raise ValueError('seal output exists; preserve and inspect, never overwrite')
 provision=c.read_json(root/'private/provision.json');c.deadline(provision['launch_not_after'])
 corpus,bundle,registration,platform=c.evaluate.load_bundle(root/'candidate');runner=c.read_json(root/'candidate/runner.json');c.policy(platform,runner,registration,scope);c.provision_binding(root,provision,registration,platform)
 uuid,inputs=c.prior_evidence(scope);revision,files=c.source_identity(root/'source');checked=static_helper(root,revision)
 if provision.get('phase')!='ready' or provision.get('purpose')!='deepseek-eval-v2-provision' or provision['candidate_dir']!=str(root/'candidate') or provision['provider']!='deepseek' or provision['model']!='deepseek-v4-flash' or provision['budget']!={'total_microusd':2000000,'task_budgets_microusd':{k:500000 for k in c.common.CASES}}:raise ValueError('complete fixed-budget provisioned identity required')
 for name in ('candidate','platform','runner','registration'):
  path=root/'candidate'/(name+'.json');inputs[str(path)]=c.sha(path)
 for path in (root/'private/provision.json',scope/'runtime/runner.json',scope/'evidence/sigterm-01/abort-before-runner/report.json'):
  inputs[str(path)]=c.sha(path)
 evidence=root/'evidence'
 if not evidence.exists():evidence.mkdir(mode=0o700)
 c.directory(evidence)
 d={'version':1,'purpose':'isolated-eval-v1','authorization_id':'deepseek-flash-2usd','output_dir':str(evidence/'eval-01'),'bundle_dir':str(root/'candidate'),'candidate_sha256':c.sha(root/'candidate/candidate.json'),'registration_sha256':bundle['registration_sha256'],'corpus_sha256':bundle['corpus_sha256'],'provider':'deepseek','model_id':'deepseek-v4-flash','total_budget_microusd':2000000,'tenant':provision['tenant_id'],'api_url':provision['api_url'],'database':dict(provision['database'],schema=provision['schema'],schema_oid=provision['schema_oid'],audit_role=provision['roles']['audit']),'cli':{k:checked['binaries']['forge'][k] for k in ('path','sha256')}}
 c.deployment.validate(d)
 c.save(root/'deployment.json',d)
 manifest={'version':1,'purpose':c.PURPOSE,'evaluation_root':str(root),'revision':revision,'source_files':files,'binaries':checked['binaries'],'inputs':inputs,'journal_uuid':uuid,'launch_not_after':provision['launch_not_after'],'provider_key_path':str(repo/'var/evaluation/deepseek.env'),'collector_descriptor_sha256':c.sha(root/'deployment.json')}
 c.save(root/'launch.json',manifest)
 return {'sealed':True,'launch_manifest':str(root/'launch.json'),'services_started':False,'provider_called':False}

def envs(root,provision):
 api=c.literal_env(root/'private/api.env',{'FORGE_DATABASE_URL'},quoted=True)
 worker=c.literal_env(root/'private/worker-db.env',{'FORGE_DATABASE_URL'},quoted=True)
 audit=c.literal_env(root/'private/audit.env',{'FORGE_EVAL_AUDIT_DSN'},quoted=True)
 for kind,values,key in (('api',api,'FORGE_DATABASE_URL'),('worker',worker,'FORGE_DATABASE_URL'),('audit',audit,'FORGE_EVAL_AUDIT_DSN')):
  c.dsn_env(values[key],provision['roles'][kind],schema=None if kind=='audit' else provision['schema'])
 client=c.literal_env(root/'private/client.env',{'FORGE_API_URL','FORGE_TENANT','FORGE_TOKEN'},quoted=False)
 if client['FORGE_API_URL']!=provision['api_url'] or client['FORGE_TENANT']!=provision['tenant_id'] or not re.fullmatch('frg_[A-Z2-7]{52,128}',client['FORGE_TOKEN']):raise ValueError('client token/identity does not follow provisioned contract')
 return api,worker,audit

def preflight(root,*,mapped=False):
 root,scope,repo,m,p,b,platform,runner=load(root)
 uuid,_=c.prior_evidence(scope)
 if uuid!=m['journal_uuid']:raise ValueError('historical journal identity changed')
 c.private(m['provider_key_path'],16384)  # Metadata only: no key read/hash here.
 envs(root,p)
 s.socket_absent(runner['server']['unix_socket'])
 api_absent(p['api_url'])
 old=s.prior_processes(scope)
 check=static_helper(root,m['revision'],'live',m['journal_uuid'])
 admin,_=s.admin_environment(repo);prior=s.previous_databases(scope,admin);fresh=s.fresh_evaluation(root,p,admin)
 audit=s.audit_role_preflight(root,p,b)
 if p['schema'] in {x['schema'] for x in prior}:raise ValueError('evaluation cannot route a retained fixture schema')
 images=s.image_check(runner)
 absent=s.containers_absent(runner,check['journal']['operation_ids'])
 return {'observed_at':c.now(),'mapped':mapped,'launch_sha256':c.sha(root/'launch.json'),'prior_processes':old,'authority':check,'prior_databases':prior,'fresh_database':fresh,'audit_role':audit,'images':images,'owned_containers_absent':absent,'provider_key':'private-file metadata only; bytes and hash omitted','passed':True}

class OwnedProcess:
 def __init__(self,name,args,env,out):
  self.name=name;self.args=args;self.out=out;self.log=open(out/(name+'.log'),'xb');os.chmod(out/(name+'.log'),0o600)
  self.p=subprocess.Popen(args,env=env,cwd=out,stdin=subprocess.DEVNULL,stdout=self.log,stderr=subprocess.STDOUT)
  try:
   self.identity=s.process_identity(self.p.pid)
   if self.identity['args']!=args or self.identity['exe_path']!=args[0]:raise ValueError('new child identity mismatch')
   c.save(out/(name+'-start.json'),dict(self.identity,started_at=c.now(),environment_keys=sorted(env)))
  except BaseException:
   # Direct child handle only; do not search by name or clean its workspace.
   if self.p.poll() is None:self.p.send_signal(signal.SIGTERM)
   self.p.wait(timeout=15);self.log.close();raise
 def healthy(self):
  if self.p.poll() is not None:raise ValueError(self.name+' exited during readiness; retain its private log')
  if not s.same_process(self.identity,s.process_identity(self.p.pid)):raise ValueError('owned child identity changed')
 def stop(self):
  record={'name':self.name,'identity':self.identity,'observed_at':c.now()}
  if self.p.poll() is None:
   self.healthy();record['signal']='SIGTERM';record['signal_sent_at']=c.now();self.p.send_signal(signal.SIGTERM)
   try:self.p.wait(timeout=20)
   except subprocess.TimeoutExpired:
    c.save(self.out/(self.name+'-stop-unresolved.json'),dict(record,exited=False));raise ValueError('owned process graceful shutdown not confirmed')
  record.update(exited_at=c.now(),exit_code=self.p.returncode,exited=True);self.log.close();c.save(self.out/(self.name+'-stop.json'),record)
  if self.p.returncode!=0:raise ValueError(self.name+' exit was not clean; preserve state')
  return record

def wait_ready(check,processes,seconds=15):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  for p in processes:p.healthy()
  try:
   if check():return
  except (OSError,ConnectionError):pass
  time.sleep(.1)
 raise ValueError('bounded readiness timeout; no evaluation submission')

def runner_peer(path,proc):
 conn=socket.socket(socket.AF_UNIX);conn.settimeout(.25)
 try:
  conn.connect(path);pid,uid,gid=struct.unpack('3i',conn.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
  current=s.process_identity(pid)
  if pid!=proc.p.pid or uid!=os.getuid() or not s.same_process(proc.identity,current):raise ValueError('actual runner UDS peer differs from owned child')
  return {'peer_pid':pid,'uid':uid,'gid':gid,'identity':current,'observed_at':c.now()}
 finally:conn.close()

def api_ready(url):
 # Numeric loopback only; HTTPConnection does not inherit proxy settings.
 import http.client
 u=c.urlsplit(url);connection=http.client.HTTPConnection(u.hostname,u.port,timeout=.5)
 try:
  connection.request('GET','/readyz');response=connection.getresponse();response.read(4096);return response.status==200
 finally:connection.close()

def api_authentication(root,provision):
 # An authenticated GET of an absent, valid run ID is read-only. Compare the
 # actual API's forbidden and not_found responses; there is no project-list CLI.
 import http.client
 values=c.literal_env(root/'private/client.env',{'FORGE_API_URL','FORGE_TENANT','FORGE_TOKEN'},quoted=False)
 if values['FORGE_API_URL']!=provision['api_url'] or values['FORGE_TENANT']!=provision['tenant_id']:raise ValueError('API authentication identity differs')
 u=c.urlsplit(provision['api_url']);observed=[]
 for authenticated,status,code in ((False,403,'forbidden'),(True,404,'not_found')):
  connection=http.client.HTTPConnection(u.hostname,u.port,timeout=3)
  headers={'X-Forge-Tenant':values['FORGE_TENANT']}
  if authenticated:headers['Authorization']='Bearer '+values['FORGE_TOKEN']
  try:
   connection.request('GET','/v1/runs/eval_preflight_absent',headers=headers);response=connection.getresponse();raw=response.read(8193)
   if len(raw)>8192:raise ValueError('bounded API authentication response required')
   value=json.loads(raw)
   if response.status!=status or value.get('code')!=code:raise ValueError('API authentication/read-only route preflight failed')
   observed.append({'authenticated':authenticated,'status':response.status,'code':code,'request_id':value.get('request_id')})
  finally:connection.close()
 return {'observed_at':c.now(),'method':'GET','path':'/v1/runs/eval_preflight_absent','tenant':provision['tenant_id'],'responses':observed,'mutations':0}

def api_absent(url):
 u=c.urlsplit(url);sock=socket.socket();sock.settimeout(.5)
 try:code=sock.connect_ex((u.hostname,u.port))
 finally:sock.close()
 if code!=errno.ECONNREFUSED:raise ValueError('API port did not explicitly refuse a new TCP connection after own API exit')
 return {'observed_at':c.now(),'new_tcp_connection':'refused','errno':code,'api_url':url}

def mapped_identity():
 uid=Path('/proc/self/uid_map').read_text().split();gid=Path('/proc/self/gid_map').read_text().split()
 for rows in (uid,gid):
  if len(rows)!=6 or rows[:3]!=['0','1000','1'] or rows[3]!='1' or int(rows[5])<65536:raise ValueError('full subordinate UID/GID mapping required')
 if os.getuid()!=0:raise ValueError('runner must execute as mapped namespace UID0')
 return {'uid_map':uid,'gid_map':gid}

def scoped_admin(raw,schema):
 if not re.fullmatch('eval_ds_[a-f0-9]{32}',schema):raise ValueError('exact evaluation cleanup schema required')
 u=c.urlsplit(raw);return c.urlunsplit((u.scheme,u.netloc,u.path,c.urlencode({'sslmode':'disable','search_path':schema,'connect_timeout':'5'}),''))

def closure(row,report,provision):
 ids={x['run_id'] for x in report['tasks']}
 if len(ids)!=4 or any(not c.common.valid_id(x) for x in ids) or {x['id'] for x in row['runs']}!=ids or any(x['tenant']!=provision['tenant_id'] for x in row['runs']):raise ValueError('closure must cover exactly the four authorized saved runs')
 if report.get('complete') is not True or report.get('conservative_accounted_exposure_microusd',2000001)>2000000 or report.get('unknown_or_unsettled_reserved_microusd')!=0 or any(x.get('budget_violation') for x in report['tasks']):raise ValueError('batch remains incomplete, over cap or uncertain')
 s.idle_pg(row)
 return sorted(ids)

def mapped_child(root):
 mapping=mapped_identity();root,scope,repo,m,p,b,platform,runner=load(root);out=root/'evidence/launch'
 entitlement=c.read_json(scope/'evaluation/deepseek-flash-2usd.entitlement.json')
 if entitlement.get('evaluation_root')!=str(root) or entitlement.get('launch_sha256')!=c.sha(root/'launch.json') or entitlement.get('total_budget_microusd')!=2000000 or entitlement.get('never_automatically_renew') is not True:raise ValueError('child requires the original one-batch entitlement')
 intent=c.read_json(out/'host-intent.json');unit=intent.get('unit')
 if not isinstance(unit,str) or not re.fullmatch('forge-eval-deepseek-[a-f0-9]{16}\\.service',unit) or not any(unit in line.split('/') for line in Path('/proc/self/cgroup').read_text().splitlines()):raise ValueError('child must belong to its recorded dedicated service cgroup')
 with s.control_lock(scope):
  c.save(out/'child-preflight.json',preflight(root,mapped=True));c.save(out/'mapping.json',mapping)
  api_db,worker_db,audit=envs(root,p);base=dict(c.environment(),HOME=str(root/'private'),XDG_RUNTIME_DIR='/run/user/1000',FORGE_METRICS_LISTEN='127.0.0.1:0')
  processes={};proof={'started_at':c.now(),'complete':False,'scope':'one new isolated deployment; ordered idle shutdown composes with separately recorded busy SIGTERM evidence'}
  admin,raw_admin=s.admin_environment(repo)
  try:
   processes['runner']=OwnedProcess('runner',[str(root/'bin/forge-runner'),'-config',str(root/'candidate/runner.json')],base,out)
   peer={}
   def ready_runner():
    peer.update(runner_peer(runner['server']['unix_socket'],processes['runner']));return True
   wait_ready(ready_runner,list(processes.values()));c.save(out/'runner-peer.json',peer)
   processes['api']=OwnedProcess('api',[str(root/'bin/forge-api'),'-config',str(root/'candidate/platform.json')],dict(base,**api_db),out)
   wait_ready(lambda:api_ready(p['api_url']),list(processes.values()))
   c.save(out/'api-authentication.json',api_authentication(root,p))
   c.save(out/'before-worker-database.json',s.fresh_evaluation(root,p,admin))
   key=c.literal_env(Path(m['provider_key_path']),{'DEEPSEEK_API_KEY'},quoted=True)
   worker_env=dict(base,**worker_db,**key)
   processes['worker']=OwnedProcess('worker',[str(root/'bin/forge-worker'),'-config',str(root/'candidate/platform.json'),'-id',platform['worker_id']],worker_env,out)
   worker_env.pop('DEEPSEEK_API_KEY');key.clear()
   # A finite stability check ensures absence/malformed credentials cannot be
   # mistaken for a ready worker. It does not claim a remote credential probe.
   start=time.monotonic()
   while time.monotonic()-start<1:
    for process in processes.values():process.healthy()
    time.sleep(.1)
   c.verify_sealed_inputs(root,m);s.fresh_evaluation(root,p,admin)
   collector=[str(root/'source/scripts/evaluation/evaluate.py'),'--deployment',str(root/'deployment.json'),'--bundle',str(root/'candidate'),'--client-env',str(root/'private/client.env'),'--output',str(root/'evidence/eval-01'),'--execute']
   args=['/usr/bin/python3','-I','-B',*collector]
   c.save(out/'evaluation-intent.json',{'argv':args,'started_at':c.now(),'budget_microusd':2000000,'allocations':4,'automatic_retry':False})
   with open(out/'collector.log','xb') as log:
    os.chmod(out/'collector.log',0o600)
    evaluator=subprocess.Popen(args,env=dict(c.environment(),**audit),cwd=root/'source',stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT)
    try:
     code=evaluator.wait(timeout=2700)
    except subprocess.TimeoutExpired:
     evaluator.terminate();evaluator.wait(timeout=10);raise ValueError('collector exceeded fixed batch deadline; do not resubmit')
   proof['collector_exit_code']=code
   if code:raise ValueError('collector stopped; saved intents/unknown costs and work remain retained')
   report=c.read_json(root/'evidence/eval-01/report.json');row=s.sql_json(s.QUERIES,admin,p['schema'])
   c.save(out/'before-cleanup-database.json',row);ids=closure(row,report,p)
   # Only known terminal IDs through the production seal/publish/release path.
   cleanup_env=dict(base,FORGE_DATABASE_URL=scoped_admin(raw_admin,p['schema']))
   for run in ids:
    argv=[str(root/'bin/forge-admin'),'-config',str(root/'candidate/platform.json'),'-tenant',p['tenant_id'],'-run',run,'-age','0s','workspace-gc']
    with open(out/('cleanup-'+run+'.log'),'xb') as log:
     os.chmod(out/('cleanup-'+run+'.log'),0o600)
     result=subprocess.run(argv,env=cleanup_env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,timeout=90)
    if result.returncode:raise ValueError('exact terminal workspace cleanup not confirmed')
   before_shutdown=s.sql_json(s.QUERIES,admin,p['schema']);closure(before_shutdown,report,p)
   c.save(out/'before-shutdown-database.json',before_shutdown)
   proof['complete']=True
  except BaseException as error:
   proof['failure']=type(error).__name__+': '+str(error)
   raise
  finally:
   # This order is admission fence/drain/reap, then stop claiming, then detach
   # runner execution. No business Cancel, SIGKILL, Docker kill or rm fallback.
   proof['shutdown']=[]
   for name in ('api','worker','runner'):
    if name in processes:
     try:
      proof['shutdown'].append(processes[name].stop())
      if name=='api':c.save(out/'api-new-connection-refused.json',api_absent(p['api_url']))
     except BaseException as err:proof['complete']=False;proof.setdefault('shutdown_errors',[]).append(type(err).__name__+': '+str(err))
   try:
    row=s.sql_json(s.QUERIES,admin,p['schema']);c.save(out/'after-shutdown-database.json',row)
    if proof['complete']:
     closure(row,c.read_json(root/'evidence/eval-01/report.json'),p)
     final=static_helper(root,m['revision'],'live',m['journal_uuid'])
     final['owned_containers_absent']=s.containers_absent(runner,final['journal']['operation_ids'])
     c.save(out/'after-shutdown-authority.json',final)
   except BaseException as err:proof['complete']=False;proof['final_audit_error']=type(err).__name__+': '+str(err)
   proof['finished_at']=c.now();c.save(out/'result.json',proof)
  if not proof['complete']:raise ValueError('deployment closure incomplete; retain exact state')
 return 0

def host_argv(root,unit,state_dir):
 return ['/usr/bin/systemd-run','--user','--unit='+unit,'--collect','--wait','--pipe','--property=Delegate=yes','--property=KillMode=control-group','--property=RuntimeMaxSec=3300s','--property=TimeoutStopSec=45s','--working-directory='+str(root),'/usr/bin/env','-i','PATH=/usr/local/bin:/usr/bin:/bin','LC_ALL=C','TMPDIR=/tmp','HOME='+str(root/'private'),'XDG_RUNTIME_DIR=/run/user/1000','/usr/bin/rootlesskit','--propagation=rslave','--state-dir='+str(state_dir),'/usr/bin/python3','-I','-B',str(root/'source/scripts/evaluation/isolated.py'),'child','--evaluation-root',str(root)]

def launch(root):
 if os.getuid()!=1000 or os.getuid()!=os.geteuid():raise ValueError('launch from the ordinary UID1000 host session')
 root,scope,repo,m,p,b,platform,runner=load(root)
 with s.control_lock(scope):observed=preflight(root)
 # A permanent one-batch entitlement prevents a second evaluation directory
 # from silently reauthorizing the same $2 registration, even after failures.
 entitlement=scope/'evaluation/deepseek-flash-2usd.entitlement.json'
 out=root/'evidence/launch';out.mkdir(mode=0o700)
 c.save(out/'host-preflight.json',observed)
 c.save(entitlement,{'purpose':c.PURPOSE,'evaluation_root':str(root),'launch_sha256':c.sha(root/'launch.json'),'registration_sha256':c.sha(root/'candidate/registration.json'),'total_budget_microusd':2000000,'created_at':c.now(),'never_automatically_renew':True})
 unit='forge-eval-deepseek-'+secrets.token_hex(8)+'.service';state_dir=Path('/run/user/1000')/unit.removesuffix('.service')
 if state_dir.exists() or s.unit_state(unit).get('LoadState')!='not-found':raise ValueError('new dedicated unit and namespace directory required')
 argv=host_argv(root,unit,state_dir)
 c.save(out/'host-intent.json',{'unit':unit,'argv':argv,'observed_at':c.now(),'input':c.sha(root/'launch.json'),'rootlesskit_sha256':c.sha('/usr/bin/rootlesskit')})
 with open(out/'host-execution.log','xb') as log:
  os.chmod(out/'host-execution.log',0o600)
  process=subprocess.Popen(argv,env=dict(c.environment(),XDG_RUNTIME_DIR='/run/user/1000',DBUS_SESSION_BUS_ADDRESS='unix:path=/run/user/1000/bus'),stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT)
  try:code=process.wait(timeout=3390)
  except subprocess.TimeoutExpired:
   process.terminate();process.wait(timeout=10);code=124
 state=s.unit_state(unit);empty=s.unit_empty(unit,state);c.save(out/'host-result.json',{'exit_code':code,'unit':unit,'after':state,'cgroup':empty,'finished_at':c.now()})
 if state.get('LoadState')!='not-found' and (state.get('ActiveState') not in ('inactive','failed') or state.get('MainPID')!='0'):raise ValueError('new dedicated service termination remains unconfirmed')
 if code==0 and c.read_json(out/'result.json').get('complete') is not True:raise ValueError('child did not confirm exact deployment closure')
 return code

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=('seal','preflight','launch','child'));parser.add_argument('--evaluation-root',required=True);parser.add_argument('--report')
 args=parser.parse_args();root=Path(args.evaluation_root)
 if args.action=='seal':print(json.dumps(seal(root),sort_keys=True));return 0
 if args.action=='preflight':
  root,scope,_=c.authority(root)
  if not args.report or Path(args.report).parent!=root/'evidence':raise ValueError('explicit new E/evidence report path required')
  if os.path.lexists(args.report):raise ValueError('preflight report exists; retain it')
  try:
   with s.control_lock(scope):value=preflight(root)
  except BaseException as error:
   c.save(Path(args.report),{'passed':False,'observed_at':c.now(),'error_type':type(error).__name__,'error':str(error),'services_started':False,'provider_called':False})
   raise
  c.save(Path(args.report),value);print(json.dumps({'preflight_passed':True,'report':args.report}));return 0
 if args.report:raise ValueError('--report belongs to preflight only')
 return launch(root) if args.action=='launch' else mapped_child(root)
if __name__=='__main__':
 try:sys.exit(main())
 except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as err:
  print('isolated evaluation stopped: '+str(err),file=sys.stderr);sys.exit(1)

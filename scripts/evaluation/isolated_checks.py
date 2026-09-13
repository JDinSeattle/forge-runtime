"""Exact-scope local validation for the one DeepSeek eval-v2 deployment."""
from __future__ import annotations
import datetime, hashlib, importlib.util, json, os, re, shlex, stat, subprocess
from pathlib import Path
from urllib.parse import urlsplit, unquote, parse_qsl, urlencode, urlunsplit

HERE=Path(__file__).absolute().parent
spec=importlib.util.spec_from_file_location('isolated_evaluate',HERE/'evaluate.py');evaluate=importlib.util.module_from_spec(spec);spec.loader.exec_module(evaluate)
deployment=evaluate.deployment; common=evaluate.common
PURPOSE='isolated-deepseek-launch-v1'
HISTORICAL_MANIFEST_SHA='3ff0f3548c756214e9bdd41032c749d1b873d5322e680baac3a5850a125cf15d'
PYTHON_IMAGE='python@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36'
GO_IMAGE='golang@sha256:9fdc884aacc3bec89b20ffc69f4bb369c78210e3e4f600387b5128b12c199f81'
CASE_KEYS={'L1','L2-L3-default','L3-bytes','L3-count','L4','L5'}
BINARIES=('forge','forge-api','forge-worker','forge-runner','forge-admin','eval-provision','eval-preflight')

def canonical(path):
 p=Path(path)
 if not p.is_absolute() or str(p)!=str(path) or p.resolve()!=p: raise ValueError('canonical absolute path required')
 return p

def private(path,limit=1<<20):
 p=canonical(path);s=p.lstat()
 if not stat.S_ISREG(s.st_mode) or s.st_nlink!=1 or s.st_uid!=os.getuid() or s.st_mode&0o077 or s.st_size>limit:raise ValueError('private owned single-link file required')
 return p

def directory(path):
 p=canonical(path);s=p.stat()
 if not stat.S_ISDIR(s.st_mode) or s.st_uid!=os.getuid() or s.st_mode&0o077:raise ValueError('private owned directory required')
 return p

def read_json(path,limit=1<<20):return json.loads(private(path,limit).read_bytes(),object_pairs_hook=deployment.pairs)
def sha(path):return deployment.digest(canonical(path))
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def deadline(value):
 parsed=datetime.datetime.fromisoformat(evaluate.valid_timestamp(value))
 if datetime.datetime.now(datetime.timezone.utc)>=parsed:raise ValueError('frozen launch deadline expired; no automatic renewal')
 return parsed

def save(path,value):
 directory(path.parent);fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'wb') as f:f.write((json.dumps(value,indent=2,sort_keys=True)+'\n').encode());f.flush();os.fsync(f.fileno())
 fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(fd)
 finally:os.close(fd)

def environment():return {'PATH':'/usr/local/bin:/usr/bin:/bin','LC_ALL':'C','TMPDIR':'/tmp'}
def output(argv,env=None,timeout=30):
 r=subprocess.run(argv,env=environment() if env is None else env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=timeout)
 if r.returncode:raise ValueError('bounded local inspection failed: '+Path(argv[0]).name)
 return r.stdout

def literal_env(path,keys,*,quoted):
 result={}
 try:lines=private(path,16384).read_text().splitlines()
 except UnicodeError:raise ValueError('private environment is not valid UTF-8') from None
 for line in lines:
  key,sep,value=line.partition('=')
  if not sep or key not in keys or key in result:raise ValueError('unexpected or duplicate private environment entry')
  if quoted:
   parsed=shlex.split(value,posix=True)
   if len(parsed)!=1:raise ValueError('one literal private environment value required')
   value=parsed[0]
  if not value or any(ord(c)<32 for c in value):raise ValueError('invalid private environment value')
  result[key]=value
 if set(result)!=set(keys):raise ValueError('missing private environment entry')
 return result

def dsn_env(raw,role,*,schema=None):
 try:
  u=urlsplit(raw);pairs=parse_qsl(u.query,keep_blank_values=True,strict_parsing=True);q=dict(pairs)
  if len(q)!=len(pairs) or u.scheme not in ('postgres','postgresql') or u.hostname!='127.0.0.1' or u.port!=32773 or u.path!='/forge' or u.fragment or not u.username or not u.password or unquote(u.username)!=role:raise ValueError()
  expected={'sslmode':'disable'} if schema is None else {'sslmode':'disable','search_path':schema,'connect_timeout':'5','application_name':'forge-eval-provision','options':''}
  if q!=expected or (schema is None and u.query!='sslmode=disable'):raise ValueError()
 except (TypeError,ValueError):raise ValueError('private DSN differs from exact provisioned role/routing') from None
 return dict(environment(),PGHOST='127.0.0.1',PGPORT='32773',PGDATABASE='forge',PGUSER=role,PGPASSWORD=unquote(u.password),PGSSLMODE='disable',PGCONNECT_TIMEOUT='5',PGOPTIONS='-c default_transaction_read_only=on -c search_path=pg_catalog')

def authority(root):
 root=directory(root);scope=root.parent.parent
 if root.parent.name!='evaluation' or scope.name!='lr20260912_a' or scope.parent.name!='lifecycle-rehearsals':raise ValueError('only the dedicated lifecycle evaluation scope is supported')
 repo=scope.parent.parent.parent
 if root/'source'!=common.REPO or Path(__file__).absolute()!=root/'source/scripts/evaluation/isolated_checks.py':raise ValueError('execute the frozen evaluator from E/source')
 return root,scope,repo

def client_transport(value,socket):
 # ClientConfig has no JSON tags; its Go marshal keys are PascalCase. Nested
 # TLSFiles and the independent runner ServerConfig retain snake_case tags.
 fields={'UnixSocket','TCPAddress','TLS','MaxMessageBytes','RPCTimeout','ReconcileTimeout'}
 if not isinstance(value,dict) or set(value)-fields or value.get('UnixSocket')!=socket:return False
 if type(value.get('TCPAddress','')) is not str or value.get('TCPAddress','')!='':return False
 for key in ('MaxMessageBytes','RPCTimeout','ReconcileTimeout'):
  if type(value.get(key,0)) is not int or value.get(key,0)!=0:return False
 tls=value.get('TLS',{})
 return isinstance(tls,dict) and not set(tls)-{'cert_file','key_file','ca_file','expected_peer_uri','server_name'} and all(type(v) is str and v=='' for v in tls.values())

def policy(platform,runner,registration,scope):
 if registration['provider']!='deepseek' or registration['model_id']!='deepseek-v4-flash' or registration['total_budget_microusd']!=2000000 or registration['task_budgets_microusd']!={k:500000 for k in common.CASES}:raise ValueError('fixed selected alias and four $0.50 allocations required')
 if (registration['max_model_rounds'],registration['max_tool_calls'],registration['max_runtime_seconds'])!=(20,64,600):raise ValueError('fixed run bounds required')
 if registration['model_spec'].get('exact_pricing') is not False or registration['model_spec']['credential_group']!='deepseek-flash-evalv2' or registration['model_spec']['price_version']!='deepseek-flash-peak-ceiling-20260912':raise ValueError('fixed conservative quote required')
 original=read_json(scope/'runtime/runner.json');compare=dict(runner,sources=original['sources'],profiles=original['profiles'])
 if compare!=original or runner.get('allow_test_backend') is not False:raise ValueError('runner authority or production backend changed')
 defaults={'entry_bytes':16384,'operation_bytes':524288,'run_bytes':16777216,'max_operations':32,'preview_bytes':65536}
 if runner.get('logs')!=defaults:raise ValueError('default strict log policy required')
 expected_profiles={};expected_sources={}
 for case in common.CASES:
  identity=common.SOURCE_PREFIX+case;expected_sources[identity]=str(common.CORPUS/case/'source')
  expected_profiles[identity]={'id':identity,'image':PYTHON_IMAGE if case.startswith('py-') else GO_IMAGE,'verify_command':common.grade_command(case,'regression'),'target_command':common.grade_command(case,'target'),'tmpfs_executable':case.startswith('go-'),'trusted_tests_dir':str(common.CORPUS/'graders'),'memory_bytes':256<<20,'workspace_quota_bytes':256<<20,'cpus':1,'pids':64,'user':'1000:1000'}
 if runner['sources']!=expected_sources or runner['profiles']!=expected_profiles:raise ValueError('source/profile/trusted grader command differs from frozen corpus')
 if set(platform['sources'])!=set(expected_sources):raise ValueError('extra or missing candidate source')
 for identity,path in expected_sources.items():
  source=platform['sources'][identity]
  if set(source)!={'path','hash','profile_id','has_target'} or source['path']!=path or source['profile_id']!=identity or source['has_target'] is not True:raise ValueError('platform source binding changed')
 run={'provider':'deepseek','model':'deepseek-v4-flash','max_model_rounds':20,'max_tool_calls':64,'max_cost_microusd':500000,'max_runtime_seconds':600}
 if platform['configs']!={registration['config_id']:run} or platform['models']!={'deepseek/deepseek-v4-flash':registration['model_spec']} or platform['providers']!={'deepseek':{'deepseek-v4-flash':registration['capabilities']}} or platform.get('fake_scripts') or platform['worker_slots']!=1:raise ValueError('platform routes, limits or worker concurrency changed')
 transport=platform['runner']
 if not client_transport(transport,runner['server']['unix_socket']) or platform['runner_id']!='application-fault-runner' or platform['artifact_root']!=runner['artifact_root'] or platform['signing_key_file']!=runner['signing_key_file']:raise ValueError('platform must retain exact existing runner authority')
 if platform.get('telemetry') or platform.get('otlp_endpoint'):raise ValueError('external telemetry configuration forbidden')
 return original

def provision_binding(root,provision,registration,platform):
 expected_hashes={name+'.json':sha(root/'candidate'/(name+'.json')) for name in ('candidate','platform','runner','registration')}
 expected_hashes['authority_runner.json']=sha(root.parent.parent/'runtime/runner.json')
 expected={'phase':'ready','purpose':'deepseek-eval-v2-provision','schema_version':1,'candidate_dir':str(root/'candidate'),'candidate_hashes':expected_hashes,'provider':'deepseek','model':'deepseek-v4-flash','config_id':registration['config_id'],'credential_group':registration['model_spec']['credential_group'],'price_version':registration['model_spec']['price_version'],'exact_pricing':False,'batch_id':'deepseek-flash-2usd-'+expected_hashes['registration.json'],'budget':{'total_microusd':2000000,'task_budgets_microusd':{k:500000 for k in common.CASES}},'runner_slots':4,'worker_slots':1,'runner_id':platform['runner_id'],'runner_endpoint':'unix://'+platform['runner']['UnixSocket'],'api_url':'http://'+platform['listen']}
 if any(provision.get(k)!=v or type(provision.get(k)) is not type(v) for k,v in expected.items()):raise ValueError('provisioned candidate/batch/config identity differs from current closed inputs')

FAILED_SECOND_MANIFEST_SHA='fb68b27b8a05a22f7c572d64788b044f81fdc4111c3c07a0dda9a724d1842da2'
SECOND_OBSERVATION_SHA='9ac0d6f20dcd256af3b8feaedef407b41e98cb481f532e37248284fd32217e2c'
SECOND_SCHEMA='appfault_strictlogs_ggl4ub23utvo6qnlzlkgroms5n'
SECOND_RUN='run_VZZ7CZCKDG65KNL3BW7ZH7F2O4'
SECOND_TENANT='sl-L1-d7wtgoelviiujwjs3rrqiragqp'

def failed_second_inputs(scope):
 allowed={}
 for name,pinned,count in (('logs-02',FAILED_SECOND_MANIFEST_SHA,15),('logs-02-before-runner-observation',SECOND_OBSERVATION_SHA,4)):
  base=scope/'evidence'/name;manifest=base/'manifest.json'
  if sha(manifest)!=pinned:raise ValueError('pinned second failure/observation manifest changed')
  members=read_json(manifest)
  if not isinstance(members,dict) or len(members)!=count:raise ValueError('exact second-failure manifest size required')
  if count==4 and set(members)!={'postgres.json','history.json','journal.json','report.json'}:raise ValueError('exact readonly observation files required')
  allowed[str(manifest)]=pinned
  for rel,want in members.items():
   p=Path(rel)
   if p.is_absolute() or str(p)!=rel or '..' in p.parts or any(x.endswith(('.env','.key')) or x=='database.json' for x in p.parts):raise ValueError('unsafe second failure manifest member')
   path=canonical(base/p);deployment.sha(want)
   if sha(path)!=want:raise ValueError('second failure/observation bytes changed')
   allowed[str(path)]=want
 fixed={
  'acceptance-logs-02.json':'5998c98594c5bd466fb58625931ad7ecf8c3a57ed0d26c46b111868e6dd14a3e',
  'evidence/host-logs-02-retry/intent.json':'3aebaaa6e1148e26c9403859358a5f304746d0849088c69e4637662e2fa53bae',
  'evidence/host-logs-02-retry/result.json':'b7bb3fa5f5baddc1e9b988d1aaeefd69f9c93eec16f36b920b6eb0e200e30f30',
  'runtime/engine/operator-faults/strict-log-publication.json':'d9a34f57a0da180085d0e8afe4a52f5b6889daaf4a8aa48a24c26973c4c3d2cd',
  'runtime/engine/operator-faults/strict-log-publication.json.used':'2f9b1402d5885a4dfbdf98db97d1b53170688cc03383c20950b669ee31f9cc52',
  'evidence/sigterm-02/worker-runner-sigterm/acceptance-input.json':'993e5f618144f008c88730f76e4294fbafca1c978477b24bfe49cdc0ad8320c3'}
 for relative,want in fixed.items():
  path=canonical(scope/relative)
  if sha(path)!=want:raise ValueError('fixed retained authority proof changed')
  allowed[str(path)]=want
 old=read_json(scope/'acceptance-logs-02.json')
 for name in ('runner','worker','test'):
  path=canonical(old[name+'_binary']);want=old[name+'_sha256'];deployment.sha(want)
  if not historical_path(scope,path,{}) or sha(path)!=want:raise ValueError('retained second-attempt binary changed')
  allowed[str(path)]=want
 report=read_json(scope/'evidence/logs-02-before-runner-observation/report.json')
 expected={'passed':True,'no_execution':True,'journal_unchanged_from_preflight':True,'status':'queued','version':1,'epoch':0,'schema':SECOND_SCHEMA,'run_id':SECOND_RUN,'tenant':SECOND_TENANT}
 if any(report.get(k)!=v or type(report.get(k)) is not type(v) for k,v in expected.items()) or read_json(scope/'evidence/logs-02/acceptance.json').get('passed') is not False:raise ValueError('second failure must remain a queued unstarted observation, not successful acceptance')
 return allowed

def historical_inputs(scope):
 # Only the pinned failed corpus, a closed cleanup archive grammar and the two
 # exact typed cleanup objects can extend historical identity. Never hash an
 # arbitrary path supplied by an acceptance/report manifest.
 old=scope/'evidence/logs-01';clean=scope/'evidence/logs-01-cleanup';allowed=failed_second_inputs(scope)
 def manifest(base,pinned=None,cleanup=False):
  path=base/'manifest.json';got=sha(path)
  if pinned and got!=pinned:raise ValueError('original failed logs manifest changed')
  members=read_json(path)
  if not isinstance(members,dict) or not 0<len(members)<=2000:raise ValueError('bounded historical manifest required')
  allowed[str(path)]=got
  top={'acceptance-input.json','intent.json','release-intent.json','release.json','released-journal.json','report.json','snapshot.bytes','snapshot.json','stop.bytes','stop.json'}
  for rel,want in members.items():
   p=Path(rel)
   if p.is_absolute() or str(p)!=rel or '..' in p.parts or any(x.endswith(('.env','.key')) or x=='database.json' for x in p.parts):raise ValueError('credential or unsafe historical manifest path')
   if cleanup and rel not in top and not re.fullmatch(r'invocation-[0-9]+/(journal-before\.json|operation-[0-9]{2}\.json|report\.json|runner-[0-9]{2}-bytes\.log|runner-[0-9]{2}-peer\.json|runner-[0-9]{2}-stop\.json)',rel):raise ValueError('unexpected cleanup archive member')
   deployment.sha(want);path=canonical(base/p)
   if path==base/'manifest.json' or sha(path)!=want:raise ValueError('historical archive bytes differ')
   allowed[str(path)]=want
  if cleanup and not top.issubset(members):raise ValueError('incomplete material cleanup archive')
 manifest(old,HISTORICAL_MANIFEST_SHA)
 manifest(clean,cleanup=True)
 run='sl-L3-bytes-46djebzw3ifph2ui7nesemhkz6'
 for name,key,kind in (('stop.json','ref','workspace_stop'),('snapshot.json','artifact','workspace_snapshot')):
  ref=read_json(clean/name)[key];deployment.sha(ref.get('sha256'))
  if ref.get('tenant_id')!='strict-log-fixture' or ref.get('run_id')!=run or ref.get('kind')!=kind or ref.get('object_key')!='strict-log-fixture/'+run+'/'+ref['sha256'] or type(ref.get('size')) is not int or not 0<ref['size']<=1<<20:raise ValueError('cleanup object binding differs')
  path=canonical(scope/'runtime/artifacts'/ref['object_key'])
  if path.stat().st_size!=ref['size'] or sha(path)!=ref['sha256']:raise ValueError('cleanup object bytes differ')
  allowed[str(path)]=ref['sha256']
 return allowed

def historical_path(scope,selected,allowed):
 if str(selected) in allowed:return True
 fixed={scope/'runtime/runner.json',scope/'runtime/runner-byteguard.json',scope/'runtime/runner-countguard.json',scope/'runtime/source/app.py',scope/'acceptance-02.json',scope/'acceptance-logs-02.json',scope/'acceptance-logs-03.json',scope/'acceptance-logs-cleanup-01.json',scope/'evidence/host-logs-02/intent.json',scope/'evidence/host-logs-02/result.json'}
 if selected in fixed:return True
 return selected.name in {'forge-runner','forge-worker','application-faults.test'} and (selected.parent==scope/'bin' or selected.parent.parent==scope/'bin' and re.fullmatch('[0-9a-f]{40}',selected.parent.name) is not None)

def composite_module():
 spec=importlib.util.spec_from_file_location('isolated_composite',HERE/'isolated_composite.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 return module

def composite_evidence(scope):
 # Pass the current validation namespace so the same IO contracts are used.
 from types import SimpleNamespace
 return composite_module().prior(SimpleNamespace(**globals()),scope)

def prior_evidence(scope):
 if (scope/'logs-l4-continuation.json').exists():
  uuid,inputs,_=composite_evidence(scope)
  return uuid,inputs
 sig=scope/'evidence/sigterm-02/worker-runner-sigterm';logs=scope/'evidence/logs-03'
 a=read_json(sig/'acceptance.json');b=read_json(logs/'acceptance.json');j=read_json(logs/'final-journal.json',16<<20);cleanup=read_json(scope/'evidence/logs-01-cleanup/report.json')
 if a.get('passed') is not True or b.get('passed') is not True or set(b.get('cases',{}))!=CASE_KEYS or any(v.get('passed') is not True for v in b['cases'].values()):raise ValueError('completed E50 and all six actual logs03 cases required')
 if cleanup.get('passed') is not True or cleanup.get('released') is not True or cleanup.get('snapshot_verified') is not True:raise ValueError('retained logs01 must have explicit successful cleanup')
 uuid=a.get('journal_uuid');deployment.sha(uuid)
 if j.get('identity')!=uuid or j.get('version')!=5 or b['cases']['L5'].get('same_journal_uuid')!=uuid:raise ValueError('prior evidence journal identities differ')
 allowed=historical_inputs(scope)
 before=b.get('input_sha256_before');after=b.get('input_sha256_after')
 if not isinstance(before,dict) or before!=after or str(scope/'runtime/runner.json') not in before:raise ValueError('prior frozen source/authority identity missing or changed')
 for path,want in before.items():
  selected=canonical(path)
  if not historical_path(scope,selected,allowed) or str(selected) in allowed and allowed[str(selected)]!=want or sha(selected)!=want:raise ValueError('historical nonsecret input identity differs')
 if not set(allowed).issubset(before):raise ValueError('logs03 did not bind complete material cleanup history')
 files=[sig/'acceptance.json',logs/'acceptance.json',logs/'final-journal.json',scope/'evidence/logs-01-cleanup/report.json']
 for name in ('sigterm-01','sigterm-02','logs-01','logs-02','logs-03'):
  base=scope/'evidence'/name
  if name.startswith('sigterm'):
   base=base/'worker-runner-sigterm';files.append(base/'worker-role-preflight.json')
   if name=='sigterm-02':files.append(base/'fixture.json')
  else:
   files.extend([base/'worker-role-preflight.json',base/'private-schema.json',base/'L1/fixture.json'])
   if name=='logs-03' or (base/'L4/fixture.json').exists():files.append(base/'L4/fixture.json')
 return uuid,{**before,**{str(p):sha(p) for p in files}}

def source_identity(source):
 revision=output(['/usr/bin/git','-C',str(source),'rev-parse','HEAD']).strip()
 if not re.fullmatch('[a-f0-9]{40}',revision) or output(['/usr/bin/git','-C',str(source),'status','--porcelain','--untracked-files=all']).strip():raise ValueError('one clean frozen source checkout required')
 names=output(['/usr/bin/git','-C',str(source),'ls-files','-z']).split('\0');files={}
 for name in names:
  if not name or name.startswith('benchmarks/'):continue
  p=source/name
  if p.suffix in ('.go','.sql','.py','.json','.yaml','.yml','.mod','.sum','.txt','.sh') or name in ('go.mod','go.sum'):
   if p.is_symlink() or not p.is_file():raise ValueError('source identity includes a nonregular file')
   files[name]=sha(p)
 return revision,files

def verify_sealed_inputs(root,manifest):
 expected={'version','purpose','evaluation_root','revision','source_files','binaries','inputs','journal_uuid','launch_not_after','provider_key_path','collector_descriptor_sha256'}
 if not isinstance(manifest,dict) or set(manifest)!=expected or type(manifest['version']) is not int or manifest['version']!=1 or manifest['purpose']!=PURPOSE or manifest['evaluation_root']!=str(root):raise ValueError('invalid exact launch manifest')
 deployment.sha(manifest['journal_uuid']);deadline(manifest['launch_not_after'])
 revision,files=source_identity(root/'source')
 if revision!=manifest['revision'] or files!=manifest['source_files']:raise ValueError('frozen source changed')
 if set(manifest['binaries'])!=set(BINARIES):raise ValueError('complete frozen binary set required')
 for name,info in manifest['binaries'].items():
  path=root/'bin'/name;st=path.stat()
  if info['path']!=str(path) or info['sha256']!=sha(path) or info['revision']!=revision or info['modified'] is not False or st.st_uid!=os.getuid() or st.st_nlink!=1 or st.st_mode&0o022 or not st.st_mode&0o100:raise ValueError('frozen executable changed')
 scope=root.parent.parent;_,prior=prior_evidence(scope)
 expected_inputs=set(prior)|{str(root/'candidate'/(name+'.json')) for name in ('candidate','platform','runner','registration')}|{str(root/'private/provision.json'),str(scope/'runtime/runner.json'),str(scope/'evidence/sigterm-01/abort-before-runner/report.json')}
 if not isinstance(manifest['inputs'],dict) or set(manifest['inputs'])!=expected_inputs:raise ValueError('only the exact closed nonsecret input set may be hashed')
 for path,want in manifest['inputs'].items():
  if sha(path)!=want:raise ValueError('sealed nonsecret input changed')
 if sha(root/'deployment.json')!=manifest['collector_descriptor_sha256']:raise ValueError('collector descriptor changed')

"""Read-only live authority guards; no schema mutations, Engine or container Start."""
from __future__ import annotations
import contextlib, fcntl, importlib.util, json, os, re, socket, stat, struct, subprocess
from pathlib import Path
from urllib.parse import urlsplit, unquote, parse_qsl, urlencode, urlunsplit
spec=importlib.util.spec_from_file_location('isolated_checks',Path(__file__).with_name('isolated_checks.py'));c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)

@contextlib.contextmanager
def control_lock(scope):
 path=c.private(scope/'runtime/.sigterm-control.lock',4096)
 fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC)
 try:
  fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  yield
 finally:os.close(fd)

def unit_state(unit):
 if not re.fullmatch('forge-(lifecycle|eval)-[a-zA-Z0-9_-]+\\.service',unit):raise ValueError('dedicated recorded unit identity required')
 r=subprocess.run(['/usr/bin/systemctl','--user','show',unit,'--property=LoadState,ActiveState,SubState,MainPID,ControlGroup,InvocationID'],env=dict(c.environment(),XDG_RUNTIME_DIR='/run/user/1000',DBUS_SESSION_BUS_ADDRESS='unix:path=/run/user/1000/bus'),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=10)
 value=dict(line.split('=',1) for line in r.stdout.splitlines() if '=' in line)
 if r.returncode and value.get('LoadState')!='not-found':raise ValueError('recorded dedicated unit inspection failed')
 return value

def unit_empty(unit,state):
 if state.get('LoadState')=='not-found':return {'unit':unit,'empty_or_absent':True,'unit_absent':True}
 if state.get('ActiveState') not in ('inactive','failed') or state.get('MainPID')!='0':raise ValueError('dedicated service remains active')
 group=state.get('ControlGroup','')
 if not group:return {'unit':unit,'empty_or_absent':True,'control_group_unassigned':True}
 if not group.startswith('/user.slice/') or '..' in group.split('/') or group.split('/')[-1]!=unit:raise ValueError('unexpected dedicated cgroup binding')
 path=Path('/sys/fs/cgroup'+group)/'cgroup.procs'
 try:members=path.read_text().strip()
 except FileNotFoundError:return {'unit':unit,'empty_or_absent':True,'cgroup_absent':True,'path':str(path)}
 if members:raise ValueError('dedicated cgroup retains processes')
 return {'unit':unit,'empty_or_absent':True,'path':str(path),'members':[]}

def process_identity(pid):
 if type(pid) is not int or pid<=1:raise ValueError('positive child process ID required')
 base=Path('/proc')/str(pid)
 raw=(base/'stat').read_text();end=raw.rfind(')');fields=raw[end+2:].split()
 if end<0 or len(fields)<20:raise ValueError('invalid kernel process identity')
 exe=os.readlink(base/'exe');args=(base/'cmdline').read_bytes().split(b'\0');args=[x.decode() for x in args if x]
 return {'pid':pid,'proc_start_ticks':fields[19],'exe_path':exe,'exe_sha256':c.deployment.digest(base/'exe'),'args':args,'state':fields[0]}

def same_process(a,b):
 return all(a.get(k)==b.get(k) for k in ('pid','proc_start_ticks','exe_path','exe_sha256','args'))

def prior_processes(scope):
 observed=[]
 names=['sigterm-01','sigterm-02','logs-01','logs-02','logs-03','logs-01-cleanup']
 composite=(scope/'logs-l4-continuation.json').exists()
 if composite:names+=['logs-03-recovery','logs-l4-01']
 for name in names:
  base=scope/'evidence'/name
  paths=set(base.rglob('*.identity.json'))
  if composite and name in ('logs-03-recovery','logs-l4-01'):paths.update(base.glob('runner-*-stop.json'))
  for path in sorted(paths):
   saved=c.read_json(path)
   try:live=process_identity(saved['pid'])
   except (FileNotFoundError,ProcessLookupError):observed.append({'path':str(path),'pid':saved['pid'],'exited':True});continue
   if saved.get('proc_start_ticks')==live['proc_start_ticks']:raise ValueError('recorded lifecycle process remains active')
   observed.append({'path':str(path),'pid':saved['pid'],'pid_reused':True,'exited':True})
 units=[]
 for path in sorted((scope/'evidence').glob('host-*/intent.json')):
  saved=c.read_json(path);unit=saved.get('unit');state=unit_state(unit)
  if state.get('LoadState')!='not-found' and (state.get('ActiveState') not in ('inactive','failed') or state.get('MainPID')!='0'):raise ValueError('recorded lifecycle unit remains active')
  group=state.get('ControlGroup','')
  if group:
   if not group.startswith('/user.slice/') or '..' in group.split('/'):raise ValueError('unexpected recorded cgroup')
   path=Path('/sys/fs/cgroup'+group)/'cgroup.procs'
   if path.exists() and path.read_text().strip():raise ValueError('recorded lifecycle cgroup is not empty')
  units.append({'unit':unit,'state':state,'empty_or_absent':True})
 result={'processes':observed,'units':units}
 if composite:
  _,_,result['log_evidence_composition']=c.composite_evidence(scope)
  result['targeted_journal_baseline']=c.composite_module().live_baseline(c,scope)
 return result

def socket_absent(path):
 if os.path.lexists(path):raise ValueError('runner socket exists; inspect prior shutdown rather than unlinking or taking over')

def admin_environment(repo):
 path=c.private(repo/'var/local/review-database.env',16384);raw=None
 for line in path.read_text().splitlines():
  key,sep,value=line.removeprefix('export ').partition('=')
  if key=='FORGE_REVIEW_DATABASE_URL':
   if raw is not None or not sep:raise ValueError('one explicit private admin DSN required')
   parts=c.shlex.split(value)
   if len(parts)!=1:raise ValueError('one literal private admin DSN required')
   raw=parts[0]
 if raw is None:raise ValueError('explicit private admin DSN missing')
 try:
  u=urlsplit(raw);q=parse_qsl(u.query,keep_blank_values=True,strict_parsing=True)
  if u.scheme not in ('postgres','postgresql') or u.hostname!='127.0.0.1' or u.port!=32773 or u.path!='/forge' or u.fragment or not u.username or not u.password or q not in ([],[('sslmode','disable')]):raise ValueError()
 except (TypeError,ValueError):raise ValueError('private admin must select dedicated loopback32773/forge') from None
 env=dict(c.environment(),PGHOST='127.0.0.1',PGPORT='32773',PGDATABASE='forge',PGUSER=unquote(u.username),PGPASSWORD=unquote(u.password),PGSSLMODE='disable',PGCONNECT_TIMEOUT='5',PGOPTIONS='-c default_transaction_read_only=on -c search_path=pg_catalog')
 return env,raw

def sql_json(query,env,schema,*,extra=None):
 if not re.fullmatch('(eval_ds_[a-f0-9]{32}|appfault_(lifecycle|strictlogs)_[a-z0-9]+)',schema):raise ValueError('exact known private schema required')
 args=['/usr/bin/psql','-X','-qAt','-v','ON_ERROR_STOP=1','-v','schema='+schema]
 for key,value in (extra or {}).items():
  if not re.fullmatch('[a-z_]+',key) or not isinstance(value,str) or '\0' in value:raise ValueError('invalid bounded SQL variable')
  args+=['-v',key+'='+value]
 query='BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;\n'+query+'\nROLLBACK;'
 r=subprocess.run(args,input=query,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=20)
 if r.returncode:raise ValueError('exact-schema read-only audit failed; diagnostic text suppressed to protect credentials')
 value=json.loads(r.stdout,object_pairs_hook=c.deployment.pairs)
 if value.get('database')!='forge' or value.get('current_user')!=env['PGUSER'] or value.get('superuser') is not True:raise ValueError('live guard requires the explicit read-only administrator identity')
 return value

QUERIES="""SELECT pg_catalog.json_build_object(
 'database',pg_catalog.current_database(),'current_user',current_user,'superuser',(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user),
 'schema', :'schema', 'schema_oid',(SELECT oid::bigint FROM pg_catalog.pg_namespace WHERE nspname=:'schema'),
 'now',pg_catalog.clock_timestamp(),
 'runs',COALESCE((SELECT json_agg(json_build_object('id',id,'tenant',tenant_id,'state',state,'version',version,'epoch',lease_epoch,'owner',lease_owner,'workspace',workspace_id,'snapshot',snapshot)) FROM :"schema".runs),'[]'::json),
 'effects',(SELECT count(*) FROM :"schema".effects),
 'unsettled_effects',(SELECT count(*) FROM :"schema".effects WHERE status NOT IN ('skipped','succeeded','failed','cancelled')),
 'attempts',(SELECT count(*) FROM :"schema".model_attempts),
 'artifacts',(SELECT count(*) FROM :"schema".artifacts),
 'projects',(SELECT count(*) FROM :"schema".projects),
 'active',(SELECT COALESCE(sum(active_count),0) FROM :"schema".tenant_runtime),
 'runner_reserved',(SELECT COALESCE(sum(reserved_slots),0) FROM :"schema".runners),
 'allocations',(SELECT count(*) FROM :"schema".runner_allocations WHERE state<>'released'),
 'requests',(SELECT COALESCE(sum(active_requests),0) FROM :"schema".provider_quotas),
 'unsettled_reservations',(SELECT count(*) FROM :"schema".quota_reservations WHERE status<>'settled' OR NOT request_slot_released),
 'cleanup',(SELECT count(*) FROM :"schema".workspace_cleanup WHERE phase<>'released'),
 'quotas',COALESCE((SELECT json_agg(to_jsonb(q)) FROM :"schema".provider_quotas q),'[]'::json)
);"""

def idle_pg(row,old=None):
 for key in ('unsettled_effects','active','runner_reserved','allocations','requests','unsettled_reservations','cleanup'):
  if type(row.get(key)) is not int or row[key]!=0:raise ValueError('retained PostgreSQL authority: '+key)
 if old is not None:
  runs=row['runs']
  if row['schema']!=old['schema'] or len(runs)!=1 or runs[0]['id']!=old['run_id'] or runs[0]['state']!='cancel_requested' or runs[0]['version']!=2 or runs[0]['epoch']!=0 or runs[0]['owner'] or runs[0]['workspace'] or runs[0]['snapshot']!=old['after']['state'] or any(row[x]!=0 for x in ('effects','attempts','artifacts')):raise ValueError('original unstarted run must remain unchanged and unrouted')
 elif any(x['state'] not in c.evaluate.TERMINAL for x in row['runs']):raise ValueError('prior run remains nonterminal')

SECOND_HISTORY_QUERY="""SELECT json_build_object('database',current_database(),'current_user',current_user,'superuser',(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user),
 'runs',(SELECT json_agg(to_jsonb(r)) FROM :"schema".runs r),
 'events',(SELECT json_agg(to_jsonb(r) ORDER BY seq) FROM :"schema".run_events r),
 'snapshots',(SELECT json_agg(to_jsonb(r) ORDER BY version) FROM :"schema".run_snapshots r),
 'steps',(SELECT json_agg(to_jsonb(r) ORDER BY seq) FROM :"schema".steps r),
 'quota_reservations',(SELECT count(*) FROM :"schema".quota_reservations),
 'runner_allocations',(SELECT count(*) FROM :"schema".runner_allocations));"""

def second_unstarted(scope,row,admin):
 # The original immutable observer selected pg_catalog.oid, whose JSON is a
 # string; current queries explicitly cast bigint. Only this typed OID is
 # normalized. Every saved run/snapshot/history value stays exact.
 base=scope/'evidence/logs-02-before-runner-observation';saved=c.read_json(base/'postgres.json')
 if saved.get('schema')!=c.SECOND_SCHEMA or row.get('schema')!=c.SECOND_SCHEMA:raise ValueError('exact unstarted second schema required')
 if not re.fullmatch('[0-9]+',str(saved.get('schema_oid'))):raise ValueError('saved schema OID must be numeric')
 saved=dict(saved,schema_oid=int(saved['schema_oid']))
 if type(row.get('schema_oid')) is not int:raise ValueError('live schema OID must be bigint JSON number')
 keys=set(saved)-{'now','current_user'}
 if set(row)-{'now','current_user'}!=keys or json.dumps({k:row[k] for k in keys},sort_keys=True)!=json.dumps({k:saved[k] for k in keys},sort_keys=True):raise ValueError('unstarted second PostgreSQL row/snapshot/capacity changed')
 runs=row['runs']
 if len(runs)!=1 or (runs[0]['id'],runs[0]['tenant'],runs[0]['state'],runs[0]['version'],runs[0]['epoch'],runs[0]['owner'],runs[0]['workspace'])!=(c.SECOND_RUN,c.SECOND_TENANT,'queued',1,0,'',''):raise ValueError('second queued run binding changed')
 if any(row[k]!=0 for k in ('effects','unsettled_effects','attempts','artifacts','active','runner_reserved','allocations','requests','unsettled_reservations','cleanup')):raise ValueError('second failed fixture acquired work')
 recorded=c.read_json(base/'history.json');history=sql_json(SECOND_HISTORY_QUERY,admin,c.SECOND_SCHEMA)
 fields=('runs','events','snapshots','steps','quota_reservations','runner_allocations')
 if json.dumps({k:history.get(k) for k in fields},sort_keys=True)!=json.dumps({k:recorded.get(k) for k in fields},sort_keys=True) or history.get('steps') not in (None,[]) or history.get('quota_reservations')!=0 or history.get('runner_allocations')!=0:raise ValueError('second failed fixture immutable history or dispatch changed')
 return {'schema':c.SECOND_SCHEMA,'run_id':c.SECOND_RUN,'status':'queued','version':1,'epoch':0,'unchanged_saved_snapshot_and_history':True,'worker_routed':False}

def previous_databases(scope,admin):
 report=c.read_json(scope/'evidence/sigterm-01/abort-before-runner/report.json')
 if report.get('passed') is not True or report.get('terminal') is not False or report.get('status')!='cancel_requested':raise ValueError('original before-runner abort intent evidence required')
 result=[];seen=set()
 names=['sigterm-private','sigterm-private-02','strict-logs-private','strict-logs-private-02','strict-logs-private-03']
 composite=(scope/'logs-l4-continuation.json').exists()
 if composite:names.append('strict-logs-l4-private-01')
 for name in names:
  descriptor=c.read_json(scope/'runtime'/name/'database.json');schema=descriptor.get('schema');u=urlsplit(descriptor.get('worker_dsn',''))
  q=dict(parse_qsl(u.query));prefix='appfault_lifecycle_' if name.startswith('sigterm') else 'appfault_strictlogs_'
  if not isinstance(schema,str) or not schema.startswith(prefix) or schema in seen or u.hostname!='127.0.0.1' or u.port!=32773 or u.path!='/forge' or q.get('search_path')!=schema:raise ValueError('retained fixture database binding changed')
  attempt='03' if name.endswith('-03') else '02' if name.endswith('-02') else '01';base=scope/'evidence'/('sigterm-'+attempt if name.startswith('sigterm') else 'logs-'+attempt)
  if name=='strict-logs-l4-private-01':base=scope/'evidence/logs-l4-01'
  if name.startswith('sigterm'):base=base/'worker-runner-sigterm'
  recorded=c.read_json(base/'worker-role-preflight.json')
  if recorded.get('schema')!=schema or recorded.get('current_user')!=unquote(u.username or '') or recorded.get('session_user')!=recorded['current_user'] or recorded.get('production_CheckWorkerRole')!='passed':raise ValueError('private DSN differs from the original public role/schema binding')
  seen.add(schema);row=sql_json(QUERIES,admin,schema)
  if name=='strict-logs-private-02':second_unstarted(scope,row,admin)
  else:idle_pg(row,report if name=='sigterm-private' else None)
  if name!='sigterm-private':
   fixtures=[c.read_json(base/'fixture.json')] if name.startswith('sigterm') else [c.read_json(path) for path in (base/'L1/fixture.json',base/'L4/fixture.json') if path.exists()]
   if not fixtures or {(x['id'],x['tenant']) for x in row['runs']}!={(x['run_id'],x['tenant']) for x in fixtures}:raise ValueError('prior database no longer contains its exact recorded runs')
   if not name.startswith('sigterm') and c.read_json(base/'private-schema.json').get('schema')!=schema:raise ValueError('original strict-log schema changed')
  if composite and name in ('strict-logs-private-03','strict-logs-l4-private-01'):
   c.composite_module().database_closure(c,scope,row,name)
  result.append(row)
 return result

def fresh_evaluation(root,report,admin):
 row=sql_json(QUERIES,admin,report['schema'])
 if type(row['schema_oid']) is not int or row['schema_oid']!=report['schema_oid'] or any(row[x]!=0 for x in ('projects','effects','attempts','artifacts')) or row['runs']:raise ValueError('evaluation schema is not the fresh provisioned identity')
 idle_pg(row)
 if len(row['quotas'])!=1:raise ValueError('one exact provider quota required')
 quota=row['quotas'][0]
 for key in ('active_requests','reserved_tokens','committed_tokens','reserved_microusd','committed_microusd'):
  if quota.get(key)!=0:raise ValueError('evaluation quota already used')
 for key in ('credential_group','max_concurrent','max_tokens','max_microusd','window_generation','window_ends_at'):
  if quota.get(key)!=report['initial_quota'].get(key):raise ValueError('provider quota identity/window changed')
 if quota['credential_group']!='deepseek-flash-evalv2' or quota['max_microusd']!=2000000 or quota['max_concurrent']!=1 or quota['window_generation']!=1:raise ValueError('fixed initial budget required')
 db_now=c.datetime.datetime.fromisoformat(c.evaluate.valid_timestamp(row['now']))
 for end in (quota['window_ends_at'],report['launch_not_after'],report['token_expires_at']):
  if db_now>=c.datetime.datetime.fromisoformat(c.evaluate.valid_timestamp(end)):raise ValueError('trusted database clock exceeds the original provisioned deadline')
 c.deadline(quota['window_ends_at']);c.deadline(report['launch_not_after']);c.deadline(report['token_expires_at'])
 return row

def audit_role_preflight(root,provision,binding):
 values=c.literal_env(root/'private/audit.env',{'FORGE_EVAL_AUDIT_DSN'},quoted=True)
 env=c.dsn_env(values['FORGE_EVAL_AUDIT_DSN'],provision['roles']['audit'])
 query='BEGIN TRANSACTION READ ONLY;\n'+(c.HERE/'audit_scope.sql').read_text().strip()+';\nROLLBACK;'
 argv=['/usr/bin/psql','-X','-qAt','-v','ON_ERROR_STOP=1','-v','schema='+provision['schema']]
 r=subprocess.run(argv,input=query,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=20)
 if r.returncode:raise ValueError('exact audit-role preflight failed; credentials not printed')
 scope=json.loads(r.stdout,object_pairs_hook=c.deployment.pairs);c.deployment.check_scope(scope,binding)
 return scope

def docker_read(runner,*args):
 if runner['docker_host']!='unix:///run/user/1000/forge-runtime-docker.sock' or runner.get('docker_binary') not in (None,'','/usr/bin/docker'):raise ValueError('dedicated rootless daemon and fixed CLI required')
 return c.output(['/usr/bin/docker','--host',runner['docker_host'],*args],env=dict(c.environment(),HOME=str(Path(runner['root_dir']).parent)),timeout=20)

def image_check(runner):
 result=[]
 for image in (c.PYTHON_IMAGE,c.GO_IMAGE):
  value=json.loads(docker_read(runner,'image','inspect',image))
  if len(value)!=1 or image not in value[0].get('RepoDigests',[]):raise ValueError('required pinned image not present; no automatic pull')
  result.append({'requested':image,'id':value[0]['Id'],'repo_digests':value[0]['RepoDigests']})
 return result

def containers_absent(runner,operation_ids):
 if len(operation_ids)>10000 or len(set(operation_ids))!=len(operation_ids):raise ValueError('bounded unique journal operation inventory required')
 for identity in operation_ids:
  if not c.common.valid_id(identity):raise ValueError('invalid retained operation identity')
  if docker_read(runner,'ps','--all','--no-trunc','--filter','label=forge.operation_id='+identity,'--format','{{.ID}}').strip():raise ValueError('journal-owned container remains after recorded release; preserve and inspect')
 return {'checked_operation_ids':operation_ids,'remaining':0,'scope':'read-only exact journal operation-label queries; no daemon-wide deletion or unrelated container operation'}

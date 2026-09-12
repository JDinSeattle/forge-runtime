"""Read-only, explicit composition of five logs03 cases and one targeted L4.

This is an evidence consumer, never an execution/recovery or budget authority.
All paths are admitted before hashing. The failed aggregate is immutable.
"""
from __future__ import annotations
import base64, datetime, hashlib, json, os, re, sqlite3, stat, struct, zlib
from pathlib import Path

FAILED_SHA='8781bba443e5a83da96cb077eb18223facfeee3469de0b23fce6c37529eef97c'
OBS_SHA='7ace4cab71b3f4fbaba9f8ed58ae73dbfe202ab94d83bb4ecbe85bac6d8ab736'
LEASE_SHA='5277e3980f434163dddb5dbfa45d7b19446b4a0f95b2b496cb0a5b33c5c8679c'
UUID='b010dd7440a54c58601967342f397faf686b7e306061af9f49809ee342b0ec8e'
RUN='run_DKT2OOLEVCKYXHW5NGNS7BKBHJ'; TENANT='sl-L4-xsra53ckdacsus345hmddtehsj'
SCHEMA='appfault_strictlogs_zg4ub4tnaruvpaojcj5dmmeqpc'; OID=852843
REVISION='bdff9b1b511ae1c38327b8441ac973bb4db262e2'
OLD_CASES={'L1','L2-L3-default','L3-bytes','L3-count','L5'}
TABLES={'volume_slots','volume_leases','workspaces','operations','operation_logs','log_runs','runner_artifacts'}
PRODUCTION=('internal','cmd','db','proto','go.mod','go.sum')
RECOVERY_TOP={
 'acceptance-input.json','postgres-before.json','journal-before.json','containers-before.json','docker-before.json','preflight.json',
 'pressure.gz','pressure-archive.json','pressure-delete-intent.json','pressure-deleted.json','cancel-requested.json','claimed.json',
 'terminal.json','settled-journal.json','cleanup.json','artifacts.json','postgres-after.json','closure-oracle.json','control-rpc-calls.json',
 'final-journal.json','report.json'}
TARGET_TOP={'config-default.json','config-bytes.json','config-count.json','acceptance.json','preflight.json','private-schema.json','worker-role-preflight.json','final-journal.json',
 'L4-before-pressure.spool','L4-worker-paused.json','L4-worker-pause.json','L4-before-pressure-journal.json','L4-pressure.json',
 'L4-spool-failure-journal.json','L4-stopped-before-pressure-removal.json','L4-physical-full.spool',
 'L4-physical-full-spool-oracle.json','L4-physical-full.meta','L4-physical-full.meta.tmp','L4-full-spool-stop-proof.json',
 'L4-pressure-removed.json','L4-http-proof.json','L4-http.flg','L4-idle.json','L4-health-workspace.json',
 'L4-health-stop.json','L4-health-snapshot.json','L4-health-release.json','L4-health-released-journal.json'}
OP_SUFFIX={'request.json','operation.json','journal.json','log.flg','receipt.json','disk.spool','disk.meta','docker.json','disk-identity.json','oracle.json'}

def require(ok,message):
 if not ok:raise ValueError(message)

def exact(value,expected,message):
 require(all(type(value.get(k)) is type(v) and value[k]==v for k,v in expected.items()),message)

def timestamp(value):
 require(isinstance(value,str),'timestamp required')
 result=datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
 require(result.tzinfo is not None,'timestamp requires timezone')
 return result

def safe_relative(relative):
 p=Path(relative)
 return (isinstance(relative,str) and not p.is_absolute() and str(p)==relative and '..' not in p.parts
         and all(x not in ('database.json','runner.key') and not x.endswith(('.env','.key')) for x in p.parts))

def generated_member(relative,kind):
 if not safe_relative(relative):return False
 if re.fullmatch(r'runner-[0-9]{2}-((?:bytes|default)\.log|peer\.json|stop\.json)',relative):return True
 if kind=='recovery':return relative in RECOVERY_TOP or re.fullmatch(r'artifacts/[A-Za-z0-9_-]+\.bytes',relative) is not None
 if relative in TARGET_TOP:return True
 if any(relative=='L4-'+label+'-'+suffix for label in ('recovered','health') for suffix in OP_SUFFIX):return True
 return re.fullmatch(r'L4/(fixture\.json|sl-L4\.log(?:\.identity\.json)?|worker-finished-signal\.json|(?:physical-full|terminal|cleanup)-postgres\.json|cleanup\.json|closure-oracle\.json|artifacts/[A-Za-z0-9_-]+\.json)',relative) is not None

def archive(c,base,*,pinned=None,count=None,kind=None,required=()):
 manifest=base/'manifest.json';digest=c.sha(manifest)
 require(pinned is None or digest==pinned,'pinned failed/observation manifest changed')
 members=c.read_json(manifest,1<<20)
 require(isinstance(members,dict) and 0<len(members)<=2000 and (count is None or len(members)==count),'bounded exact manifest membership required')
 require(set(required).issubset(members),'required material evidence missing')
 # Check the whole grammar before any member read, even for a pinned manifest.
 for rel,want in members.items():
  require(safe_relative(rel) and rel!='manifest.json' and (kind is None or generated_member(rel,kind)),'unexpected or credential archive path')
  c.deployment.sha(want)
 actual=set()
 for root,dirs,files in os.walk(base,followlinks=False):
  require(all(not (Path(root)/d).is_symlink() for d in dirs),'symlink in evidence tree')
  for file in files:
   rel=str((Path(root)/file).relative_to(base))
   if rel!='manifest.json':actual.add(rel)
 require(actual==set(members),'manifest must cover the exact complete archive')
 result={str(manifest):digest}
 for rel,want in members.items():
  p=c.canonical(base/rel);st=p.lstat()
  require(stat.S_ISREG(st.st_mode) and st.st_nlink==1 and st.st_size<=64<<20,'bounded regular evidence member required')
  require(c.sha(p)==want,'evidence bytes changed');result[str(p)]=want
 return result

def rows(j,table,key,value):
 found=[x for x in j['tables'][table] if x.get(key)==value]
 require(len(found)==1,'one bound journal row required: '+table)
 return found[0]

def canonical_journal(j):
 require(j.get('identity')==UUID and j.get('version')==5 and set(j.get('tables',{}))==TABLES,'journal UUID/version/table set changed')
 result={}
 for table,values in j['tables'].items():
  require(isinstance(values,list) and len(values)<=10000,'bounded journal rows required')
  normalized=[]
  for row in values:
   row=dict(row)
   for key in ('request_json','result_json','receipt_json','ref_json','spec_json','policy_json'):
    if isinstance(row.get(key),str) and row[key]:
     parsed=json.loads(row[key])
     if key=='request_json':
      # The authoritative sampler strips Grant; no capability is hashed here.
      require(parsed.get('grant','')=='','public journal cannot contain a capability')
      parsed['grant']=''
     row[key]=parsed
   normalized.append(json.dumps(row,sort_keys=True,separators=(',',':')))
  result[table]=sorted(normalized)
 return result

def idle(j):
 canonical_journal(j)
 require(len(j['tables']['volume_slots'])==4,'four fixed slots required')
 require(all(x.get('status') in ('succeeded','failed','cancelled') for x in j['tables']['operations']),'unresolved journal operation')
 require(all(x.get('released')==1 and x.get('active_operation')=='' for x in j['tables']['workspaces']),'workspace not released')
 require(all(x.get('released')==1 for x in j['tables']['volume_leases']),'volume lease not released')
 require(all(x.get('cleanup_state')=='removed' for x in j['tables']['operation_logs']),'owned container cleanup unconfirmed')

def closed_inputs(c,scope,report,allowed):
 before=report.get('input_sha256_before')
 require(isinstance(before,dict) and before and before==report.get('input_sha256_after'),'immutable before/after inputs changed')
 for raw,want in before.items():
  selected=c.canonical(raw);c.deployment.sha(want)
  require(safe_relative(str(selected.relative_to(scope))),'secret or unsafe input')
  require(c.historical_path(scope,selected,allowed),'input is outside the closed nonsecret history')
  require(str(selected) not in allowed or allowed[str(selected)]==want,'input differs from pinned history')
  require(c.sha(selected)==want,'input identity changed')
 return before

def binary_build(c,path,revision):
 go='/home/postedism/.local/share/mise/installs/go/1.26.8/bin/go'
 raw=c.output([go,'version','-m',str(path)])
 require(len(raw)<65536 and raw.splitlines()[0].endswith(': go1.26.8'),'frozen Go toolchain required')
 fields={}
 for line in raw.splitlines()[1:]:
  parts=line.strip().split('\t')
  if len(parts)==2 and parts[0]=='path':fields['package']=parts[1]
  if len(parts)==2 and parts[0]=='build' and '=' in parts[1]:
   k,v=parts[1].split('=',1);require(k not in fields,'duplicate build setting');fields[k]=v
 exact(fields,{'package':'github.com/JDinSeattle/forge-runtime/cmd/'+path.name,'vcs.revision':revision,'vcs.modified':'false','GOOS':'linux','GOARCH':'amd64'},'actual executable build identity differs')
 return {k:fields[k] for k in ('package','vcs.revision','vcs.modified','GOOS','GOARCH')}

def compatibility(c,scope,old,phases):
 normalized=lambda a:{k:v for k,v in a.items() if k not in {x+'_'+y for x in ('runner','worker','test') for y in ('binary','sha256')}}
 source=Path(c.__file__).resolve().parents[2]
 def tree(revision):
  require(re.fullmatch('[0-9a-f]{40}',revision) is not None,'full frozen Git revision required')
  raw=c.output(['/usr/bin/git','-C',str(source),'ls-tree','-r','-z',revision,'--',*PRODUCTION])
  entries={x.split('\t',1)[1]:x.split('\t',1)[0] for x in raw.split('\0') if x}
  require(all(any(k==p or k.startswith(p+'/') for k in entries) for p in PRODUCTION),'complete production Git objects required')
  return entries
 baseline=tree(REVISION);proof=[]
 for label,a in [('logs03',old),*phases]:
  require(normalized(a)==normalized(old),'phase changed non-binary authority')
  revisions=set();binaries={}
  for name,filename in (('runner','forge-runner'),('worker','forge-worker'),('test','application-faults.test')):
   p=c.canonical(a[name+'_binary']);digest=a[name+'_sha256'];c.deployment.sha(digest)
   require(p.name==filename and p.parent.parent==scope/'bin' and re.fullmatch('[a-f0-9]{40}',p.parent.name) is not None,'phase binary revision path differs')
   require(c.sha(p)==digest,'phase executable bytes changed');revisions.add(p.parent.name)
   binaries[name]={'path':str(p),'sha256':digest}
   if name!='test':binaries[name]['build_info']=binary_build(c,p,p.parent.name)
  require(len(revisions)==1,'phase binaries must share one frozen revision')
  rev=revisions.pop();require(label!='logs03' or rev==REVISION,'original logs03 revision changed')
  require(tree(rev)==baseline,'production Git objects changed, including proto/generated Go')
  proof.append({'phase':label,'revision':rev,'binaries':binaries})
 return {'original_aggregate_passed':False,'production_paths':list(PRODUCTION),'production_objects_sha256':hashlib.sha256(json.dumps(baseline,sort_keys=True).encode()).hexdigest(),'phases':proof,'scope':'Git object compatibility and exact phase executable hashes; earlier cases retain their original executable identity. Fixture/tool/document changes are allowed.'}

def frames(raw):
 require(0<len(raw)<=524288,'bounded nonempty log prefix required')
 pos=0;sequence=0;streams=[bytearray(),bytearray()]
 while len(raw)-pos>=32:
  h=raw[pos:pos+32];length=int.from_bytes(h[16:20],'big')
  if not(h[:4]==b'FLG1' and h[4] in (1,2) and h[5:8]==b'\0'*3 and h[24:32]==b'\0'*8 and int.from_bytes(h[8:16],'big')==sequence and 0<length<=16384-32 and pos+32+length<=len(raw)):break
  data=raw[pos+32:pos+32+length]
  if zlib.crc32(data)!=int.from_bytes(h[20:24],'big'):break
  streams[h[4]-1]+=data;pos+=32+length;sequence+=1
 require(pos>0 and all(streams),'both actual streams required')
 return raw[:pos]

def artifact_binding(ref,request,kind,raw):
 exact(ref,{'tenant_id':request['tenant_id'],'run_id':request['run_id'],'kind':kind,'sha256':hashlib.sha256(raw).hexdigest(),'size':len(raw)},'receipt artifact binding differs')
 require(ref.get('object_key')==request['tenant_id']+'/'+request['run_id']+'/'+ref['sha256'],'artifact key differs')

def operation(c,base,label,*,success):
 request=c.read_json(base/(label+'-request.json'));op=c.read_json(base/(label+'-operation.json'),2<<20)
 require(request.get('grant','')=='' and op.get('request')==request,'RPC immutable operation differs')
 raw=c.private(base/(label+'-log.flg'),524288).read_bytes();receipt=c.private(base/(label+'-receipt.json'),2<<20).read_bytes()
 durable=json.loads(receipt,object_pairs_hook=c.deployment.pairs)
 require(durable.get('request')==request and durable.get('status')==op.get('status') and durable.get('result')==op.get('result'),'original receipt differs from RPC')
 artifact_binding(op['receipt'],request,'operation_receipt',receipt)
 job=durable['result'];log=job['log'];artifact_binding(log['artifact'],request,'operation_log',raw)
 require(frames(raw)==raw and log.get('retained_bytes')==len(raw),'published FLG prefix differs')
 require((op.get('status')=='succeeded') if success else op.get('status')=='failed','operation result differs')
 require(log.get('complete') is success,'capture completeness differs')
 if not success:require(log.get('dropped_known') is False and log.get('dropped_bytes')==0,'incomplete capture invented exact dropped bytes')
 j=c.read_json(base/(label+'-journal.json'),16<<20);jr=rows(j,'operations','id',request['operation_id'])
 q=json.loads(jr['request_json']);q['grant']=''
 require(q==request and jr['status']==op['status'] and jr['job_id']==op['job_id'],'original SQLite operation was changed/restarted')
 lr=rows(j,'operation_logs','operation_id',request['operation_id']);cid=lr.get('container_id')
 d=c.read_json(base/(label+'-docker.json'));require(len(d)==1,'one original container required');d=d[0]
 require(d['Id']==cid and d['Name']=='/'+op['job_id'] and job['id']==op['job_id'],'actual container name/ID differs')
 exact(d['State'],{'Running':False,'OOMKilled':False},'container not stopped without OOM')
 require(d['LogPath']=='' and d['HostConfig']['LogConfig']['Type']=='none' and d['HostConfig']['NetworkMode']=='none' and d['HostConfig']['ReadonlyRootfs'] is True,'actual log/sandbox configuration differs')
 labels=d['Config']['Labels']
 require(labels.get('forge.operation_id')==request['operation_id'] and labels.get('forge.runtime')=='1','actual container labels differ')
 identity=c.read_json(base/(label+'-disk-identity.json'))
 require(identity['spool_device']==identity['checkout_device'] and identity['spool_inode']>0,'spool not on workspace filesystem')
 return request,op,log,j,d

# Exact finite operator fixture: the 20-second oracle is invalid for any other
# program, even when all caller-controlled copies of that program agree.
ENOSPC_PROGRAM = r"""import os,time
os.write(1,b'SL_STDOUT\x00\xff');os.write(2,b'SL_STDERR\x00\xfe')
time.sleep(8)
for i in range(300):
 os.write(1,b'X'*4096);os.write(2,b'Y'*4096);time.sleep(.05)"""

def canonical_args(args):
 return json.dumps(args,sort_keys=True,separators=(',',':'),ensure_ascii=False).replace('&','\\u0026').replace('<','\\u003c').replace('>','\\u003e').replace('\u2028','\\u2028').replace('\u2029','\\u2029').encode()

def effect_binding(effect,request):
 exact(effect,{k:request[k] for k in ('tenant_id','run_id','operation_id','kind','args','args_hash','policy_version','expected_revision','epoch')},'PG effect immutable request binding differs')
 raw=canonical_args(request['args'])
 require(request['args_hash']==hashlib.sha256(raw).hexdigest() and effect.get('canonical_args')=='\\x'+raw.hex(),'PG canonical argument bytes differ')

def ready_receipt(row,ref,request):
 exact(row,{k:ref[k] for k in ('tenant_id','run_id','kind','object_key','sha256')},'PG READY receipt identity differs')
 exact(row,{'state':'ready','byte_size':ref['size']},'PG READY receipt size/state differs')
 require(row['kind']=='operation_receipt' and row['tenant_id']==request['tenant_id'] and row['run_id']==request['run_id'],'foreign PG receipt')

def health_cleanup(c,base,request,op,final):
 stop=c.read_json(base/'L4-health-stop.json');snap=c.read_json(base/'L4-health-snapshot.json');release=c.read_json(base/'L4-health-release.json')
 expected={'tenant_id':request['tenant_id'],'run_id':request['run_id'],'id':request['workspace_id'],'epoch':request['epoch'],'revision':op['after_revision']}
 for v in (stop['workspace'],snap['workspace']):
  exact(v,expected,'health workspace tuple differs')
  require(v.get('active_operation','')=='' and v.get('stopped') is True and v.get('released') is False,'health workspace is not stopped before release')
 require(stop['workspace']==snap['workspace'] and stop['no_active_operations'] is True,'health stop/snapshot differ')
 exact(release,{'workspace_id':request['workspace_id'],'released':True},'health release differs')
 objects={}
 def read_object(ref,kind):
  for key in ('tenant_id','run_id'):
   require(re.fullmatch('[A-Za-z0-9_-]{1,128}',request[key]) is not None,'unsafe artifact identity')
  c.deployment.sha(ref.get('sha256'))
  require(ref.get('object_key')==request['tenant_id']+'/'+request['run_id']+'/'+ref['sha256'],'health artifact key differs')
  path=c.canonical(base.parent.parent/'runtime/artifacts'/ref['object_key'])
  raw=c.private(path,64<<20).read_bytes();artifact_binding(ref,request,kind,raw)
  pin=rows(final,'runner_artifacts','object_key',ref['object_key']);bound=json.loads(pin['ref_json'])
  require(all(bound.get(k)==ref[k] for k in ('tenant_id','run_id','object_key','sha256','size')),'health artifact durable pin differs')
  objects[str(path)]=ref['sha256']
  return json.loads(raw,object_pairs_hook=c.deployment.pairs)
 stopped=read_object(stop['ref'],'workspace_stop')
 require(stopped.get('workspace')==stop['workspace'] and stopped.get('no_active_operations') is True,'health stop artifact body differs')
 body=read_object(snap['artifact'],'workspace_snapshot')
 require(body.get('workspace')==snap['workspace'] and isinstance(body.get('files'),dict),'health snapshot body differs')
 digest=hashlib.sha256()
 for name,file in sorted(body['files'].items()):
  require(safe_relative(name) and type(file.get('executable')) is bool,'invalid snapshot file')
  raw=base64.b64decode(file['content'],validate=True)
  require(hashlib.sha256(raw).hexdigest()==file['sha256'],'snapshot file digest differs')
  digest.update((str(len(name.encode()))+':'+name+':'+file['sha256']+':'+str(file['executable']).lower()+'\n').encode())
 require(snap.get('hash')==digest.hexdigest(),'snapshot tree hash differs')
 w=rows(final,'workspaces','id',request['workspace_id'])
 exact(w,dict(expected,released=1,stopped=1,active_operation=''),'final health workspace differs')
 lease=rows(final,'volume_leases','workspace_id',request['workspace_id'])
 exact(lease,{'tenant_id':request['tenant_id'],'run_id':request['run_id'],'released':1},'final health volume lease differs')
 return objects

def targeted(c,base,final,object_inputs=None):
 a=c.read_json(base/'acceptance.json')
 require(a.get('passed') is True and set(a.get('cases',{}))=={'L4'},'actual targeted L4 must pass alone')
 case=a['cases']['L4']
 exact(case,{'passed':True,'physical_errno':'ENOSPC','actual_spool_failure_observed':True,'pressure_removed_after_actual_stop':True,'same_id_inspection_only':True,'all_four_volume_identities_reverified':True,'fresh_real_health_operation':True},'full targeted L4 proof missing')
 req,op,log,j,d=operation(c,base,'L4-recovered',success=False)
 expected_args={'command':['python','-I','-B','-c',ENOSPC_PROGRAM]}
 require(req.get('kind')=='run_command' and req.get('args')==expected_args and req.get('args_hash')==hashlib.sha256(canonical_args(expected_args)).hexdigest(),'early-stop oracle requires exact finite ENOSPC command bytes')
 fixture=c.read_json(base/'L4/fixture.json');require((req['tenant_id'],req['run_id'],req['operation_id'])==(fixture['tenant'],fixture['run_id'],fixture['target_operation']),'target fixture identity differs')
 pressure=c.read_json(base/'L4-pressure.json');stages=pressure.get('stages',[])
 require([x.get('chunk_bytes') for x in stages]==[1<<20,4096,1] and all(x.get('error')=='no space left on device' and x.get('synced') is True and type(x.get('written_bytes')) is int and x['written_bytes']>=0 for x in stages),'actual refined ENOSPC and sync required')
 require(pressure.get('fill_error')=='<nil>' and pressure['owner_uid']==pressure['effective_uid']==0 and sum(x['written_bytes'] for x in stages)==pressure.get('written_bytes') and 0<pressure['allocated_bytes']<=256<<20 and pressure['after']['Bavail']==0 and pressure['device']>0 and pressure['inode']>0,'bounded physical pressure/zero Bavail missing')
 disk=c.read_json(base/'L4-recovered-disk-identity.json');require(pressure['device']==disk['spool_device'],'pressure/spool devices differ')
 lease=rows(j,'volume_leases','workspace_id',req['workspace_id']);slot=rows(j,'volume_slots','id',lease['slot_id']);spec=json.loads(slot['spec_json'])
 require(pressure['path']==str(Path(spec['mount_path'])/('workspace-'+req['workspace_id'])/'strict-logs-enospc-pressure'),'pressure path outside original trusted sibling')
 removed=c.read_json(base/'L4-pressure-removed.json');exact(removed,{'device':pressure['device'],'inode':pressure['inode'],'only_after_confirmed_job_stop':True},'pressure cleanup identity differs')
 stop=c.read_json(base/'L4-stopped-before-pressure-removal.json');require(len(stop)==1 and stop[0]['Id']==d['Id'],'stop observation changed original container');state=stop[0]['State']
 begin=timestamp(state['StartedAt']);end=timestamp(state['FinishedAt']);pause=c.read_json(base/'L4-worker-pause.json')
 require(state['Running'] is False and state['OOMKilled'] is False and state['ExitCode']!=0 and 0<(end-begin).total_seconds()<20,'natural exit/OOM is not spool-failure stop')
 require(timestamp(pause['stopped_confirmed_at'])<end<timestamp(removed['at'])<timestamp(pause['continued_at'])<timestamp(req['deadline']) and pause.get('watchdog') is False and not pause.get('resume_error') and pause['lease_epoch']==req['epoch'],'paused worker/immutable deadline ordering differs')
 full=c.private(base/'L4-physical-full.spool',524288).read_bytes();before=c.private(base/'L4-before-pressure.spool',524288).read_bytes()
 require(full.startswith(before) and len(full)<524288-16384 and frames(full)==c.private(base/'L4-recovered-log.flg',524288).read_bytes(),'retained prefix differs or byte-limit could explain stop')
 failure=c.read_json(base/'L4-spool-failure-journal.json',16<<20);row=rows(failure,'operations','id',req['operation_id'])
 original=c.read_json(base/'L4-before-pressure-journal.json',16<<20)
 for snapshot in (original,failure,j,final):
  r=rows(snapshot,'operations','id',req['operation_id']);lr=rows(snapshot,'operation_logs','operation_id',req['operation_id'])
  q=json.loads(r['request_json']);q['grant']=''
  require(q==req and r['job_id']==op['job_id'] and r['dispatch_started']==1 and r['docker_start_intent']==1 and lr['container_id']==d['Id'],'same immutable operation/container/start intent changed')
 full_pg=c.read_json(base/'L4/physical-full-postgres.json',16<<20)
 require(len(full_pg['runs'])==1 and (full_pg['runs'][0]['id'],full_pg['runs'][0]['state'],full_pg['runs'][0]['lease_epoch'])==(req['run_id'],'running',req['epoch']),'physical error observed after business cancellation/epoch change')
 require('no space left' in row.get('error','') or 'spool_io_error' in row.get('result_json',''),'actual SQLite sink-failure observation missing')
 require(rows(failure,'volume_leases','workspace_id',req['workspace_id'])['released']==0,'unresolved full workspace prematurely released')
 http=c.read_json(base/'L4-http-proof.json');body=c.private(base/'L4-http.flg',524288).read_bytes()
 exact(http,{'status':200,'list_status':200,'listed_matches':1,'cross_tenant_download':404,'cross_tenant_list':404,'body_sha256':log['artifact']['sha256'],'body_bytes':len(body)},'real authorized HTTP/publication proof missing')
 require(body==frames(full),'HTTP bytes differ from original retained prefix')
 art=http['artifact'];ref=log['artifact']
 exact(art,{k:ref[k] for k in ('tenant_id','run_id','kind','object_key','sha256')},'HTTP artifact differs from receipt')
 exact(art,{'state':'ready','byte_size':len(body)},'HTTP artifact not READY')
 pg=c.read_json(base/'L4/cleanup-postgres.json',16<<20)
 matched=[x for x in pg['artifacts'] if x['id']==art['id'] and x['tenant_id']==req['tenant_id']]
 require(len(matched)==1 and all(matched[0].get(k)==v for k,v in art.items() if k!='created_at') and timestamp(matched[0]['created_at'])==timestamp(art['created_at']),'actual PG READY binding differs')
 effect=[x for x in pg['effects'] if x['operation_id']==req['operation_id']]
 require(len(effect)==1 and effect[0]['status']=='failed' and effect[0]['epoch']==req['epoch'],'original PG effect identity/status differs')
 effect_binding(effect[0],req)
 receipt=[x for x in pg['artifacts'] if x['id']==effect[0]['receipt_ref']]
 require(len(receipt)==1,'one PG effect receipt required');ready_receipt(receipt[0],op['receipt'],req)
 closure=c.read_json(base/'L4/closure-oracle.json')
 exact(closure,{'ready_receipt_joined_effects':1,'durable_effect_confirmations':1,'tenant_active':0,'runner_reserved':0,'unreleased_allocations':0,'provider_active_requests':0},'actual PG closure missing')
 clean=c.read_json(base/'L4/cleanup.json');require(clean['tenant_id']==req['tenant_id'] and clean['run_id']==req['run_id'] and clean['phase']=='released' and clean['snapshot_ref'],'target cleanup incomplete')
 health,h_op,h_log,h_j,h_d=operation(c,base,'L4-health',success=True)
 require(health['operation_id']!=req['operation_id'] and health['workspace_id']!=req['workspace_id'],'health must be a separate actual operation')
 for label,request in [('L4-recovered',req),('L4-health',health)]:
  r=rows(final,'operations','id',request['operation_id']);require(r['status']==('failed' if label=='L4-recovered' else 'succeeded'),'final original operation status changed')
  rows(final,'operation_logs','operation_id',request['operation_id'])
 health_objects=health_cleanup(c,base,health,h_op,final)
 if object_inputs is not None:object_inputs.update(health_objects)
 return fixture


def prior(c,scope):
 # The existence of the explicit seal selects this contract, never a failed
 # aggregate or a missing final-journal fallback.
 seal=c.read_json(scope/'logs-l4-continuation.json')
 exact(seal,{'purpose':'strict-logs-targeted-l4-v1','recovered_run_id':RUN,'failed_manifest_sha256':FAILED_SHA,'observation_manifest_sha256':OBS_SHA},'explicit targeted continuation seal differs')
 allowed=c.historical_inputs(scope)
 oldbase=scope/'evidence/logs-03';recovery=scope/'evidence/logs-03-recovery';target=scope/'evidence/logs-l4-01'
 old_inputs=archive(c,oldbase,pinned=FAILED_SHA,count=602);allowed.update(old_inputs)
 observation_inputs=archive(c,scope/'evidence/logs-03-retained-observation',pinned=OBS_SHA,count=6);allowed.update(observation_inputs)
 allowed.update(archive(c,scope/'evidence/logs-03-lease-observation',pinned=LEASE_SHA,count=3))
 for name in ('recovery_manifest_sha256','acceptance_sha256'):c.deployment.sha(seal.get(name))
 recovery_inputs=archive(c,recovery,pinned=seal['recovery_manifest_sha256'],kind='recovery',required=RECOVERY_TOP);allowed.update(recovery_inputs)
 allowed.update(archive(c,target,kind='target',required={'acceptance.json','preflight.json','final-journal.json','L4/fixture.json','L4/cleanup-postgres.json'}))
 for relative in ('logs-l4-continuation.json','acceptance-logs-03-recovery.json','acceptance-logs-l4-01.json'):
  p=scope/relative;allowed[str(p)]=c.sha(p)
 require(allowed[str(scope/'acceptance-logs-l4-01.json')]==seal['acceptance_sha256'],'target acceptance seal changed')
 original=c.read_json(oldbase/'acceptance.json')
 require(original.get('passed') is False and set(original.get('cases',{}))==OLD_CASES and all(x.get('passed') is True for x in original['cases'].values()),'original failed aggregate/five cases changed')
 report=c.read_json(recovery/'report.json')
 exact(report,{'passed':True,'released':True,'snapshot_verified':True,'run_id':RUN,'tenant_id':TENANT,'schema':SCHEMA,'schema_oid':OID,'journal_uuid':UUID},'explicit old-run recovery incomplete')
 sig=c.read_json(scope/'evidence/sigterm-02/worker-runner-sigterm/acceptance.json')
 require(sig.get('passed') is True and sig.get('journal_uuid')==UUID and original['cases']['L5'].get('same_journal_uuid')==UUID,'E50/L5 original journal identity differs')
 old=c.read_json(oldbase/'preflight.json',16<<20)['acceptance'];a=c.read_json(scope/'acceptance-logs-l4-01.json');r=c.read_json(scope/'acceptance-logs-03-recovery.json')
 require(c.read_json(recovery/'acceptance-input.json')==r,'recovery acceptance changed')
 pre=c.read_json(target/'preflight.json',16<<20);require(pre['acceptance']==a,'target acceptance changed')
 compat=compatibility(c,scope,old,[('recovery',r),('targeted-L4',a)])
 final=c.read_json(target/'final-journal.json',16<<20);recovered=c.read_json(recovery/'final-journal.json',16<<20)
 idle(recovered);idle(final)
 require(canonical_journal(pre['journal'])==canonical_journal(recovered),'targeted start did not use recovered journal baseline')
 config=c.read_json(scope/'runtime/runner.json')
 for slot in config['volume_slots']:
  p=c.canonical(Path(slot['mount_path'])/'.forge-pool.owner')
  require(p==scope/'pool-root/var/workspace-storage/mounts'/slot['id']/'.forge-pool.owner','unexpected pool owner path')
  require(stat.S_ISREG(p.lstat().st_mode) and p.stat().st_nlink==1 and p.stat().st_size<=4096,'bounded single-link pool owner marker required')
  require(p.read_bytes()==(config['journal_path']+'\n').encode(),'journal pool ownership changed')
  allowed[str(p)]=c.sha(p)
 files=[scope/'evidence/sigterm-02/worker-runner-sigterm/acceptance.json',scope/'evidence/sigterm-01/abort-before-runner/report.json']
 for name in ('sigterm-01','sigterm-02','logs-01','logs-02','logs-03'):
  b=scope/'evidence'/name
  if name.startswith('sigterm'):
   b=b/'worker-runner-sigterm';files.append(b/'worker-role-preflight.json')
   if name=='sigterm-02':files.append(b/'fixture.json')
  else:
   files.extend([b/'worker-role-preflight.json',b/'private-schema.json',b/'L1/fixture.json'])
   if (b/'L4/fixture.json').exists():files.append(b/'L4/fixture.json')
 for path in files:allowed[str(path)]=c.sha(path)
 observed_inputs=[]
 for value in (original,report,c.read_json(target/'acceptance.json')):
  observed_inputs.append(closed_inputs(c,scope,value,allowed));allowed.update(observed_inputs[-1])
 required=set(old_inputs)|set(observation_inputs)|set(recovery_inputs)|set(observed_inputs[0])|set(observed_inputs[1])|{str(scope/'logs-l4-continuation.json'),str(scope/'acceptance-logs-l4-01.json')}
 require(required.issubset(observed_inputs[2]),'target did not bind complete old failure/recovery history')
 for acceptance,inputs in zip((old,r,a),observed_inputs):
  for name in ('runner','worker','test'):
   require(inputs.get(acceptance[name+'_binary'])==acceptance[name+'_sha256'],'phase did not bind its actual executable')
 for phase,config_path in [('default','runner.json'),('bytes','runner-byteguard.json'),('count','runner-countguard.json')]:
  require(c.read_json(target/('config-'+phase+'.json'))==c.read_json(scope/'runtime'/config_path),'targeted config artifact differs from original authority')
 targeted(c,target,final,allowed)
 # This returned report is for observers; the launcher seal binds the complete
 # source and all evidence members, not a mutable summary or inferred PASS.
 return UUID,allowed,{'purpose':'logs03-five-plus-targeted-l4-v1','original_passed':False,'recovery_is_enospc_acceptance':False,'compatibility':compat,'final_journal':str(target/'final-journal.json'),'final_journal_sha256':allowed[str(target/'final-journal.json')]}


def live_baseline(c,scope):
 baseline=c.read_json(scope/'evidence/logs-l4-01/final-journal.json',16<<20);idle(baseline)
 config=c.read_json(scope/'runtime/runner.json');path=c.canonical(config['journal_path'])
 require(path==scope/'runtime/journal.sqlite','exact original journal path required')
 require(path.is_file(),'original journal missing')
 con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=3)
 try:
  con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
  result={'identity':con.execute('SELECT id FROM journal_identity WHERE singleton=1').fetchone()[0], 'version':con.execute('PRAGMA user_version').fetchone()[0], 'tables':{}}
  for table in sorted(TABLES):
   cur=con.execute('SELECT * FROM '+table);names=[x[0] for x in cur.description];values=cur.fetchmany(10001);require(len(values)<=10000,'journal size exceeds evidence bound')
   result['tables'][table]=[dict(zip(names,row)) for row in values]
  # Only the grant-stripped projection is compared; no raw journal is emitted.
  for row in result['tables']['operations']:
   q=json.loads(row['request_json']);q['grant']='';row['request_json']=json.dumps(q)
  require(canonical_journal(result)==canonical_journal(baseline),'live journal changed since targeted final baseline')
 finally:con.close()
 return {'baseline':str(scope/'evidence/logs-l4-01/final-journal.json'),'same_rows':True,'journal_uuid':UUID,'scope':'read-only snapshot with capability fields stripped before comparison; no raw journal output'}


def database_closure(c,scope,row,name):
 """Bind live original/targeted rows to their archived post-cleanup identity.

The SQL reader already uses a repeatable read-only transaction and rejects all
unsettled effects/capacities, including planned effects. These are observations,
not connection information to be forwarded to the evaluation worker.
 """
 original=name=='strict-logs-private-03'
 base=scope/'evidence'/('logs-03-recovery' if original else 'logs-l4-01')
 saved=c.read_json(base/('postgres-after.json' if original else 'L4/cleanup-postgres.json'),16<<20)
 public=c.read_json(scope/'evidence/logs-l4-01/private-schema.json') if not original else {'schema':SCHEMA}
 require(row['schema']==public['schema'] and type(row['schema_oid']) is int and row['schema_oid']>0,'original schema identity changed')
 if original:require(row['schema_oid']==OID,'original logs03 namespace OID changed')
 fields={'id':'id','tenant':'tenant_id','state':'state','version':'version','epoch':'lease_epoch','owner':'lease_owner','workspace':'workspace_id','snapshot':'snapshot'}
 expected=[{k:r[v] for k,v in fields.items()} for r in saved['runs']]
 require(sorted(row['runs'],key=lambda x:x['id'])==sorted(expected,key=lambda x:x['id']),'retained run snapshot/epoch/terminal authority changed')
 for count,table in [('effects','effects'),('attempts','model_attempts'),('artifacts','artifacts')]:
  require(row[count]==len(saved[table]),'retained execution/publication history changed')
 require(all(r['status'] in ('succeeded','failed','cancelled') for r in saved['effects']),'archived unresolved effect')
 if original:
  old=[r for r in row['runs'] if r['id']==RUN]
  require(len(old)==1 and old[0]['tenant']==TENANT and old[0]['state']=='cancelled','original L4 has not been cancelled through recovery')
  old_observation=c.read_json(scope/'evidence/logs-03-retained-observation/postgres.json',16<<20)
  prior=next(r for r in old_observation['runs'] if r['id']==RUN)
  require(timestamp(old[0]['snapshot']['limits']['deadline'])==timestamp(prior['snapshot']['limits']['deadline']),'recovery changed original immutable deadline')
 else:
  fixture=c.read_json(base/'L4/fixture.json')
  require(len(row['runs'])==1 and (row['runs'][0]['id'],row['runs'][0]['tenant'],row['runs'][0]['state'])==(fixture['run_id'],fixture['tenant'],'completed'),'targeted repair fixture terminal identity differs')
 return {'schema':row['schema'],'schema_oid':row['schema_oid'],'same_archived_terminal_rows':True}

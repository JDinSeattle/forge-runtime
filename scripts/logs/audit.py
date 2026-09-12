#!/usr/bin/env python3
"""Recompute E44 saved evidence; does not access Docker, services or a database."""
import argparse,base64,hashlib,json,pathlib,struct,zlib

def digest(path):
 return hashlib.sha256(path.read_bytes()).hexdigest()
def frames(raw,entry_max=16384,op_max=524288):
 assert len(raw)<=op_max
 position=0;records=0;streams={1:bytearray(),2:bytearray()}
 while position<len(raw):
  assert len(raw)-position>=32
  head=raw[position:position+32]
  sequence,length,crc=struct.unpack('>QII',head[8:24])
  assert head[:4]==b'FLG1' and head[4] in streams and head[5:8]==b'\0'*3 and head[24:32]==b'\0'*8
  assert sequence==records and 1<=length<=entry_max-32 and position+32+length<=len(raw)
  payload=raw[position+32:position+32+length]
  assert zlib.crc32(payload)==crc
  streams[head[4]].extend(payload);position+=32+length;records+=1
 return records,streams

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);p.add_argument('--backend',type=pathlib.Path);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args()
 root=a.root.resolve();evidence=root/'benchmarks/results/log-limits-e44'
 report=json.loads((evidence/'preflight-01/report.json').read_text());assert report['passed']
 events={v['kind']:v['monotonic_ns'] for v in report['events']}
 assert events['created']<events['attach_ack_before_start']<events['start_begin']<events['start_ack']<events['capture_complete']<events['owned_container_removed']
 assert base64.b64decode(report['streams']['1'])==b'early-out\0' and base64.b64decode(report['streams']['2'])==b'early-err\xff'
 assert report['log_config']['Type']=='none' and report['log_path']=='' and report['mounts']==[] and report['exit_code']==0
 assert digest(root/'scripts/logs/preflight_attach.py')==report['script_sha256']
 source=json.loads((evidence/'backend-build-inputs.json').read_text())
 for item in source['files']:
  path=root/item['path'];assert path.stat().st_size==item['size'] and digest(path)==item['sha256'],item['path']
 binary=json.loads((evidence/'build-source.json').read_text())['binary'];path=pathlib.Path(binary['path']);assert path.stat().st_size==binary['size'] and digest(path)==binary['sha256']
 result={'preflight':True,'ack_before_start_ns':events['start_begin']-events['attach_ack_before_start'],'linked_source_inputs':len(source['files']),'binary_sha256':binary['sha256'],'backend_observed':False}
 if a.backend:
  backend=a.backend.resolve();r=json.loads((backend/'report.json').read_text());assert r['passed'] and len(r['cases'])==4 and r['fixture_removed']
  assert r['owner_hash_before']==r['owner_hash_after'] and r['fixture_files']<=20 and r['fixture_bytes']<=4<<20
  assert r['binary_sha256']==binary['sha256']
  cases=[]
  for item in r['cases']:
   assert item['passed'] and item['removed'] and item['prefix_verified'] and item['start_intents']==item['creates']==1
   op=item['operation'];facts=item['facts'];retry=item['retry_facts'];log=item['job']['log']
   assert facts['ID']==item['container_id']==retry['ID'] and facts['Name']=='/'+op['operation_id']
   assert facts['Config']['Labels']==retry['Config']['Labels'] and all(facts['Config']['Labels'].get(k)==v for k,v in item['expected_labels'].items())
   assert facts['HostConfig']['LogConfig']['Type']=='none' and facts['LogPath']=='' and facts['HostConfig']['NetworkMode']=='none'
   assert facts['State']['StartedAt']==retry['State']['StartedAt']
   path=backend/(op['operation_id']+'.spool');data=path.read_bytes();assert len(data)==item['retained_bytes']==log['retained_bytes'] and digest(path)==item['retained_sha256']
   count,streams=frames(data,log['policy']['entry_bytes'],log['policy']['operation_bytes']);assert count==log['records']
   kept=len(streams[1])+len(streams[2]);assert kept==log['retained_payload']
   seen=log['stdout_seen']+log['stderr_seen'];assert log['stdout_seen']>=len(streams[1]) and log['stderr_seen']>=len(streams[2])
   if log['complete']:assert log['dropped_known'] and seen-kept==log['dropped_bytes']
   else:assert not log['dropped_known']
   kind=item['name']
   if not facts['State']['Running']:assert facts['State']['Pid']==0
   if kind in ('cancel','shutdown'):assert b'parent-out\n' in streams[1] and b'child-err\n' in streams[2]
   if kind=='binary':assert streams[1]==b'out\0' and streams[2]==b'err\xff' and log['complete'] and not log['truncated']
   elif kind=='flood':assert log['reason']=='output_limit' and log['termination_requested'] and log['termination_observed'] and not facts['State']['Running']
   elif kind=='cancel':assert item['job']['interrupted'] and not log['termination_requested'] and log['complete'] and not facts['State']['Running']
   elif kind=='shutdown':assert item['detached_still_running'] and facts['State']['Running'] and retry['State']['Running'] and log['reason']=='runner_shutdown' and not log['complete'] and not log['termination_requested']
   else:raise AssertionError(kind)
   cases.append({'case':kind,'records':count,'retained_bytes':len(data),'stdout_seen':log['stdout_seen'],'stderr_seen':log['stderr_seen'],'sha256':item['retained_sha256']})
  result.update(backend_observed=True,cases=cases,pid=r['pid'])
 a.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
if __name__=='__main__':main()

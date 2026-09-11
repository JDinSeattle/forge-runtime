#!/usr/bin/env python3
"""Exercise the operator GC on one synthetic aged orphan; verify all real refs."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
from urllib.parse import urlsplit, parse_qs, unquote
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--runner-config', required=True)
    parser.add_argument('--admin-binary', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    output = Path(args.output).absolute()
    output.mkdir(mode=0o700)
    platform = json.loads(Path(args.config).read_text())
    runner = json.loads(Path(args.runner_config).read_text())
    root = Path(platform['artifact_root'])
    if not root.is_absolute() or root.resolve() != root or runner['artifact_root'] != str(root):
        raise ValueError('both configured artifact roots must be the same canonical absolute path')
    u = urlsplit(os.environ['FORGE_DATABASE_URL'])
    if u.hostname != '127.0.0.1' or u.path != '/forge':
        raise ValueError('only the explicitly selected local /forge deployment is supported')
    pg = dict(os.environ, PGHOST=u.hostname, PGPORT=str(u.port or 5432), PGDATABASE='forge',
              PGUSER=unquote(u.username or ''), PGPASSWORD=unquote(u.password or ''),
              PGSSLMODE=parse_qs(u.query).get('sslmode',['disable'])[0])
    query = "SELECT COALESCE(json_agg(json_build_object('object_key',object_key,'sha256',sha256,'size',byte_size)),'[]'::json) FROM artifacts"
    raw = subprocess.check_output(['psql','-X','-At','-v','ON_ERROR_STOP=1','-c',query],env=pg,text=True)
    refs = {ref['object_key']:ref for ref in json.loads(raw)}
    ready_count = len(refs)
    with sqlite3.connect('file:'+runner['journal_path']+'?mode=ro',uri=True) as journal:
        if journal.execute('PRAGMA user_version').fetchone()[0] != 4:
            raise ValueError('upgrade/restart all publishers before this rehearsal')
        for row in journal.execute('SELECT receipt_json FROM operations UNION ALL SELECT ref_json FROM runner_artifacts'):
            ref = json.loads(row[0])
            if ref.get('object_key'):
                refs[ref['object_key']] = ref

    def verify_refs():
        for key,ref in refs.items():
            path = root/key
            if path.resolve() != path or not path.is_relative_to(root) or not path.is_file():
                raise ValueError('reference escaped root or is missing: '+key)
            h = hashlib.sha256()
            with path.open('rb') as stream:
                while chunk := stream.read(1<<20):
                    h.update(chunk)
            if path.stat().st_size != ref['size'] or h.hexdigest() != ref['sha256']:
                raise ValueError('authoritative bytes failed verification: '+key)

    verify_refs()
    tenant = 'gc_acceptance_'+uuid.uuid4().hex
    body = b'Synthetic unpublished orphan for the local GC acceptance rehearsal.\n'
    digest = hashlib.sha256(body).hexdigest()
    key = tenant+'/sentinel/'+digest
    # Cooperate with the normal publication protocol while creating the test
    # fixture. It intentionally commits no PostgreSQL or journal reference.
    with (root/'.publication.lock').open('r+') as lock:
        fcntl.flock(lock,fcntl.LOCK_SH)
        (root/tenant).mkdir(mode=0o700)
        (root/tenant/'sentinel').mkdir(mode=0o700)
        with (root/key).open('xb') as stream:
            stream.write(body); stream.flush()
            aged = time.time()-8*86400
            os.utime(stream.fileno(),(aged,aged))
            os.fsync(stream.fileno())
        for directory in [root/tenant/'sentinel',root/tenant,root]:
            fd = os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
    common = [str(Path(args.admin_binary).absolute()),'-config',str(Path(args.config).absolute()),
              '-runner-config',str(Path(args.runner_config).absolute()),'-age','168h','-limit','100']
    def collect(apply):
        command = common+(['-apply'] if apply else [])+['artifact-gc']
        result = json.loads(subprocess.check_output(command,text=True))
        (output/('apply.json' if apply else 'dry-run.json')).write_text(json.dumps(result,indent=2)+'\n')
        if [obj['object_key'] for obj in result['objects']] != [key]:
            raise ValueError('refusing to proceed: candidate set differs from the sole synthetic orphan')
        return result
    dry = collect(False)
    if not (root/key).is_file(): raise ValueError('dry run removed fixture')
    applied = collect(True)
    if (root/key).exists(): raise ValueError('applied GC retained selected orphan')
    verify_refs()
    report = {'at':datetime.datetime.now(datetime.timezone.utc).isoformat(), 'passed':True,
              'fixture':'one synthetic orphan, mtime deliberately aged 8 days; no published reference',
              'retention_hours':168,'ready_keys_verified':ready_count,
              'all_authoritative_keys_verified_before_and_after':len(refs),
              'dry_run_preserved_fixture':True,'only_synthetic_orphan_deleted':True,
              'orphan_key':key,'protected_at_apply':applied['protected'],
              'admin_binary_sha256':hashlib.sha256(Path(args.admin_binary).read_bytes()).hexdigest(),
              'harness_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__ == '__main__':
    main()

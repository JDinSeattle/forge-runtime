"""Strict, credential-free binding for one isolated DeepSeek evaluation batch."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit

FIELDS = {'version', 'purpose', 'authorization_id', 'output_dir', 'bundle_dir', 'candidate_sha256', 'registration_sha256', 'corpus_sha256', 'provider', 'model_id', 'total_budget_microusd', 'tenant', 'api_url', 'database', 'cli'}
TABLES = ('runs', 'model_attempts', 'quota_reservations', 'artifacts', 'run_events')


def exact(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError('deployment has missing or unknown fields')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def absolute(value):
    if not isinstance(value, str) or any(ord(c) < 32 for c in value):
        raise ValueError('deployment requires an absolute canonical path')
    p = Path(value)
    if not p.is_absolute() or str(p) != value or p.resolve() != p:
        raise ValueError('deployment path cannot contain aliases or symlinks')
    return p


def private_file(path):
    p = absolute(str(path))
    st = p.lstat()
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077 or st.st_nlink != 1 or st.st_size > 65536:
        raise ValueError('deployment requires a private owned regular file')
    return p


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate deployment field')
        result[key] = value
    return result


def sha(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('deployment requires a SHA256 digest')


def validate(value):
    exact(value, FIELDS)
    if type(value['version']) is not int or value['version'] != 1 or value['purpose'] != 'isolated-eval-v1' or value['authorization_id'] != 'deepseek-flash-2usd':
        raise ValueError('unsupported deployment authorization')
    if value['provider'] != 'deepseek' or value['model_id'] != 'deepseek-v4-flash' or type(value['total_budget_microusd']) is not int or value['total_budget_microusd'] != 2000000:
        raise ValueError('deployment must preserve the selected model and $2 ceiling')
    output, bundle = absolute(value['output_dir']), absolute(value['bundle_dir'])
    if output.name != 'eval-01' or output == bundle or bundle in output.parents or output in bundle.parents:
        raise ValueError('deployment requires a separate fixed eval-01 output')
    parent = output.parent.stat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise ValueError('fixed output parent must be private and owned')
    for field in ('candidate_sha256', 'registration_sha256', 'corpus_sha256'):
        sha(value[field])
    if not isinstance(value['tenant'], str) or not re.fullmatch('[A-Za-z0-9_-]{1,128}', value['tenant']):
        raise ValueError('deployment tenant is invalid')
    u = urlsplit(value['api_url'])
    if u.scheme != 'http' or u.hostname != '127.0.0.1' or not u.port or u.netloc != '127.0.0.1:' + str(u.port) or u.path or u.query or u.fragment:
        raise ValueError('deployment API must be one exact numeric loopback origin')
    database = value['database']
    exact(database, ('host', 'port', 'name', 'schema', 'schema_oid', 'audit_role'))
    if database['host'] != '127.0.0.1' or type(database['port']) is not int or database['port'] != 32773 or database['name'] != 'forge':
        raise ValueError('deployment requires the dedicated loopback database')
    if not isinstance(database['schema'], str) or not re.fullmatch('eval_ds_[0-9a-f]{8,40}', database['schema']):
        raise ValueError('deployment requires an exact private schema')
    if type(database['schema_oid']) is not int or not 0 < database['schema_oid'] <= 4294967295:
        raise ValueError('deployment schema OID is invalid')
    if not isinstance(database['audit_role'], str) or not re.fullmatch('[a-z_][a-z0-9_]{0,62}', database['audit_role']):
        raise ValueError('deployment audit role is invalid')
    exact(value['cli'], ('path', 'sha256'))
    binary = absolute(value['cli']['path'])
    sha(value['cli']['sha256'])
    st = binary.stat()
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o022 or not st.st_mode & stat.S_IXUSR or digest(binary) != value['cli']['sha256']:
        raise ValueError('pinned CLI identity changed')
    return value


def load(path, bundle, output, seal, registration):
    raw = private_file(path).read_bytes()
    value = validate(json.loads(raw, object_pairs_hook=pairs))
    if value['bundle_dir'] != str(absolute(str(bundle))) or value['output_dir'] != str(absolute(str(output))):
        raise ValueError('deployment fixes the exact bundle and single output directory')
    if value['candidate_sha256'] != digest(bundle / 'candidate.json') or value['registration_sha256'] != seal['registration_sha256'] or value['corpus_sha256'] != seal['corpus_sha256']:
        raise ValueError('deployment candidate/registration/corpus binding changed')
    if any(value[key] != registration[key] for key in ('provider', 'model_id', 'total_budget_microusd')) or set(registration['task_budgets_microusd'].values()) != {500000} or len(registration['task_budgets_microusd']) != 4:
        raise ValueError('deployment registration must keep all four fixed allocations')
    return {'descriptor': value, 'sha256': hashlib.sha256(raw).hexdigest()}


def check_scope(scope, binding):
    expected = binding['descriptor']['database']
    if not isinstance(scope, dict) or any(scope.get(key) != expected[field] for key, field in (('database', 'name'), ('schema', 'schema'), ('schema_oid', 'schema_oid'), ('current_user', 'audit_role'), ('session_user', 'audit_role'))):
        raise ValueError('audit database/schema/OID/role binding changed')
    if scope.get('read_only') is not True or scope.get('select_allowed') is not True or scope.get('dml_allowed') is not False or scope.get('unsafe_role') is not False:
        raise ValueError('audit role must be nonowner read-only with exact SELECT grants')


def subprocess_environment():
    # Never forward provider credentials, PG service overrides or shell startup
    # state into the CLI/audit subprocess. Secrets are added per role only.
    return {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'TMPDIR': '/tmp'}

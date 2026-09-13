#!/usr/bin/python3
"""Park only this project's four recorded pools (16 volumes), preserving data."""
from contextlib import ExitStack
import importlib.util
import os
from pathlib import Path
import sys
sys.dont_write_bytecode = True
HERE = Path(__file__).absolute().parent
sys.path.insert(0, str(HERE))
from common import MANIFEST, Tree, UnsafeState, image_fd, json_print, validate_manifest
spec = importlib.util.spec_from_file_location('forge_volume_mount', HERE / 'mount.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
ROOT = HERE.parent.parent
ROOTS = (ROOT, ROOT / 'var/lifecycle-rehearsals/lr20260912_a/pool-root',
         ROOT / 'var/recovery-rehearsals/r20260911_a/source',
         ROOT / 'var/recovery-rehearsals/r20260911_a/target')

def main():
    if len(sys.argv) != 1 or not sys.flags.isolated or os.geteuid() != 0 or Path('/proc/self/uid_map').read_text().split() != ['0', '0', '4294967295']:
        raise UnsafeState('run sudo /usr/bin/python3 -I <this fixed script>, without arguments')
    with ExitStack() as stack:
        pools = []
        # Check all fixed pools before the first unmount; no scanning/adoption.
        for root in ROOTS:
            tree = stack.enter_context(Tree(root))
            stack.enter_context(tree.locked())
            manifest = validate_manifest(tree.read_json(MANIFEST, owner=tree.uid), tree, require_ready=True)
            state = m.validated_state(tree, manifest)
            for slot in manifest['slots']:
                with image_fd(tree, slot):
                    loop, row = m.mounted_slot(tree, slot)
                if loop:
                    record = state['slots'].get(slot['id'])
                    if not record or record.get('loop') != loop['name'] or record.get('device') != loop['maj:min']:
                        raise UnsafeState('fixed pool has mismatched privileged ownership')
            pools.append((tree, manifest, state))
        for tree, manifest, state in pools:
            slots = [m.unmount_slot(tree, manifest, state, slot, preserve_contents=True) for slot in manifest['slots']]
            json_print({'pool': str(tree.storage_path), 'action': 'park', 'images_deleted': False, 'logical_leases_modified': False, 'slots': slots})
            sys.stdout.flush()
    print('All 16 project volumes parked; images, data and logical evidence preserved.')

if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, UnsafeState) as error:
        print('refused: ' + str(error), file=sys.stderr)
        sys.exit(1)

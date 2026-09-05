#!/usr/bin/env python3
"""Reap only process incarnations recorded under this harness's temporary roots."""
import pathlib,json,os,ctypes,struct,signal,time,subprocess,shutil
root=pathlib.Path(__file__).resolve().parents[2];lib=ctypes.CDLL('/usr/lib/libproc.dylib');observations=[];owned={}
for temporary in pathlib.Path(os.environ['TMPDIR']).glob('gajaeway-recordings-*'):
    for pattern in ['workspace/.gjc/state/sdk/*.json','agent/sdk/*.json','agent/sdk/sessions/index.jsonl']:
        for path in temporary.glob(pattern):
            try:
                values=[json.loads(line) for line in path.read_text().splitlines()] if path.suffix=='.jsonl' else [json.loads(path.read_text())]
                for value in values:
                    if isinstance(value,dict) and isinstance(value.get('pid'),int):owned[value['pid']]=(value.get('hostIncarnation') or value.get('processIncarnation'),str(temporary))
            except (ValueError,OSError):pass
for pid,(expected,temporary) in owned.items():
    info=ctypes.create_string_buffer(136);n=lib.proc_pidinfo(pid,3,0,info,136)
    current='darwin:%s:%s'%struct.unpack_from('<QQ',info.raw,120) if n==136 else None
    result={'pid':pid,'expectedIncarnation':expected,'observedIncarnation':current,'temporaryRoot':temporary}
    if current and expected and current==expected:
        os.kill(pid,signal.SIGTERM)
        for attempt in range(50):
            if lib.proc_pidinfo(pid,3,0,info,136)!=136:break
            time.sleep(.1)
        else:os.kill(pid,signal.SIGKILL)
        result['action']='terminated_matching_owned_incarnation'
    elif current:result['action']='not_signalled_incarnation_unproven_or_changed'
    else:result['action']='already_absent'
    observations.append(result)
# Explicitly recorded serve child PIDs must no longer exist as gjc serve processes.
b=json.loads((root/'artifacts/session-channel-economics-baseline.json').read_text())
for cohort in b['K'].values():
    for child in cohort['samples'][0]['children']:
        cmd=subprocess.run(['ps','-p',str(child['pid']),'-o','command='],capture_output=True,text=True).stdout.strip()
        assert 'sdk serve --stdio' not in cmd, f'recording serve survived: {child["pid"]}'
(root/'artifacts/pinned-runtime-cleanup-0.16.3.json').write_text(json.dumps({'ownedIncarnationChecks':observations,'recordedEconomicsServeChildrenAbsent':True},indent=2))
cache=root/'scripts/recordings/__pycache__'
if cache.exists():shutil.rmtree(cache)
print('Cleanup verified:',len(observations),'owned process identities; economics serve children absent')

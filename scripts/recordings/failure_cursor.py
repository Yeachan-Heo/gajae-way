#!/usr/bin/env python3
"""Observe terminal provider failure and CLI checkpoint exchange with protected input."""
from probe import *
try:
    print(str(TMP),flush=True)
    config=AGENT/'config.yml';config.write_text(config.read_text()+'\nretry:\n  enabled: false\n  maxRetries: 0\n  requestMaxRetries: 0\n')
    records.append({'sha256':hashlib.sha256(pathlib.Path(BIN).read_bytes()).hexdigest(),'temporaryRoot':str(TMP),'retry':'disabled in throwaway config for failed-turn probe'})
    run(['sdk','session','list','--scope','cwd'])
    created=parsed(run(['sdk','session','raw','global','--op','session.create','--idempotency-key','recordings-create','--json-input',json.dumps({'cwd':str(WORK)})]));sid=created['result']['sessionId']
    c=Channel(sid)
    c.request('control_request','model.profile.set',{'id':'gpt-default'})
    c.request('control_request','model.set',{'id':'layofflabs/gpt-image-2'})
    cli('send',sid,'--text','Reply FAIL_PROBE.','--op-ref','failed','--wait','--timeout-ms','180000',timeout=195)
    cli('status',sid,'failed')
    cp=c.request('query_request','session.checkpoint');token=cp.get('result',{}).get('checkpointToken')
    if token:
        payload=TMP/'checkpoint.json';payload.write_text(json.dumps({'checkpointToken':token}));payload.chmod(0o600)
        run(['sdk','session','raw','query',sid,'--query','session.checkpoint','--json-input-file',str(payload)])
    other=Channel(sid)
    foreign={'envelope':{'cursorVersion':0},'mac':'foreign'}
    other.request('query_request','session.checkpoint',{'checkpointToken':json.dumps(foreign)})
finally:
    for p in children:
        if p.poll() is None:
            p.terminate()
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
    cleanup()
    (ROOT/'artifacts/pinned-runtime-failure-cursor-0.16.3.json').write_text(json.dumps(clean(records),ensure_ascii=False,indent=2))
    print('DONE '+str(TMP),flush=True)

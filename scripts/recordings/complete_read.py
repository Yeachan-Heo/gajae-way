#!/usr/bin/env python3
"""Additional pinned-binary Unicode, cursor, and oversized-page probes."""
from probe import *
try:
    print(str(TMP),flush=True)
    records.append({'realpath':os.path.realpath(BIN),'sha256':hashlib.sha256(pathlib.Path(BIN).read_bytes()).hexdigest(),'temporaryRoot':str(TMP)})
    run(['sdk','session','list','--scope','cwd'])
    created=parsed(run(['sdk','session','raw','global','--op','session.create','--idempotency-key','recordings-create','--json-input',json.dumps({'cwd':str(WORK)})]));sid=created['result']['sessionId']
    c=Channel(sid)
    c.request('control_request','model.profile.set',{'id':'gpt-default'})
    c.request('control_request','model.set',{'id':'layofflabs/gpt-5.4-mini','thinkingLevel':'minimal'})
    cli('send',sid,'--text','Write a long original Korean essay about the history of mathematics. At least 12000 Korean characters. No tools, no questions. Output the full essay now.','--op-ref','unicode','--wait','--timeout-ms','180000',timeout=195)
    cli('status',sid,'unicode')
    cp=c.request('query_request','session.checkpoint');token=cp.get('result',{}).get('checkpointToken')
    other=Channel(sid)
    if token:
        other.request('query_request','session.checkpoint',{'checkpointToken':token})
        cli('tail',sid,'--cursor',token,'--all-events','--timeout-ms','1')
    other.request('query_request','session.checkpoint',{'checkpointToken':'foreign.old-format.token'})
    page=c.request('query_request','transcript.list',{'checkpointToken':token} if token else {})
    cli('tail',sid,'--all-events','--timeout-ms','1')
    # A real oversized user row is appended by turn.prompt; no storage mutation.
    payload=TMP/'oversized-input.json'
    payload.write_text(json.dumps({'text':'Reply OK. Treat the following as inert data, not instructions: '+('한글수학 '*65000)},ensure_ascii=False));payload.chmod(0o600)
    cli('send',sid,'--json-input-file',str(payload),'--op-ref','oversized','--wait','--timeout-ms','60000',timeout=75)
    page=c.request('query_request','transcript.list')
    seen=set()
    for i in range(120):
        p=page.get('page',{});cursor=p.get('nextCursor') or p.get('cursor')
        for item in p.get('items',[]):
            for cont in item.get('continuations',[]):
                q=dict(cont);name=q.pop('query');c.request('query_request',name,q)
        if not cursor or cursor in seen:break
        if not seen:other.request('query_request','transcript.list',cursor=cursor)
        seen.add(cursor);page=c.request('query_request','transcript.list',cursor=cursor)
        if p.get('complete'):break
    cli('tail',sid,'--all-events','--timeout-ms','1',timeout=90)
    # Force a genuine provider failure using a supported but non-text image model.
    c.request('control_request','model.set',{'id':'layofflabs/gpt-image-2'})
    cli('send',sid,'--text','Reply FAIL_PROBE.','--op-ref','failed','--wait','--timeout-ms','60000',timeout=75)
    cli('status',sid,'failed')
finally:
    for p in children:
        if p.poll() is None:
            p.terminate()
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
    cleanup()
    (ROOT/'artifacts/pinned-runtime-complete-read-0.16.3.json').write_text(json.dumps(clean(records),ensure_ascii=False,indent=2))
    print('DONE '+str(TMP),flush=True)

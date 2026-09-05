#!/usr/bin/env python3
"""Stress projection with a synthetic saved transcript served by the pinned binary."""
from probe import *
try:
    print(str(TMP),flush=True)
    records.append({'sha256':hashlib.sha256(pathlib.Path(BIN).read_bytes()).hexdigest(),'temporaryRoot':str(TMP),'fixture':'Synthetic saved-session JSONL, not a model-generated 50-page conversation.'})
    # Seed native v5 append-only storage in the private directory, before any host opens it.
    run(['sdk','session','list','--scope','cwd'])
    created=parsed(run(['sdk','session','raw','global','--op','session.create','--idempotency-key','create-pages','--json-input',json.dumps({'cwd':str(WORK)})]));sid=created['result']['sessionId']
    cli('send',sid,'--text','Hello','--op-ref','bootstrap')
    inspection=parsed(cli('inspect',sid));pid=inspection['result']['session']['pid']
    os.kill(pid,signal.SIGTERM)
    for attempt in range(100):
        status=subprocess.run(['ps','-p',str(pid),'-o','stat='],capture_output=True,text=True).stdout.strip()
        if not status or status.startswith('Z'):break
        time.sleep(.1)
    else:raise RuntimeError('Private session host did not terminate; refusing transcript mutation')
    file=next((AGENT/'sessions').rglob('*_'+sid+'.jsonl'))
    header=json.loads(file.read_text().splitlines()[0]);rows=[header];parent=None
    for i in range(520):
        rid=f'{i:08x}';text=('한'*100000) if i==130 else ('row_'+str(i)+' '+('x'*13000))
        rows.append({'type':'message','id':rid,'parentId':parent,'timestamp':'2026-09-05T00:00:00.000Z','message':{'role':'user','content':[{'type':'text','text':text}],'timestamp':1788566400000+i}});parent=rid
    file.write_text('\n'.join(json.dumps(x,ensure_ascii=False) for x in rows)+'\n')
    run(['sdk','session','list','--scope','cwd'])
    resumed=parsed(run(['sdk','session','raw','global','--op','session.resume','--idempotency-key','resume-pages','--json-input',json.dumps({'sessionId':sid,'sessionPath':str(file.resolve()),'cwd':str(WORK.resolve())})]))
    assert resumed.get('ok'), resumed
    c=Channel(sid);other=Channel(sid)
    cp=c.request('query_request','session.checkpoint');token=cp.get('result',{}).get('checkpointToken')
    page=c.request('query_request','transcript.list',{'checkpointToken':token} if token else {})
    pages=[];seen=set()
    for i in range(100):
        pages.append(page);p=page.get('page',{});cursor=p.get('continuationCursor')
        for item in p.get('items',[]):
            for cont in item.get('continuations',[]):
                q=dict(cont);name=q.pop('query');body=c.request('query_request',name,q)
                body_seen=set()
                while body.get('page',{}).get('continuationCursor'):
                    bc=body['page']['continuationCursor']
                    if bc in body_seen:break
                    body_seen.add(bc);body=c.request('query_request',name,q,cursor=bc)
        if not cursor or cursor in seen or p.get('complete'):break
        seen.add(cursor);page=c.request('query_request','transcript.list',cursor=cursor)
    records.append({'pageCount':len(pages),'pageKeys':[list(p.get('page',{})) for p in pages],'expectedRows':520});save()
    fresh=c.request('query_request','transcript.list')
    foreign_cursor=fresh.get('page',{}).get('continuationCursor')
    if foreign_cursor:other.request('query_request','transcript.list',cursor=foreign_cursor)
    cli('tail',sid,'--all-events','--timeout-ms','1',timeout=120)
finally:
    for p in children:
        if p.poll() is None:
            p.terminate()
            try:p.wait(timeout=5)
            except subprocess.TimeoutExpired:p.kill();p.wait()
    cleanup()
    (ROOT/'artifacts/pinned-runtime-pagination-0.16.3.json').write_text(json.dumps(clean(records),ensure_ascii=False,indent=2))
    print('DONE '+str(TMP),flush=True)

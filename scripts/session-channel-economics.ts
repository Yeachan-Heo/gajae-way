#!/usr/bin/env bun
/** Non-product, pinned-runtime I9c measurement. No live deployment state is used. */
import { resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const measurement = String.raw`
import os,json,pathlib,tempfile,shutil,subprocess,hashlib,time,threading,queue,uuid,ctypes,math
ROOT=pathlib.Path(os.environ['ECONOMICS_ROOT'])
baseline=json.loads((ROOT/'artifacts/session-channel-economics-baseline.json').read_text())
BIN=baseline['executable']['realpath']
digest=hashlib.sha256(pathlib.Path(BIN).read_bytes()).hexdigest()
if digest != baseline['executable']['sha256']:
    raise SystemExit('Executable SHA-256 mismatch; refusing incomparable economics measurement: '+digest)
TMP=pathlib.Path(tempfile.mkdtemp(prefix='gajaeway-channel-economics-'))
AGENT=TMP/'agent';WORK=TMP/'workspace';AGENT.mkdir();WORK.mkdir()
env=os.environ.copy();env['GAJAEWAY_HOME']=str(TMP);env['GJC_CODING_AGENT_DIR']=str(AGENT)
fixture=not bool(env.get('OPENAI_API_KEY'))
for name in ['models.yml','model-presets','config.yml']:
    src=pathlib.Path.home()/'.gjc/agent'/name
    if src.is_dir():shutil.copytree(src,AGENT/name)
    elif src.exists():shutil.copy2(src,AGENT/name)
if fixture:
    env['GAJAEWAY_FAKE_GJC_MODES']='serve:bidirectional'
command=['bun',str(ROOT/'packages/gateway/test/fixtures/fake-gjc.mjs')] if fixture else [BIN]
report={'executable':{'realpath':BIN,'sha256':digest,'temporaryRoot':str(TMP)},'procedure':{'idleSeconds':600,'sampleIntervalSeconds':1,'turns':10,'cohorts':'sequential isolated K=1,2,4','readerChildren':'bidirectional serve --stdio children attached to one session; prompts owned by channel','sampling':'libproc proc_pidinfo TASKINFO=4 and LISTFDS=1, identical to I0a','deviation':'fixture child; provider turns unavailable' if fixture else None},'baselineRef':'artifacts/session-channel-economics-baseline.json','K':{},'ratios':{},'pass':False,'rules':{'perChildRssP95RatioMax':1.25,'aggregateRssP95RatioMax':1.25,'fdsPerChildDeltaMax':4,'threadsDeltaMax':2,'coldStartP95DeltaMsMax':1000,'residentChannelsMax':'<= maxTailProcesses (K)','turnsOk':10}}
target=ROOT/'artifacts/session-channel-economics.json'
def save():target.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
def cli(args):
    p=subprocess.run(command+args,cwd=WORK,env=env,capture_output=True,text=True,timeout=75)
    if p.returncode:raise RuntimeError('CLI setup failed (exit '+str(p.returncode)+')')
    return json.loads(p.stdout)
def quantile(values):
    v=sorted(values);return v[max(0,math.ceil(len(v)*.95)-1)] if v else None
lib=ctypes.CDLL('/usr/lib/libproc.dylib')
class TaskInfo(ctypes.Structure):
    _fields_=[(x,ctypes.c_uint64) for x in ['virtual','rss','utime','stime','threads_utime','threads_stime']]+[(x,ctypes.c_int32) for x in ['policy','faults','pageins','cow','sent','received','sysmach','sysunix','switches','threads','running','priority']]
def sample(c):
    info=TaskInfo();n=lib.proc_pidinfo(c.p.pid,4,0,ctypes.byref(info),ctypes.sizeof(info));fdbytes=lib.proc_pidinfo(c.p.pid,1,0,None,0)
    buf=ctypes.create_string_buffer(max(4096,fdbytes+4096));fdbytes=lib.proc_pidinfo(c.p.pid,1,0,buf,len(buf))
    return {'pid':c.p.pid,'rssBytes':info.rss if n else None,'threads':info.threads if n else None,'fds':fdbytes//8 if fdbytes>0 else None,'alive':c.p.poll() is None}
children=[]
class Channel:
    def __init__(self,sid):
        start=time.monotonic();self.q=queue.Queue();self.p=subprocess.Popen(command+['sdk','serve','--stdio','--session',sid],cwd=WORK,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True,bufsize=1);children.append(self)
        def read():
            for line in self.p.stdout:
                try:self.q.put(json.loads(line))
                except ValueError:pass
        threading.Thread(target=read,daemon=True).start()
        if fixture:
            self.p.stdin.write(json.dumps({'type':'query_request','id':'first-frame','query':'session.checkpoint','input':{}})+'\n');self.p.stdin.flush()
        self.first=self.q.get(timeout=30);self.cold=(time.monotonic()-start)*1000
    def request(self,kind,name,input):
        rid=str(uuid.uuid4());frame={'type':kind,'id':rid,'input':input}
        if kind=='control_request':frame.update(op=name,operation=name)
        else:frame['query']=name
        self.p.stdin.write(json.dumps(frame)+'\n');self.p.stdin.flush();end=time.monotonic()+30
        while time.monotonic()<end:
            try:f=self.q.get(timeout=max(.01,end-time.monotonic()))
            except queue.Empty:break
            if f.get('id')==rid or f.get('requestId')==rid:return f
        return {'ok':False,'error':'timeout'}
    def close(self):
        if self.p.poll() is None:
            self.p.terminate()
            try:self.p.wait(timeout=5)
            except subprocess.TimeoutExpired:self.p.kill();self.p.wait()
def cleanup():
    for c in children:c.close()
    # Only signal private descendants whose command includes this unique agent directory.
    table=subprocess.check_output(['ps','-axo','pid=,command='],text=True)
    for line in table.splitlines():
        if str(AGENT) in line and ('broker-internal' in line or 'session-host-internal' in line):
            pid=int(line.strip().split()[0])
            try:os.kill(pid,15)
            except ProcessLookupError:pass
save()
try:
    cli(['sdk','session','list','--scope','cwd'])
    created=cli(['sdk','session','raw','global','--op','session.create','--idempotency-key',str(uuid.uuid4()),'--json-input',json.dumps({'cwd':str(WORK)})]);sid=created['result']['sessionId']
    for k in [1,2,4]:
        channels=[Channel(sid) for _ in range(k)];samples=[];stop=threading.Event();phase=['idle'];errors=[]
        def sampling():
            start=time.monotonic();tick=0
            while not stop.is_set():
                rows=[sample(c) for c in channels];samples.append({'elapsedSeconds':time.monotonic()-start,'phase':phase[0],'children':rows,'residentChannels':sum(c['alive'] for c in rows)})
                tick+=1;stop.wait(max(0,start+tick-time.monotonic()))
        t=threading.Thread(target=sampling);t.start();print('idle K='+str(k)+' root='+str(TMP),flush=True)
        stop.wait(600);phase[0]='turns';turnsOk=0
        channels[0].request('control_request','model.profile.set',{'id':'gpt-default'})
        channels[0].request('control_request','model.set',{'id':'layofflabs/gpt-5.4-mini','thinkingLevel':'minimal'})
        try:
            for i in range(10):
                c=channels[i%k];ref='economics-'+str(uuid.uuid4())
                receipt=c.request('control_request','turn.prompt',{'text':'Reply exactly CHANNEL_'+str(k)+'_'+str(i)+'.','clientRef':ref})
                # Receipt ambiguity is reconciled only by clientRef; never re-prompt.
                end=time.monotonic()+75;result={}
                while time.monotonic()<end:
                    result=c.request('query_request','turn.result',{'kind':'prompt','clientRef':ref});r=result.get('result',{})
                    if r.get('status')=='terminal_ok':turnsOk+=1;break
                    if r.get('status') in ['failed','cancelled','aborted','terminal_error']:break
                    time.sleep(.25)
                if result.get('result',{}).get('status')!='terminal_ok':errors.append({'turn':i,'status':result.get('result',{}).get('status'),'receiptOk':receipt.get('ok')})
        finally:stop.set();t.join()
        valid=all(all(c[m] is not None for m in ['rssBytes','fds','threads']) and c['alive'] for s in samples for c in s['children'])
        rss=[quantile([s['children'][i]['rssBytes'] or 0 for s in samples]) for i in range(k)]
        fds=[quantile([s['children'][i]['fds'] or 0 for s in samples]) for i in range(k)]
        threads=[quantile([s['children'][i]['threads'] or 0 for s in samples]) for i in range(k)]
        row={'samples':samples,'perChildRssP95':rss,'aggregateRssP95':quantile([sum(c['rssBytes'] or 0 for c in s['children']) for s in samples]),'fdsPerChild':fds,'threads':threads,'coldStartP95Ms':quantile([c.cold for c in channels]),'turnsOk':turnsOk,'turnErrors':errors,'residentChannelsMax':max(s['residentChannels'] for s in samples),'maxTailProcesses':k,'validSamples':valid}
        row['coldStartMs']=[c.cold for c in channels]
        row['firstFrames']=[c.first for c in channels]
        row['coldStartP50Ms']=sorted(row['coldStartMs'])[max(0,math.ceil(k*.5)-1)]
        b=baseline['K'][str(k)];ratios={'perChildRssP95':max(rss[i]/b['perChildP95'][i]['rssBytes'] for i in range(k)),'aggregateRssP95':row['aggregateRssP95']/b['aggregateP95']['rssBytes'],'fdsPerChildDelta':max(fds[i]-b['perChildP95'][i]['fds'] for i in range(k)),'threadsDelta':max(threads[i]-b['perChildP95'][i]['threads'] for i in range(k)),'coldStartP95DeltaMs':row['coldStartP95Ms']-b['coldStartP95Ms'],'residentChannelsRatio':row['residentChannelsMax']/k}
        ratios['pass']=valid and turnsOk==10 and ratios['perChildRssP95']<=1.25 and ratios['aggregateRssP95']<=1.25 and ratios['fdsPerChildDelta']<=4 and ratios['threadsDelta']<=2 and ratios['coldStartP95DeltaMs']<=1000 and ratios['residentChannelsRatio']<=1
        report['K'][str(k)]=row;report['ratios'][str(k)]=ratios;save();print(json.dumps({'K':k,'ratios':ratios,'turnsOk':turnsOk}),flush=True)
        for c in channels:c.close()
    report['pass']=all(r['pass'] for r in report['ratios'].values()) and len(report['K'])==3
except Exception as e:
    report['error']=type(e).__name__+': '+str(e);raise
finally:
    cleanup();save()
`;
const child = Bun.spawn(["python3", "-u", "-c", measurement], {
	cwd: root,
	env: { ...process.env, ECONOMICS_ROOT: root },
	stdout: "inherit",
	stderr: "inherit",
});
process.exit(await child.exited);

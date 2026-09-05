#!/usr/bin/env python3
"""Real pinned SDK probes; run with --idle-seconds 600 for the full baseline."""
import pinned_runtime as h
import argparse, threading, queue, uuid, ctypes, math
from pinned_runtime import *
parser=argparse.ArgumentParser();parser.add_argument('--idle-seconds',type=int,default=600);args=parser.parse_args()
children=[]
class Channel:
    def __init__(self,sid):
        start=time.monotonic();self.frames=[];self.q=queue.Queue()
        self.p=subprocess.Popen([BIN,'sdk','serve','--stdio','--session',sid],cwd=WORK,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
        children.append(self.p)
        def reader():
            for line in self.p.stdout:
                try:frame=json.loads(line)
                except:frame={'raw':line}
                self.frames.append(frame);self.q.put(frame)
        threading.Thread(target=reader,daemon=True).start()
        try:self.first=self.q.get(timeout=20);self.cold=(time.monotonic()-start)*1000
        except queue.Empty:self.first={'timeout':'first frame'};self.cold=None
    def request(self,typ,name,input=None,cursor=None,timeout=20):
        rid=str(uuid.uuid4()); frame={'type':typ,'id':rid,'input':input or {}}
        frame['query' if typ=='query_request' else 'operation']=name
        if cursor:frame['cursor']=cursor
        self.p.stdin.write(json.dumps(frame,ensure_ascii=False)+'\n');self.p.stdin.flush()
        end=time.monotonic()+timeout;observed=[]
        while time.monotonic()<end:
            try:f=self.q.get(timeout=max(.01,end-time.monotonic()))
            except queue.Empty:break
            observed.append(f)
            if f.get('id')==rid or f.get('requestId')==rid:
                records.append({'channelRequest':frame,'channelResponse':f});save();return f
        out={'timeout':timeout,'observed':observed};records.append({'channelRequest':frame,'channelResponse':out});save();return out
    def close(self):
        self.p.terminate()
        try:self.p.wait(timeout=5)
        except subprocess.TimeoutExpired:self.p.kill();self.p.wait()
def cli(verb,sid,*extra,timeout=60):return run(['sdk','session',verb,sid,*extra],timeout)
def query(sid,name,input=None,cursor=None):
    a=['sdk','session','raw','query',sid,'--query',name,'--json-input',json.dumps(input or {})]
    if cursor:a+=['--cursor',cursor]
    return parsed(run(a))
def quantile(values,p):
    v=sorted(x for x in values if x is not None);return v[max(0,math.ceil(len(v)*p)-1)] if v else None
lib=ctypes.CDLL('/usr/lib/libproc.dylib')
class TaskInfo(ctypes.Structure):
    _fields_=[(x,ctypes.c_uint64) for x in ['virtual','rss','utime','stime','threads_utime','threads_stime']]+[(x,ctypes.c_int32) for x in ['policy','faults','pageins','cow','sent','received','sysmach','sysunix','switches','threads','running','priority']]
def sample(p):
    info=TaskInfo();n=lib.proc_pidinfo(p.pid,4,0,ctypes.byref(info),ctypes.sizeof(info));fdbytes=lib.proc_pidinfo(p.pid,1,0,None,0)
    fd_buffer=ctypes.create_string_buffer(max(4096,fdbytes+4096))
    fdbytes=lib.proc_pidinfo(p.pid,1,0,fd_buffer,len(fd_buffer))
    return {'pid':p.pid,'rssBytes':info.rss if n else None,'threads':info.threads if n else None,'fds':fdbytes//8 if fdbytes>0 else None,'alive':p.poll() is None}
def economics(sid):
    baseline={'executable':records[0],'procedure':{'idleSeconds':args.idle_seconds,'sampleIntervalSeconds':1,'turns':10,'cohorts':'sequential isolated K=1,2,4','readerChildren':'read-only serve --stdio attached to one session; prompts use CLI','deviation':'Parent authorized 300-second idle instead of planned 600 seconds.' if args.idle_seconds!=600 else None},'K':{}}
    target=ROOT/'artifacts/session-channel-economics-baseline.json'
    for k in [1,2,4]:
        channels=[Channel(sid) for _ in range(k)];samples=[];stop=threading.Event();phase=['idle']
        def sampling():
            start=time.monotonic();tick=0
            while not stop.is_set():
                samples.append({'elapsedSeconds':time.monotonic()-start,'phase':phase[0],'children':[sample(c.p) for c in channels]});tick+=1
                stop.wait(max(0,start+tick-time.monotonic()))
        thread=threading.Thread(target=sampling);thread.start()
        print('economics idle K='+str(k),flush=True)
        stop.wait(args.idle_seconds);phase[0]='turns';turns=[]
        for i in range(10):turns.append(cli('send',sid,'--text',f'Reply exactly BASELINE_{k}_{i}.','--wait','--timeout-ms','60000',timeout=75))
        stop.set();thread.join()
        metrics=['rssBytes','fds','threads']
        row={'samples':samples,'sampleCount':len(samples),'coldStartMs':[c.cold for c in channels],'firstFrames':[c.first for c in channels],'coldStartP50Ms':quantile([c.cold for c in channels],.5),'coldStartP95Ms':quantile([c.cold for c in channels],.95),'perChildP95':[{m:quantile([s['children'][i][m] for s in samples],.95) for m in metrics} for i in range(k)],'aggregateP95':{m:quantile([sum(c[m] for c in s['children']) for s in samples if all(c[m] is not None for c in s['children'])],.95) for m in metrics},'turns':turns}
        baseline['K'][str(k)]=clean(row);target.write_text(json.dumps(baseline,ensure_ascii=False,indent=2))
        for c in channels:c.close()
    return baseline
def main():
    try:
        print(str(TMP),flush=True)
        records.append({'realpath':os.path.realpath(BIN),'sha256':hashlib.sha256(pathlib.Path(BIN).read_bytes()).hexdigest(),'temporaryRoot':str(TMP)})
        run(['--version']);run(['sdk','session','list','--scope','cwd'])
        created=parsed(run(['sdk','session','raw','global','--op','session.create','--idempotency-key','recordings-create','--json-input',json.dumps({'cwd':str(WORK)})]));sid=created['result']['sessionId']
        cli('inspect',sid);run(['sdk','session','list','--scope','cwd'])
        channel=Channel(sid);records.append({'serveFirstFrame':channel.first});save()
        channel.request('control_request','model.profile.set',{'id':'gpt-default'})
        channel.request('control_request','model.set',{'id':'layofflabs/gpt-5.4-mini','thinkingLevel':'minimal'})
        ref='recording-'+str(uuid.uuid4())
        channel.request('control_request','turn.prompt',{'text':'Reply exactly CHANNEL_OK.','clientRef':ref})
        channel.request('query_request','turn.result',{'kind':'prompt','clientRef':ref})
        channel.request('query_request','session.checkpoint')
        channel.request('query_request','transcript.list')
        channel.request('control_request','turn.steer',{'text':'Reply briefly.','clientRef':'recording-steer'})
        channel.request('control_request','model.profile.set',{'id':'gpt-default'})
        channel.request('control_request','service_tier.set',{'tier':'default'})
        channel.request('query_request','queue.messages.list')
        channel.close()
        economics(sid)
    finally:
        for p in children:
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=5)
                except subprocess.TimeoutExpired:p.kill();p.wait()
        cleanup()
        (ROOT/'artifacts/pinned-runtime-raw-0.16.3.json').write_text(json.dumps(clean(records),ensure_ascii=False,indent=2))
        print('DONE '+str(TMP),flush=True)
if __name__=='__main__':main()

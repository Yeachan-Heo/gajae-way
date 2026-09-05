#!/usr/bin/env python3
"""Pinned-runtime recording harness. Secrets are loaded in memory, never logged."""
import os, sys, json, pathlib, tempfile, shutil, subprocess, hashlib, plistlib, time, signal
BIN='/Users/bellman/gajaeway-play/bin/gjc'
ROOT=pathlib.Path(__file__).resolve().parents[2]
TMP=pathlib.Path(tempfile.mkdtemp(prefix='gajaeway-recordings-'))
AGENT=TMP/'agent'; WORK=TMP/'workspace'; AGENT.mkdir(); WORK.mkdir()
env=os.environ.copy(); env['GJC_CODING_AGENT_DIR']=str(AGENT)
plist=pathlib.Path.home()/'Library/LaunchAgents/dev.gajaeway.gateway.plist'
creds=plistlib.loads(plist.read_bytes()).get('EnvironmentVariables',{})
for key in ['OPENAI_API_KEY','OPENAI_BASE_URL']:
    if key in creds: env[key]=creds[key]
for name in ['models.yml','model-presets','config.yml']:
    src=pathlib.Path.home()/'.gjc/agent'/name
    if src.is_dir(): shutil.copytree(src,AGENT/name)
    elif src.exists(): shutil.copy2(src,AGENT/name); (AGENT/name).chmod(0o600)
config=AGENT/'config.yml'
config.write_text('\n'.join(x for x in config.read_text().splitlines() if not x.startswith(('steeringMode:','interruptMode:')))+'\nsteeringMode: all\ninterruptMode: wait\n')
records=[]
def clean(value):
    text=json.dumps(value,ensure_ascii=False)
    for key,value in creds.items():
        if ('KEY' in key or 'TOKEN' in key or 'SECRET' in key) and isinstance(value,str) and value:
            text=text.replace(value,'[redacted:sha256:'+hashlib.sha256(value.encode()).hexdigest()[:12]+']')
    return json.loads(text)
def run(args,timeout=60):
    start=time.monotonic()
    try:
        p=subprocess.run([BIN,*args],cwd=WORK,env=env,capture_output=True,text=True,timeout=timeout)
        out={'argv':args,'exitCode':p.returncode,'stdout':p.stdout,'stderr':p.stderr,'elapsedMs':round((time.monotonic()-start)*1000)}
    except subprocess.TimeoutExpired as e:
        out={'argv':args,'timeout':timeout,'stdout':(e.stdout or b'').decode(),'stderr':(e.stderr or b'').decode()}
    records.append(clean(out)); save(); return out
def save():
    (TMP/'records.json').write_text(json.dumps(records,ensure_ascii=False,indent=2))
def parsed(out):
    try:return json.loads(out['stdout'])
    except:return {}
def cleanup():
    targets=[]
    for p in list(AGENT.rglob('*.json'))+list((WORK/'.gjc/state/sdk').glob('*.json')):
        try:
            data=json.loads(p.read_text())
            if isinstance(data,dict) and isinstance(data.get('pid'),int):targets.append(data['pid'])
        except:pass
    table=subprocess.check_output(['ps','-axo','pid=,command='],text=True)
    for line in table.splitlines():
        if str(AGENT) in line and ('broker-internal' in line or 'session-host-internal' in line):
            targets.append(int(line.strip().split()[0]))
    for pid in set(targets):
        if pid==os.getpid():continue
        command=subprocess.run(['ps','-p',str(pid),'-o','command='],capture_output=True,text=True).stdout.strip()
        if not command.startswith(BIN+' sdk '):continue
        try:os.kill(pid,signal.SIGTERM)
        except ProcessLookupError:pass
    survivors=[]
    for pid in set(targets):
        for attempt in range(50):
            command=subprocess.run(['ps','-p',str(pid),'-o','command='],capture_output=True,text=True).stdout.strip()
            if not command.startswith(BIN+' sdk '):break
            time.sleep(.1)
        else:
            try:os.kill(pid,signal.SIGKILL)
            except ProcessLookupError:pass
            survivors.append(pid)
    records.append({'cleanupSignalledPids':sorted(set(targets)),'cleanupRequiredKill':survivors});save()
if __name__=='__main__':
    print(str(TMP),flush=True)
    try:
        records.append({'realpath':os.path.realpath(BIN),'sha256':hashlib.sha256(pathlib.Path(BIN).read_bytes()).hexdigest(),'temporaryRoot':str(TMP)})
        run(['--version'])
        run(['sdk','session','list','--scope','cwd'])
        created=run(['sdk','session','raw','global','--op','session.create','--idempotency-key','recordings-create','--json-input',json.dumps({'cwd':str(WORK)})])
        print(json.dumps(parsed(created)),flush=True)
    finally:cleanup()

# Copyright (C) 2026 The Syncthing Authors.
# SPDX-License-Identifier: MPL-2.0
# Run only against disposable benchmark directories; production peers are never configured.
import os, sys, time, json, hashlib, subprocess, socket, shutil, urllib.request, xml.etree.ElementTree as ET, statistics
from pathlib import Path
ROOT=None; EXE=None
N=8192; SIZE=16384
ENV=os.environ.copy(); ENV.update(STNOUPGRADE='1', STNORESTART='1', STNOBROWSER='1', STNOTRAY='1')
results=[]
def emit(x): print(json.dumps(x),flush=True)
def port():
    with socket.socket() as s: s.bind(('127.0.0.1',0)); return s.getsockname()[1]
def api(p,k,path):
    req=urllib.request.Request(f'http://127.0.0.1:{p}/rest/{path}',headers={'X-API-Key':k})
    with urllib.request.urlopen(req,timeout=5) as r: return json.load(r)
def settext(e,k,v):
    x=e.find(k)
    if x is None: x=ET.SubElement(e,k)
    x.text=str(v)
def setup(home,src,ownport,gui,peerid,peerport):
    subprocess.run([str(EXE),'generate','--home',str(home),'--no-port-probing'],env=ENV,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT,check=True)
    tree=ET.parse(home/'config.xml'); c=tree.getroot(); own=c.find('device').get('id')
    for f in c.findall('folder'): c.remove(f)
    if peerid:
        d=ET.SubElement(c,'device',{'id':peerid,'name':'isolated-benchmark-peer','compression':'metadata'})
        ET.SubElement(d,'address').text=f'tcp://127.0.0.1:{peerport}'
    f=ET.SubElement(c,'folder',{'id':'benchmark','label':'Isolated benchmark','path':str(src),'type':'sendreceive','rescanIntervalS':'0','fsWatcherEnabled':'false','fsWatcherDelayS':'1'})
    ET.SubElement(f,'device',{'id':own})
    if peerid: ET.SubElement(f,'device',{'id':peerid})
    for k,v in [('copiers','0'),('maxConcurrentWrites','16'),('disableFsync','false'),('experimentalBatchedTransfers','false')]: settext(f,k,v)
    g=c.find('gui'); g.set('enabled','true'); g.set('tls','false'); settext(g,'address',f'127.0.0.1:{gui}'); key=g.findtext('apikey')
    o=c.find('options')
    for x in o.findall('listenAddress'): o.remove(x)
    ET.SubElement(o,'listenAddress').text=f'tcp://127.0.0.1:{ownport}'
    for k,v in [('globalAnnounceEnabled','false'),('localAnnounceEnabled','false'),('relaysEnabled','false'),('natEnabled','false'),('startBrowser','false'),('autoUpgradeIntervalH','0'),('urAccepted','-1')]: settext(o,k,v)
    tree.write(home/'config.xml',encoding='utf-8',xml_declaration=True)
    return own,key

def addpeer(home,peerid,peerport):
    t=ET.parse(home/'config.xml'); c=t.getroot()
    d=ET.SubElement(c,'device',{'id':peerid,'name':'isolated-benchmark-peer','compression':'metadata'}); ET.SubElement(d,'address').text=f'tcp://127.0.0.1:{peerport}'
    ET.SubElement(c.find('folder'),'device',{'id':peerid}); t.write(home/'config.xml',encoding='utf-8',xml_declaration=True)

def transfer(src,dst,run,expected_files,expected_bytes):
    global last_bundle_requests
    homea=run/'sender'; homeb=run/'receiver'; ports=[port() for _ in range(4)]
    ida,ka=setup(homea,src,ports[0],ports[2],None,None)
    idb,kb=setup(homeb,dst,ports[1],ports[3],ida,ports[0]); addpeer(homea,idb,ports[1])
    procs=[]; logs=[]; start=time.perf_counter(); last=0
    try:
        for home in (homea,homeb):
            log=open(home/'console.log','wb'); logs.append(log)
            procs.append(subprocess.Popen([str(EXE),'serve','--home',str(home),'--no-browser','--no-tray','--no-restart','--no-upgrade','--log-file=-'],env=ENV,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW))
        while time.perf_counter()-start<600:
            if any(p.poll() is not None for p in procs): raise RuntimeError('Benchmark instance exited; inspect console.log')
            try:
                s=api(ports[3],kb,'db/status?folder=benchmark')
                if s['localFiles']==expected_files and s['localBytes']==expected_bytes and s['needTotalItems']==0:
                    # Completion is observed after receiver database commit.
                    stats=api(ports[3],kb,'system/connections')
                    last_bundle_requests=sum(c.get('batchRequestsSent',0) for c in stats['connections'].values())
                    emit({'bundle_requests':last_bundle_requests})
                    return time.perf_counter()-start
                if time.perf_counter()-last>20:
                    emit({'progress':run.name,'elapsed_s':round(time.perf_counter()-start,1),'local_files':s['localFiles'],'need_files':s['needFiles']}); last=time.perf_counter()
            except (OSError,ValueError): pass
            time.sleep(.1)
        raise TimeoutError(str(run))
    finally:
        for gui,key in ((ports[2],ka),(ports[3],kb)):
            try:
                req=urllib.request.Request(f'http://127.0.0.1:{gui}/rest/system/shutdown',headers={'X-API-Key':key},method='POST')
                urllib.request.urlopen(req,timeout=5).close()
            except OSError: pass
        for p in procs:
            try: p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                # Windows builds can have a monitor child; kill only the tree
                # rooted at the Popen process we created for this benchmark.
                if os.name == 'nt':
                    subprocess.run(['taskkill','/PID',str(p.pid),'/T','/F'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                else:
                    p.kill()
                p.wait()
        for log in logs: log.close()

def verify(dst,manifest):
    files=[p for p in dst.rglob('*') if p.is_file() and '.stfolder' not in p.parts]
    assert len(files)==N,(len(files),N)
    for name,(digest,stamp) in manifest.items():
        p=dst/name
        assert hashlib.sha256(p.read_bytes()).hexdigest()==digest,name
        assert abs(p.stat().st_mtime_ns-stamp)<=1000,(name,'mtime')


import argparse
parser=argparse.ArgumentParser(description='Compare durable ordinary sync and explicit whole-file bundles over loopback.')
parser.add_argument('--binary',required=True,type=Path)
parser.add_argument('--work',required=True,type=Path)
parser.add_argument('--files',type=int,default=8192)
parser.add_argument('--trials',type=int,default=3)
args=parser.parse_args()
if args.files < 1 or args.trials < 1: parser.error('--files and --trials must be positive')
ROOT=args.work.resolve(); EXE=args.binary.resolve(); N=args.files
if ROOT.exists(): raise SystemExit('--work must be a new directory')
ROOT.mkdir(parents=True)
fixture=ROOT/'fixture'; fixture.mkdir(); manifest={}
for i in range(N):
    p=fixture/f'dir-{i//64:03d}'/f'file-{i:05d}.bin'; p.parent.mkdir(exist_ok=True)
    data=os.urandom(SIZE); p.write_bytes(data); os.utime(p,ns=(1700000000000000000,1700000000000000000))
    manifest[p.relative_to(fixture).as_posix()]=(hashlib.sha256(data).hexdigest(),p.stat().st_mtime_ns)
base_setup=setup
mode=False
def setup(*args):
    identity,key=base_setup(*args)
    tree=ET.parse(args[0]/'config.xml'); c=tree.getroot()
    c.find('folder/experimentalBatchedTransfers').text=str(mode).lower()
    tree.write(args[0]/'config.xml',encoding='utf-8',xml_declaration=True)
    return identity,key

rows=[]
for trial in range(1,args.trials+1):
    order=[False,True] if trial%2 else [True,False]
    for mode in order:
        run=ROOT/f"{trial}-{'bundle' if mode else 'ordinary'}"
        run.mkdir(); dst=run/'received'; dst.mkdir()
        elapsed=transfer(fixture,dst,run,N,N*SIZE); verify(dst,manifest)
        row=dict(trial=trial,mode='bundle' if mode else 'ordinary',seconds=elapsed,verified=N,bundle_requests=last_bundle_requests)
        rows.append(row); (ROOT/'results.json').write_text(json.dumps(rows,indent=2)); emit(row)
emit({mode:statistics.median(r['seconds'] for r in rows if r['mode']==mode) for mode in ['ordinary','bundle']})

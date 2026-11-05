# Copyright (C) 2026 The Syncthing Authors.
# SPDX-License-Identifier: MPL-2.0
# Disposable generated fixtures only; never points at production peers/folders.
import argparse,base64,hashlib,json,os,signal,subprocess,time,urllib.request,urllib.parse
from pathlib import Path
import benchmark_bundles as bench
parser=argparse.ArgumentParser(description='Compare isolated initial, unchanged and modified/deleted-file scans against an older build.')
parser.add_argument('--binary',required=True,type=Path)
parser.add_argument('--baseline-binary',required=True,type=Path)
parser.add_argument('--work',required=True,type=Path)
parser.add_argument('--files',type=int,default=4096)
parser.add_argument('--trials',type=int,default=2)
parser.add_argument('--profile',action='store_true',help='Write a CPU profile in each disposable peer directory')
args=parser.parse_args()
if args.files<128 or args.trials<1:parser.error('--files must be at least 128 and --trials positive')
root=args.work.resolve()
if root.exists():parser.error('--work must be a new directory')
root.mkdir(parents=True)
N=args.files
rows=[]
def req(port,key,path,method='GET'):
 r=urllib.request.Request(f'http://127.0.0.1:{port}/rest/'+path,headers={'X-API-Key':key},method=method)
 with urllib.request.urlopen(r,timeout=120) as response:
  data=response.read();return json.loads(data) if data else None
def process_usage(process):
 # Sample only the disposable peer. STMONITORED below keeps the service in
 # this child so these counters cannot accidentally measure just its monitor.
 if os.name!='nt':return {}
 import ctypes
 from ctypes import wintypes
 class Memory(ctypes.Structure):
  _fields_=[('cb',wintypes.DWORD),('faults',wintypes.DWORD)]+[(name,ctypes.c_size_t) for name in
   ('peak_ws','ws','peak_paged','paged','peak_nonpaged','nonpaged','pagefile','peak_pagefile')]
 kernel=ctypes.WinDLL('kernel32',use_last_error=True)
 kernel.GetProcessTimes.argtypes=[wintypes.HANDLE]+[ctypes.POINTER(wintypes.FILETIME)]*4
 kernel.K32GetProcessMemoryInfo.argtypes=[wintypes.HANDLE,ctypes.POINTER(Memory),wintypes.DWORD]
 times=[wintypes.FILETIME() for _ in range(4)];memory=Memory();memory.cb=ctypes.sizeof(memory)
 if not kernel.GetProcessTimes(int(process._handle),*[ctypes.byref(value) for value in times]):raise ctypes.WinError(ctypes.get_last_error())
 if not kernel.K32GetProcessMemoryInfo(int(process._handle),ctypes.byref(memory),memory.cb):raise ctypes.WinError(ctypes.get_last_error())
 cpu=sum((value.dwHighDateTime<<32)+value.dwLowDateTime for value in times[2:])/10000000
 return {'cpu_s':cpu,'peak_working_set_bytes':memory.peak_ws}

cases=[(trial,mode) for trial in range(1,args.trials+1) for mode in (['before','after'] if trial%2 else ['after','before'])]
for trial,mode in cases:
 run=root/f'{trial}-{mode}';run.mkdir();fixture=run/'fixture';fixture.mkdir()
 manifest={}
 for i in range(N):
  p=fixture/f'dir-{i//64:03d}'/f'file-{i:05d}.bin';p.parent.mkdir(exist_ok=True)
  data=os.urandom(16384);p.write_bytes(data);os.utime(p,ns=(1700000000000000000,)*2)
  manifest[p.relative_to(fixture).as_posix()]=hashlib.sha256(data).digest()
 binary=(args.baseline_binary if mode=='before' else args.binary).resolve()
 bench.EXE=binary
 port,gui=bench.port(),bench.port()
 home=run/'home';_,key=bench.setup(home,fixture,port,gui,None,None)
 log=open(run/'console.log','wb');start=time.perf_counter()
 options={'creationflags':subprocess.CREATE_NO_WINDOW} if os.name=='nt' else {'start_new_session':True}
 process_env={**bench.ENV,'STMONITORED':'1', **({'STCPUPROFILE':'1'} if args.profile else {})}
 process=subprocess.Popen([str(binary),'serve','--home',str(home),'--no-browser','--no-tray','--no-restart','--no-upgrade','--log-file=-'],env=process_env,cwd=run,stdout=log,stderr=subprocess.STDOUT,**options)
 try:
  while time.perf_counter()-start<180:
   if process.poll() is not None:raise RuntimeError('isolated peer exited')
   try:
    status=req(gui,key,'db/status?folder=benchmark')
    if status['localFiles']==N and status['state']=='idle':break
   except (OSError,ValueError):pass
   time.sleep(.05)
  else:raise TimeoutError('initial scan')
  initial=time.perf_counter()-start
  sequence=status['sequence']
  unchanged=[];unchanged_cpu=[]
  for _ in range(3):
   usage_before=process_usage(process)
   start=time.perf_counter();req(gui,key,'db/scan?folder=benchmark','POST');unchanged.append(time.perf_counter()-start)
   usage_after=process_usage(process)
   if usage_before:unchanged_cpu.append(usage_after['cpu_s']-usage_before['cpu_s'])
   status=req(gui,key,'db/status?folder=benchmark');assert status['sequence']==sequence,(mode,'unchanged files reindexed')
  names=list(manifest)
  for name in names[:20]:
   data=os.urandom(17001);(fixture/name).write_bytes(data);manifest[name]=hashlib.sha256(data).digest()
  for name in names[20:30]:(fixture/name).unlink();del manifest[name]
  extra='new.bin';data=os.urandom(1001);(fixture/extra).write_bytes(data);manifest[extra]=hashlib.sha256(data).digest()
  start=time.perf_counter();req(gui,key,'db/scan?folder=benchmark','POST');changed=time.perf_counter()-start
  status=req(gui,key,'db/status?folder=benchmark');assert status['localFiles']==len(manifest),status
  for name in names[:20]+names[31:95]+[extra]:
   info=req(gui,key,'db/file?folder=benchmark&file='+urllib.parse.quote(name,safe=''))['local']
   assert base64.b64decode(info['blocksHash'])==hashlib.sha256(manifest[name]).digest(),(mode,name,'index hash mismatch')
   assert not info['deleted'] and info['numBlocks']==1,(mode,name)
  for name in names[20:30]:
   info=req(gui,key,'db/file?folder=benchmark&file='+urllib.parse.quote(name,safe=''))['local'];assert info['deleted'],name
  row={'trial':trial,'mode':mode,'files':N,'initial_s':initial,'unchanged_s':unchanged,'unchanged_cpu_s':unchanged_cpu,'process_usage':process_usage(process),'changed_s':changed,'index_hash_samples':85,'deletions':10,'unchanged_sequence_preserved':True}
  rows.append(row);(root/'results.json').write_text(json.dumps(rows,indent=2));print(json.dumps(row),flush=True)
 finally:
  try:req(gui,key,'system/shutdown','POST')
  except OSError:pass
  try:process.wait(timeout=20)
  except subprocess.TimeoutExpired:
   if os.name=='nt':subprocess.run(['taskkill','/PID',str(process.pid),'/T','/F'],stdout=subprocess.DEVNULL)
   else:os.killpg(process.pid,signal.SIGKILL)
   process.wait()
  log.close()

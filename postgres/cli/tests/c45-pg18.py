# usage: python3 -B pg18.py init | python3 -B pg18.py run <cases.json>
# cases: list of {"name":..., "steps":[["Q",sql] | ["X",sql,[oids],[values or null]]]}
import sys, socket, struct, subprocess, time, json
from pathlib import Path
root=Path('/private/tmp/codex-C45-pg18'); root.mkdir(exist_ok=True); bin=Path('/opt/homebrew/opt/postgresql@18/bin'); port='57445'
def recv(s,n):
    b=b''
    while len(b)<n:
        x=s.recv(n-len(b)); assert x,'EOF'; b+=x
    return b
def messages(s):
    out=[]
    while True:
        tag=recv(s,1); n=struct.unpack('!I',recv(s,4))[0]; body=recv(s,n-4)
        if tag==b'E' or tag==b'N':
            f=body.rstrip(b'\0').split(b'\0'); d={x[:1].decode():x[1:].decode() for x in f}
            out.append([tag.decode(),{k:d[k] for k in 'CMD' if k in d}])
        elif tag==b'D':
            cnt=struct.unpack('!H',body[:2])[0]; i=2; row=[]
            for _ in range(cnt):
                l=struct.unpack('!i',body[i:i+4])[0]; i+=4
                if l<0: row.append(None)
                else: row.append(body[i:i+l].decode('utf8','replace')); i+=l
            out.append(['D',row])
        elif tag==b'T':
            cnt=struct.unpack('!H',body[:2])[0]; i=2; cols=[]
            for _ in range(cnt):
                j=body.index(b'\0',i); name=body[i:j].decode(); i=j+1
                _,_,typ,_,_,_=struct.unpack('!IhIhih',body[i:i+18]); i+=18; cols.append([name,typ])
            out.append(['T',cols])
        elif tag==b't':
            cnt=struct.unpack('!H',body[:2])[0]; out.append(['t',list(struct.unpack('!'+'I'*cnt,body[2:2+4*cnt]))])
        elif tag in b'C': out.append(['C',body.rstrip(b'\0').decode()])
        elif tag==b'S': out.append(['S',[x.decode() for x in body.rstrip(b'\0').split(b'\0')]])
        elif tag in [b'1',b'2',b'3',b'n',b'I',b's']: out.append([tag.decode()])
        elif tag==b'Z': out.append(['Z',body.decode()]); return out
def msg(t,b): return t+struct.pack('!I',len(b)+4)+b
if sys.argv[1]=='init':
    r=subprocess.run([str(bin/'initdb'),'-D',str(root/'data'),'-A','trust','-U','postgres','--no-locale','-E','UTF8'],capture_output=True,text=True,timeout=120)
    print(r.returncode, r.stderr[-500:]); sys.exit(r.returncode)
cases=json.load(open(sys.argv[2]))
log=(root/'server.txt').open('a')
p=subprocess.Popen([str(bin/'postgres'),'-D',str(root/'data'),'-h','','-k',str(root),'-p',port,'-c','shared_buffers=16MB','-c','max_connections=10'],stdout=log,stderr=log)
print(subprocess.check_output([str(bin/'postgres'),'--version'],text=True).strip())
try:
    for _ in range(200):
        if (root/('.s.PGSQL.'+port)).exists(): break
        assert p.poll() is None,'server exited'; time.sleep(.05)
    time.sleep(0.3)
    for case in cases:
        with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
            s.settimeout(10); s.connect(str(root/('.s.PGSQL.'+port)))
            st=struct.pack('!I',196608)+b'user\0postgres\0database\0postgres\0\0'
            s.sendall(struct.pack('!I',len(st)+4)+st); startup=messages(s)
            res=[]
            s2=None
            if case.get('startup'): res.append(['startup',[m for m in startup if m[0]=='S']])
            for step in case['steps']:
                k=step[0]
                if k=='Q2':
                    if s2 is None:
                        s2=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM); s2.settimeout(10); s2.connect(str(root/('.s.PGSQL.'+port)))
                        s2.sendall(struct.pack('!I',len(st)+4)+st); messages(s2)
                    s2.sendall(msg(b'Q',step[1].encode()+b'\0')); res.append(['other: '+step[1],messages(s2)])
                    continue
                if k=='Q':
                    s.sendall(msg(b'Q',step[1].encode()+b'\0')); res.append([step[1],messages(s)])
                elif k=='P':
                    _,name,sql,oids=step
                    s.sendall(msg(b'P',name.encode()+b'\0'+sql.encode()+b'\0'+struct.pack('!H',len(oids))+b''.join(struct.pack('!I',o) for o in oids)))
                elif k=='B':
                    _,portal,name,pcodes,vals,rcodes=step
                    b=portal.encode()+b'\0'+name.encode()+b'\0'+struct.pack('!H',len(pcodes))+b''.join(struct.pack('!h',c) for c in pcodes)+struct.pack('!H',len(vals))
                    for v in vals:
                        b+=struct.pack('!i',-1) if v is None else struct.pack('!i',len(v.encode()))+v.encode()
                    b+=struct.pack('!H',len(rcodes))+b''.join(struct.pack('!h',c) for c in rcodes)
                    s.sendall(msg(b'B',b))
                elif k=='E':
                    s.sendall(msg(b'E',(step[1] if len(step)>1 else '').encode()+b'\0'+struct.pack('!I',0)))
                elif k=='D':
                    s.sendall(msg(b'D',step[1].encode()+step[2].encode()+b'\0'))
                elif k=='S':
                    s.sendall(msg(b'S',b'')); res.append(['Sync',messages(s)])
                elif k=='X':
                    _,sql,oids,vals=step[:4]; describe=len(step)>4 and step[4]
                    parse=b'\0'+sql.encode()+b'\0'+struct.pack('!H',len(oids))+b''.join(struct.pack('!I',o) for o in oids)
                    bind=b'\0\0'+struct.pack('!HH',0,len(vals))
                    for v in vals:
                        bind+=struct.pack('!i',-1) if v is None else struct.pack('!i',len(v.encode()))+v.encode()
                    bind+=struct.pack('!H',0)
                    m=msg(b'P',parse)
                    if describe: m+=msg(b'D',b'S\0')
                    m+=msg(b'B',bind)+msg(b'E',b'\0'+struct.pack('!I',0))+msg(b'S',b'')
                    s.sendall(m); res.append(['X '+sql+' '+json.dumps(vals),messages(s)])
                elif k=='DS':
                    _,sql,oids=step
                    m=msg(b'P',b'\0'+sql.encode()+b'\0'+struct.pack('!H',len(oids))+b''.join(struct.pack('!I',o) for o in oids))+msg(b'D',b'S\0')+msg(b'S',b'')
                    s.sendall(m); res.append(['DS '+sql,messages(s)])
            print(json.dumps({'case':case['name'],'res':res}))
            s.sendall(msg(b'X',b''))
            if s2 is not None: s2.sendall(msg(b'X',b'')); s2.close()
finally:
    p.terminate()
    try: p.wait(timeout=10)
    except subprocess.TimeoutExpired: p.kill(); p.wait(timeout=5)
    print('stopped',p.returncode)

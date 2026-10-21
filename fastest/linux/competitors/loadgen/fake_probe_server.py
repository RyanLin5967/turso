#!/usr/bin/env python3
"""fake_probe_server.py PROTO MODE PORTFILE -- a minimal PG or MySQL server for bbload's end-of-run home probe.
Written by the fresh reviewer comp-rev-2404 (its fakesrv.py, scratchpad rev2414/), taken in unchanged apart from this
header for test_probe_shapes.py (comp-rev-2404 MED-1, LOW-1). Speaks just enough of each protocol for bbload: PG
trust startup with sslmode/gssencmode off (bbload forces both), MySQL native-password handshake (any password).
Added for comp-rev-a145: mode late (both protocols: the probe is answered after 4.5 s, and Terminate/Quit is
answered by closing 4.5 s later); mode metaclose3 (mysql: a STEP's 'SELECT 3' gets count+def+EOF, then the server
closes the socket); every connection is closed when it ends (LOW-5).
Every query is answered with one row, except the text 'SELECT 1' (the probe; steps use SELECT 2), which gets MODE:
  pg:    silent | noz (T,D,C then stall, no ReadyForQuery) | partial (3 bytes of T then stall)
  mysql: silent | colcount (column-count packet then stall) | metastall (count+def+EOF then stall)
         | metaclose (count+def+EOF then close the socket)
Writes the port to PORTFILE, logs to stderr."""
import socket, struct, sys, threading, time

PROTO, MODE, PORTFILE = sys.argv[1:4]


def log(*a):
    print(time.strftime("%H:%M:%S"), *a, file=sys.stderr, flush=True)


def rd(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise EOFError
        b += c
    return b


def pgmsg(t, body):
    return t + struct.pack("!I", len(body) + 4) + body


def pg_conn(s, cid):
    n = struct.unpack("!I", rd(s, 4))[0]
    rd(s, n - 4)
    out = pgmsg(b"R", struct.pack("!I", 0))
    for k, v in (("server_version", "18.6"), ("client_encoding", "UTF8"), ("server_encoding", "UTF8"),
                 ("standard_conforming_strings", "on"), ("integer_datetimes", "on"), ("DateStyle", "ISO, MDY")):
        out += pgmsg(b"S", k.encode() + b"\0" + v.encode() + b"\0")
    out += pgmsg(b"K", struct.pack("!II", 4242 + cid, 7)) + pgmsg(b"Z", b"I")
    s.sendall(out)
    T = pgmsg(b"T", struct.pack("!H", 1) + b"x\0" + struct.pack("!IhIhih", 0, 0, 23, 4, -1, 0))
    D = pgmsg(b"D", struct.pack("!HI", 1, 1) + b"1")
    C = pgmsg(b"C", b"SELECT 1\0")
    Z = pgmsg(b"Z", b"I")
    while True:
        t = rd(s, 1)
        n = struct.unpack("!I", rd(s, 4))[0]
        body = rd(s, n - 4)
        if t == b"X":
            log("conn", cid, "terminate")
            if MODE == "late":  # a slow peer is slow to close too (comp-rev-a145 LOW-4)
                time.sleep(4.5)
            return
        q = body.rstrip(b"\0").decode()
        log("conn", cid, "query", q)
        if q == "SELECT 1" and MODE == "late":
            log("conn", cid, "probe -> late 4.5 s")
            time.sleep(4.5)
            s.sendall(T + D + C + Z)
            continue
        if q == "SELECT 1" and MODE != "normal":
            log("conn", cid, "probe -> mode", MODE)
            if MODE == "noz":
                s.sendall(T + D + C)
            elif MODE == "partial":
                s.sendall(T[:3])
            time.sleep(3600)
            return
        s.sendall(T + D + C + Z)


def mypkt(seq, body):
    return struct.pack("<I", len(body))[:3] + bytes([seq & 255]) + body


def myread(s):
    h = rd(s, 4)
    n = h[0] | h[1] << 8 | h[2] << 16
    return h[3], rd(s, n)


def my_conn(s, cid):
    caps = 0x1 | 0x4 | 0x8 | 0x200 | 0x2000 | 0x8000 | 0x10000 | 0x20000 | 0x80000
    salt = b"abcdefghijklmnopqrst"
    hs = (b"\x0a" + b"5.7.99-fake\0" + struct.pack("<I", 100 + cid) + salt[:8] + b"\0" + struct.pack("<H", caps & 0xFFFF)
          + bytes([33]) + struct.pack("<H", 2) + struct.pack("<H", caps >> 16) + bytes([21]) + b"\0" * 10 + salt[8:] + b"\0"
          + b"mysql_native_password\0")
    s.sendall(mypkt(0, hs))
    seq, _ = myread(s)
    s.sendall(mypkt(seq + 1, b"\x00\x00\x00\x02\x00\x00\x00"))
    lenc = lambda b: bytes([len(b)]) + b
    coldef = lenc(b"def") + lenc(b"") + lenc(b"") + lenc(b"") + lenc(b"1") + lenc(b"") + b"\x0c" + struct.pack(
        "<HIBHB", 63, 1, 8, 0x81, 0) + b"\0\0"
    eof = b"\xfe\x00\x00\x02\x00"
    while True:
        seq, body = myread(s)
        if body[:1] == b"\x01":
            log("conn", cid, "quit")
            if MODE == "late":  # a slow peer is slow to close too (comp-rev-a145 LOW-4)
                time.sleep(4.5)
            return
        q = body[1:].decode()
        log("conn", cid, "cmd", body[:1], q)
        if body[:1] != b"\x03":
            s.sendall(mypkt(1, b"\x00\x00\x00\x02\x00\x00\x00"))
            continue
        cc, cd, e1 = mypkt(1, b"\x01"), mypkt(2, coldef), mypkt(3, eof)
        if q == "SELECT 3" and MODE == "metaclose3":  # a MEASURED op's reply breaks after the metadata
            log("conn", cid, "step SELECT 3 -> metadata then close")
            s.sendall(cc + cd + e1)
            time.sleep(0.2)
            s.close()
            return
        if q == "SELECT 1" and MODE == "late":
            log("conn", cid, "probe -> late 4.5 s")
            time.sleep(4.5)
            s.sendall(cc + cd + e1 + mypkt(4, lenc(b"1")) + mypkt(5, eof))
            continue
        if q == "SELECT 1" and MODE not in ("normal", "metaclose3"):
            log("conn", cid, "probe -> mode", MODE)
            if MODE == "colcount":
                s.sendall(cc)
            elif MODE in ("metastall", "metaclose"):
                s.sendall(cc + cd + e1)
            if MODE == "metaclose":
                time.sleep(0.2)
                s.close()
                log("conn", cid, "closed after metadata")
                return
            time.sleep(3600)
            return
        s.sendall(cc + cd + e1 + mypkt(4, lenc(b"1")) + mypkt(5, eof))


def serve(c, cid):
    try:
        (pg_conn if PROTO == "pg" else my_conn)(c, cid)
    except (EOFError, OSError) as e:
        log("conn", cid, "ended", type(e).__name__)
    finally:
        # comp-rev-a145 LOW-5: close as a real server does after Terminate/Quit (or any end), so a client's close
        # sees EOF at once; the controls used to take 5.0 s waiting out bbload's close bound
        try:
            c.close()
        except OSError:
            pass


ls = socket.socket()
ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
ls.bind(("127.0.0.1", 0))
ls.listen(16)
open(PORTFILE, "w").write(str(ls.getsockname()[1]))
cid = 0
while True:
    c, _ = ls.accept()
    log("accept", cid)
    threading.Thread(target=serve, args=(c, cid), daemon=True).start()
    cid += 1

#!/usr/bin/env python3
"""copyrec.py HOST PORT [DB] -- record a server's answers to the COPY FROM STDIN scenarios of
copy_from_stdin_* (wire_sessions.rs): every backend message of each round, in order, as one line:
type byte and its readable content (G: format and column formats, C: tag, E/N: SQLSTATE and message,
Z: status, D: row). Run against PostgreSQL 18.6 for the expectations, and against tursopg to compare."""
import socket, struct, sys

HOST, PORT = sys.argv[1], int(sys.argv[2])
DB = sys.argv[3] if len(sys.argv) > 3 else "postgres"
USER = sys.argv[4] if len(sys.argv) > 4 else "postgres"


def msg(t, b):
    return t + struct.pack("!i", len(b) + 4) + b


class C:
    def __init__(self):
        self.s = socket.create_connection((HOST, PORT))
        self.s.settimeout(30)
        self.buf = b""
        body = struct.pack("!i", 196608)
        for k, v in (("user", USER), ("database", DB)):
            body += k.encode() + b"\0" + v.encode() + b"\0"
        body += b"\0"
        self.s.sendall(struct.pack("!i", len(body) + 4) + body)
        while self.read()[0] != b"Z":
            pass

    def read(self):
        while len(self.buf) < 5:
            d = self.s.recv(65536)
            if not d:
                return (b"", b"")
            self.buf += d
        n = struct.unpack("!i", self.buf[1:5])[0]
        while len(self.buf) < 1 + n:
            self.buf += self.s.recv(65536)
        t, b, self.buf = self.buf[:1], self.buf[5:1 + n], self.buf[1 + n:]
        return t, b

    def send(self, *msgs):
        self.s.sendall(b"".join(msgs))

    def until(self, stop=b"Z"):
        out = []
        while True:
            t, b = self.read()
            if t == b"":
                out.append("<closed>")
                return out
            if t == b"G":
                fmt, n = struct.unpack("!bh", b[:3])
                cols = [struct.unpack("!h", b[3 + 2 * i:5 + 2 * i])[0] for i in range(n)]
                out.append("G format=%d columns=%s" % (fmt, cols))
            elif t == b"C":
                out.append("C " + b.rstrip(b"\0").decode())
            elif t in (b"E", b"N"):
                f = {chr(p[0]): p[1:].decode("utf-8", "replace") for p in b.split(b"\0") if p}
                out.append("%s %s %s" % (t.decode(), f.get("C"), f.get("M")))
            elif t == b"Z":
                out.append("Z " + chr(b[0]))
            elif t == b"D":
                n = struct.unpack("!h", b[:2])[0]
                p, row = 2, []
                for _ in range(n):
                    ln = struct.unpack("!i", b[p:p + 4])[0]
                    p += 4
                    row.append(None if ln < 0 else b[p:p + ln].decode())
                    p += max(ln, 0)
                out.append("D %s" % row)
            elif t in (b"T", b"S"):
                continue
            else:
                out.append(t.decode())
            if t == stop or t == b"Z":
                return out


def q(c, sql):
    c.send(msg(b"Q", sql.encode() + b"\0"))
    return c.until()


def copy(c, sql, rounds):
    """A simple Query; at each CopyInResponse the next of `rounds` is sent: ("data", bytes) is
    CopyData then CopyDone, ("fail", text) CopyFail, ("query", sql) a Query message instead."""
    c.send(msg(b"Q", sql.encode() + b"\0"))
    out = []
    rounds = list(rounds)
    while True:
        part = c.until(stop=b"G")
        out += part
        if not part[-1].startswith("G"):
            return out
        kind, val = rounds.pop(0)
        if kind == "data":
            c.send(*( [msg(b"d", val)] if val else []), msg(b"c", b""))
        elif kind == "fail":
            c.send(msg(b"f", val.encode() + b"\0"))
        elif kind == "query":
            c.send(msg(b"Q", val.encode() + b"\0"))


def ext_copy(c, sql, data):
    """libpq's extended COPY: Parse, Bind, Execute, Sync; at CopyInResponse CopyData, CopyDone, Sync."""
    c.send(msg(b"P", b"\0" + sql.encode() + b"\0\0\0"), msg(b"B", b"\0\0\0\0\0\0\0\0"),
           msg(b"E", b"\0\0\0\0\0"), msg(b"S", b""))
    out = c.until(stop=b"G")
    if out[-1].startswith("G"):
        c.send(msg(b"d", data), msg(b"c", b""), msg(b"S", b""))
        out += c.until()
    return out


c = C()
R = []
def rec(label, out):
    print("%-14s %s" % (label, " | ".join(out)))

rec("setup", q(c, "DROP TABLE IF EXISTS cs; CREATE TABLE cs (id int PRIMARY KEY, v varchar(5), n numeric(6,2))"))
rec("copy", copy(c, "COPY cs (id, v) FROM STDIN", [("data", b"1\tab\n2\t\\N\n")]))
rec("rows", q(c, "SELECT id, v FROM cs ORDER BY id"))
rec("then", copy(c, "COPY cs (id, v) FROM STDIN; SELECT count(*) FROM cs", [("data", b"3\tc\n")]))
rec("twice", copy(c, "COPY cs (id) FROM STDIN; COPY cs (id) FROM STDIN; SELECT count(*) FROM cs",
                  [("data", b"4\n"), ("data", b"5\n")]))
rec("fail", copy(c, "INSERT INTO cs (id) VALUES (10); COPY cs (id, v) FROM STDIN; SELECT 1", [("fail", "boom")]))
rec("count", q(c, "SELECT count(*) FROM cs"))
rec("dup", copy(c, "COPY cs (id) FROM STDIN", [("data", b"6\n1\n")]))
rec("long", copy(c, "COPY cs (id, v) FROM STDIN", [("data", b"7\tabcdef\n")]))
rec("overflow", copy(c, "COPY cs (id, n) FROM STDIN", [("data", b"8\t10000\n")]))
rec("count", q(c, "SELECT count(*) FROM cs"))
rec("begin", q(c, "BEGIN"))
rec("inblock", copy(c, "COPY cs (id) FROM STDIN", [("data", b"20\n")]))
rec("failblock", copy(c, "COPY cs (id) FROM STDIN", [("fail", "stop")]))
rec("aborted", copy(c, "COPY cs (id) FROM STDIN", [("data", b"21\n")]))
rec("rollback", q(c, "ROLLBACK"))
rec("count", q(c, "SELECT count(*) FROM cs"))
rec("nosuch", copy(c, "COPY nosuch FROM STDIN", [("data", b"1\n")]))
rec("nocol", copy(c, "COPY cs (id, zz) FROM STDIN", [("data", b"1\n")]))
rec("extended", ext_copy(c, "COPY cs (id) FROM STDIN", b"30\n31\n"))
rec("count", q(c, "SELECT count(*) FROM cs"))
rec("options", copy(c, "COPY cs (id, v) FROM STDIN (DELIMITER ',', NULL 'nil', HEADER)",
                    [("data", b"id,v\n40,x\n41,nil\n")]))
rec("rows", q(c, "SELECT id, v FROM cs WHERE id >= 40 ORDER BY id"))
rec("empty", copy(c, "COPY cs (id) FROM STDIN", [("data", b"")]))
rec("badint", copy(c, "COPY cs (id) FROM STDIN", [("data", b"x\n")]))
rec("columns", copy(c, "COPY cs (id, v) FROM STDIN", [("data", b"50\n")]))
c.send(msg(b"Q", b"COPY cs (id) FROM STDIN\0"))
g = c.until(stop=b"G")
c.send(msg(b"H", b""), msg(b"S", b""), msg(b"d", b"51\n"), msg(b"S", b""), msg(b"c", b""))
rec("syncs", g + c.until())
rec("count", q(c, "SELECT count(*) FROM cs"))
rec("unexpected", copy(c, "COPY cs (id) FROM STDIN", [("query", "SELECT 1")]))
rec("after", q(C(), "SELECT count(*) FROM cs"))

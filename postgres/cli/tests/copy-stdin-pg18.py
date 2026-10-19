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
# Part 2 (review of the first fix): comment-led COPY, bigserial past int4, varchar/char spaces, the
# text format's escapes and empty line, the check order, FROM ''.
rec("setup2", q(c, "DROP TABLE IF EXISTS cb, c1; CREATE TABLE cb (id bigserial PRIMARY KEY, v varchar(3), w char(2)); CREATE TABLE c1 (v text)"))
rec("comment", copy(c, "/* lead */ COPY cs (id) FROM STDIN", [("data", b"60\n")]))
rec("extcomment", ext_copy(c, "-- lead\nCOPY cs (id) FROM STDIN", b"61\n"))
rec("count", q(c, "SELECT count(*) FROM cs"))
rec("bigserial", copy(c, "COPY cb (id, v) FROM STDIN", [("data", b"3000000000\tab \n3000000001\tabc  \n")]))
rec("spaces", q(c, "SELECT id, v, length(v) FROM cb ORDER BY id"))
rec("charpad", copy(c, "COPY cb (id, w) FROM STDIN", [("data", b"1\tx   \n")]))
rec("charrow", q(c, "SELECT id, w, octet_length(w) FROM cb WHERE id = 1"))
rec("charlong", copy(c, "COPY cb (id, w) FROM STDIN", [("data", b"2\txyz\n")]))
rec("leadspace", copy(c, "COPY cb (id, v) FROM STDIN", [("data", b"3\t  abc\n")]))
rec("escapes", copy(c, "COPY c1 FROM STDIN", [("data", b"a\\tb\nx\\\ty\n\\101\\x41\n\n")]))
rec("escrows", q(c, "SELECT v, length(v) FROM c1 ORDER BY v"))
rec("nosuchopt", copy(c, "COPY nosuch FROM STDIN (NOSUCH 1)", [("data", b"1\n")]))
rec("fromempty", q(c, "COPY cs FROM ''"))
rec("int4range", copy(c, "COPY cs (id) FROM STDIN", [("data", b"3000000000\n")]))
# Part 3 (second review, of b74a48d82): quotes before STDIN, FROM STDOUT, non-ASCII escapes,
# backslash-newline, \. not alone, PostgreSQL 16 integer syntax, option checks, line-by-line order.
rec("setup3", q(c, "DROP TABLE IF EXISTS \"o'b\", cp3; CREATE TABLE \"o'b\" (v text); CREATE TABLE cp3 (id int PRIMARY KEY, v text)"))
rec("quote", copy(c, "/* Ryan's load */ COPY cs (id) FROM STDIN", [("data", b"70\n")]))
rec("quotedname", copy(c, "COPY \"o'b\" FROM STDIN", [("data", b"x\n")]))
rec("fromstdout", copy(c, "COPY cs (id) FROM STDOUT", [("data", b"71\n")]))
rec("count", q(c, "SELECT count(*) FROM cs"))
rec("utf8esc", copy(c, "COPY c1 FROM STDIN", [("data", b"\\303\\251\n")]))
rec("badbyte", copy(c, "COPY c1 FROM STDIN", [("data", b"\\377\n")]))
rec("nul", copy(c, "COPY c1 FROM STDIN", [("data", b"\\0\n")]))
rec("bsnewline", copy(c, "COPY c1 FROM STDIN", [("data", b"a\\\nb\n")]))
rec("dotnotalone", copy(c, "COPY c1 FROM STDIN", [("data", b"abc\\.\n")]))
rec("c1rows", q(c, "SELECT v, length(v) FROM c1 WHERE v NOT IN ('', 'AA', 'a\tb', 'x\ty') ORDER BY v"))
rec("intsyntax", copy(c, "COPY cs (id) FROM STDIN", [("data", b"0x10\n1_000\n")]))
rec("introws", q(c, "SELECT id FROM cs WHERE id IN (16, 1000) ORDER BY id"))
rec("int8over", copy(c, "COPY cb (id) FROM STDIN", [("data", b"99999999999999999999\n")]))
for label, opts in [("delim2", "(DELIMITER ',;')"), ("delimbs", "(DELIMITER E'\\\\')"),
                    ("delimn", "(DELIMITER 'n')"), ("delimnl", "(DELIMITER E'\\n')"),
                    ("nullnl", "(NULL E'\\n')"), ("nulldelim", "(DELIMITER ',', NULL 'a,b')"),
                    ("delimempty", "(DELIMITER '')"), ("delimutf8", "(DELIMITER 'é')")]:
    rec(label, copy(c, "COPY c1 FROM STDIN " + opts, [("data", b"z\n")]))
rec("order", copy(c, "COPY cp3 (id) FROM STDIN", [("data", b"1\n1\nx\ty\n")]))
rec("unexpected", copy(c, "COPY cs (id) FROM STDIN", [("query", "SELECT 1")]))
rec("after", q(C(), "SELECT count(*) FROM cs"))

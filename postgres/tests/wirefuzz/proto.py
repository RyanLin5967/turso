"""proto.py -- a raw protocol-v3 client for the wire differential fuzzer (wirefuzz.py): both servers are read
the same way, byte for byte, with no driver converting anything.

  c = Conn(port, database)
  c.simple(sql)                -> Result   (one Query message; COPY ... FROM STDIN takes `copy_data`)
  c.extended(sql, params)      -> Result   (Parse with no declared types, Bind with text parameters and text
                                            results, Describe portal, Execute, Sync, in one write)

Result: rows [[str|None]], oids [int] (the last RowDescription's), tags [str], error (sqlstate, message) | None
(the first ErrorResponse), status 'I' | 'T' | 'E' (ReadyForQuery). Stdlib only.
"""
import socket
import struct


class Refused(Exception):
    pass


class Result:
    __slots__ = ("rows", "oids", "tags", "error", "status")

    def __init__(self):
        self.rows, self.oids, self.tags, self.error, self.status = [], [], [], None, None

    def as_dict(self):
        return {"rows": self.rows, "oids": self.oids, "tags": self.tags,
                "error": list(self.error) if self.error else None, "status": self.status}


def _msg(t, body):
    return t + struct.pack("!i", len(body) + 4) + body


class Conn:
    def __init__(self, port, database, host="127.0.0.1", user="postgres", timeout=60):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
        body = struct.pack("!i", 196608)
        for k, v in (("user", user), ("database", database), ("client_encoding", "UTF8")):
            body += k.encode() + b"\0" + v.encode() + b"\0"
        body += b"\0"
        self.s.sendall(struct.pack("!i", len(body) + 4) + body)
        r = self._reply()
        if r.error or r.status is None:
            raise Refused(r.error or ("", "closed during startup"))

    def _read(self):
        while len(self.buf) < 5:
            c = self.s.recv(1 << 16)
            if not c:
                raise EOFError("server closed the connection")
            self.buf += c
        n = struct.unpack("!i", self.buf[1:5])[0]
        while len(self.buf) < 1 + n:
            c = self.s.recv(1 << 16)
            if not c:
                raise EOFError("server closed the connection")
            self.buf += c
        t, body, self.buf = self.buf[:1], self.buf[5:1 + n], self.buf[1 + n:]
        return t, body

    def _reply(self, copy_data=None):
        r = Result()
        while True:
            t, b = self._read()
            if t == b"T":
                n = struct.unpack("!h", b[:2])[0]
                p, r.oids = 2, []
                for _ in range(n):
                    p = b.index(b"\0", p) + 1
                    r.oids.append(struct.unpack("!I", b[p + 6:p + 10])[0])
                    p += 18
            elif t == b"D":
                n = struct.unpack("!h", b[:2])[0]
                p, row = 2, []
                for _ in range(n):
                    ln = struct.unpack("!i", b[p:p + 4])[0]
                    p += 4
                    if ln < 0:
                        row.append(None)
                    else:
                        row.append(b[p:p + ln].decode("utf-8", "replace"))
                        p += ln
                r.rows.append(row)
            elif t == b"C":
                r.tags.append(b.rstrip(b"\0").decode())
            elif t == b"E":
                f = {}
                for part in b.split(b"\0"):
                    if part:
                        f[chr(part[0])] = part[1:].decode("utf-8", "replace")
                if r.error is None:
                    r.error = (f.get("C", ""), f.get("M", ""))
            elif t == b"G":
                # CopyInResponse: send the data (or none) and end the copy.
                data = (copy_data or "").encode()
                out = (_msg(b"d", data) if data else b"") + _msg(b"c", b"")
                self.s.sendall(out)
            elif t == b"Z":
                r.status = chr(b[0])
                return r

    def simple(self, sql, copy_data=None):
        self.s.sendall(_msg(b"Q", sql.encode() + b"\0"))
        return self._reply(copy_data)

    def extended(self, sql, params=()):
        q = sql.encode() + b"\0"
        m = _msg(b"P", b"\0" + q + struct.pack("!h", 0))
        bind = b"\0\0" + struct.pack("!h", 0) + struct.pack("!h", len(params))
        for v in params:
            if v is None:
                bind += struct.pack("!i", -1)
            else:
                e = str(v).encode()
                bind += struct.pack("!i", len(e)) + e
        bind += struct.pack("!h", 0)
        m += _msg(b"B", bind)
        m += _msg(b"D", b"P\0")
        m += _msg(b"E", b"\0" + struct.pack("!i", 0))
        m += _msg(b"S", b"")
        self.s.sendall(m)
        return self._reply()

    def close(self):
        try:
            self.s.sendall(_msg(b"X", b""))
        except OSError:
            pass
        self.s.close()

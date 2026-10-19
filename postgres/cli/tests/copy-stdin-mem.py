"""mem.py PORT PID MODE MIB ROWBYTES: the server's VmHWM rise for loading MIB MiB of rows of ROWBYTES bytes (newline
included) into m(v text) (int rows when ROWBYTES == 2: '1\n'), in one statement. MODE copy: COPY FROM STDIN in 64 KiB
CopyData messages; MODE insert: INSERT ... SELECT from generate_series (the engine's own cost, no COPY). Instrument:
/proc/PID/status VmHWM, read before and after the reply."""
import socket, struct, sys, time
PORT, PID, MODE, MIB, ROWB = int(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
src = open("copy-stdin-pg18.py" if __import__("os").path.exists("copy-stdin-pg18.py") else "copyrec.py").read().split("c = C()")[0]
sys.argv = ["x", "127.0.0.1", str(PORT)]
exec(src)

def hwm():
    for l in open("/proc/%s/status" % PID):
        if l.startswith("VmHWM"):
            return int(l.split()[1])

c = C()
ints = ROWB == 2
q(c, "DROP TABLE IF EXISTS m")
q(c, "CREATE TABLE m(v %s)" % ("int" if ints else "text"))
total = MIB * 1024 * 1024
rows = total // ROWB
before = hwm()
t0 = time.time()
if MODE == "copy":
    row = b"1\n" if ints else (b"x" * (ROWB - 1) + b"\n")
    chunk = row * (65536 // ROWB)
    c.send(msg(b"Q", b"COPY m FROM STDIN\0"))
    head = c.until(stop=b"G")
    sent = 0
    while sent < total:
        c.s.sendall(msg(b"d", chunk))
        sent += len(chunk)
    c.s.sendall(msg(b"c", b""))
    c.s.settimeout(3600)
    out = head + c.until()
else:
    val = "1" if ints else "repeat('x', %d)" % (ROWB - 1)
    c.s.settimeout(3600)
    out = q(c, "INSERT INTO m SELECT %s FROM generate_series(1, %d)" % (val, rows))
after = hwm()
print("%s %d MiB rows=%d rowbytes=%d: %s | VmHWM kB before %d after %d rise %d (%.2fx input) %.0fs" % (
    MODE, MIB, rows, ROWB, " | ".join(out), before, after, after - before, (after - before) * 1024 / total,
    time.time() - t0), flush=True)

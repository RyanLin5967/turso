"""hostbin.py -- the one place a gate or test under fastest/linux chooses the bash it runs (the platform class, Ryan's
class rule, DECISIONS 994f7d41e0: no gate silently inherits the host platform or layout).

Every shell script here needs bash 4+ (coproc, associative arrays, ${x,,}). macOS ships /bin/bash 3.2 first on the
default PATH; a run that passed did so only because its caller had put homebrew first. So no caller runs "bash" by
name: it runs bash(), which tries each `bash` on PATH and then the stated fallbacks, takes the first that reports
BASH_VERSINFO >= 4, and REFUSES (BashMissing, naming everything it tried) when none does -- never falls back to 3.2.
env_with_bash() gives children the same bash: a private directory holding only a `bash` link to it goes first on PATH,
so a script's own `bash x.sh` resolves too, and nothing else on PATH changes (homebrew's directory would also put its
python3 and coreutils first).

Blind spot, stated: a script that runs /bin/bash by absolute path is not covered; grep for it.
"""
import os
import atexit
import shutil
import subprocess
import tempfile

FALLBACKS = ("/opt/homebrew/bin/bash", "/usr/local/bin/bash", "/usr/bin/bash", "/bin/bash")


class BashMissing(RuntimeError):
    pass


def _major(path):
    try:
        r = subprocess.run([path, "-c", 'echo "${BASH_VERSINFO[0]}"'], capture_output=True, text=True, timeout=20)
        return int(r.stdout.strip()) if r.returncode == 0 else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


_cache = {}


def bash(path=None):
    """the absolute path of a bash 4+, searched on PATH (default: os.environ's) and then FALLBACKS"""
    path = os.environ.get("PATH", "") if path is None else path
    if path in _cache:
        return _cache[path]
    tried = []
    seen = set()
    for d in path.split(os.pathsep):
        c = os.path.join(d, "bash") if d else None
        if c and os.access(c, os.X_OK) and os.path.realpath(c) not in seen:
            seen.add(os.path.realpath(c))
            tried.append(c)
    tried += [c for c in FALLBACKS if os.access(c, os.X_OK) and os.path.realpath(c) not in seen]
    for c in tried:
        m = _major(c)
        if m is not None and m >= 4:
            _cache[path] = os.path.abspath(c)
            return _cache[path]
    raise BashMissing(f"no bash 4+ found (tried {tried or 'nothing'}); on macOS: brew install bash")


_shim = {}


def env_with_bash(env=None):
    """a copy of env (default os.environ) whose PATH puts a directory holding only `bash` (a link to bash()) first"""
    e = dict(os.environ if env is None else env)
    b = bash(e.get("PATH", ""))
    if b not in _shim:
        d = tempfile.mkdtemp(prefix="hostbin-")
        os.symlink(b, os.path.join(d, "bash"))
        atexit.register(shutil.rmtree, d, True)
        _shim[b] = d
    e["PATH"] = _shim[b] + os.pathsep + e.get("PATH", "")
    return e

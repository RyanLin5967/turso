"""v3cell.py -- the V3 cells, stated explicitly (review 2 item 4), never inferred from the filesystem's name.

A cell is a filesystem on a block device (ext4, xfs, btrfs: the probe's flush path has 1 layer, the mount's source
is not a loop) or on a loop device (ext4loop, xfsloop, btrfsloop: 2 layers, the loop and the filesystem holding its
backing file). firecheck.sh takes the cell as its 2nd argument and run.sh as V3_CELL; check.py and batchgate.py
assert the mount source and the layer count against it. On GitHub-hosted runners the block cells xfs and btrfs sit
on brd (no spare disk): fire-check only, refused for credit (the probe needs V3FLOOR_BRD=1 there, run.sh's bound
mode refuses it).
"""
CELLS = {
    "ext4": {"fstype": "ext4", "loop": False},
    "xfs": {"fstype": "xfs", "loop": False},
    "btrfs": {"fstype": "btrfs", "loop": False},
    "ext4loop": {"fstype": "ext4", "loop": True},
    "xfsloop": {"fstype": "xfs", "loop": True},
    "btrfsloop": {"fstype": "btrfs", "loop": True},
}


def kind(cell):
    return CELLS[cell]["fstype"]


def is_loop(cell):
    return CELLS[cell]["loop"]


def layout_problems(cell, fstype, source, layers):
    """[] when (fstype, mount source, the probe's flush_path list) is the cell's layout; else every mismatch."""
    c = CELLS.get(cell)
    if c is None:
        return ["unknown cell %r (one of %s)" % (cell, ", ".join(sorted(CELLS)))]
    p = []
    if fstype != c["fstype"]:
        p.append("fstype %s, cell %s means %s" % (fstype, cell, c["fstype"]))
    if not str(source or "").startswith("/dev/"):
        p.append("mount source %r is not a /dev block device path" % (source,))
    loop_src = str(source or "").startswith("/dev/loop")
    if loop_src != c["loop"]:
        p.append("mount source %s: cell %s means %s" % (source, cell, "a loop device" if c["loop"] else "a block device, no loop"))
    want = 2 if c["loop"] else 1
    if len(layers or []) != want:
        p.append("the flush path has %d layers, cell %s means %d" % (len(layers or []), cell, want))
    if layers:
        if layers[0].get("fstype") != c["fstype"]:
            p.append("layer 0 is %s, cell %s means %s" % (layers[0].get("fstype"), cell, c["fstype"]))
        if bool(layers[0].get("loop_backing")) != c["loop"]:
            p.append("layer 0 %s a loop, cell %s means %s" % ("is" if layers[0].get("loop_backing") else "is not", cell,
                                                              "a loop" if c["loop"] else "no loop"))
    return p

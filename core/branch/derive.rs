//! The DERIVED write set (r13-compose PREREG amendments 5 and 6, algorithm D).
//!
//! A merge needs the rows a branch wrote. The volatile Merger (turso b161e861d) recorded them in the
//! branch's in-memory state as its cursors wrote; a durable store would have to persist that second
//! record through every path that persists, loads, captures, replays and splices a branch, and four
//! adversary rounds found a new way to lose it each time. The branch's own pages already ARE that
//! record: the store must keep them correct for the branch's ordinary reads. So the write set is
//! derived at merge time, from the pages the branch owns, against the trunk as the branch forked it.
//!
//! # Why the pages suffice
//!
//! The branch differs from its base only on the pages it owns (O = keys(current) ∪ keys(inherited),
//! A6.1): every other page resolves to the trunk at the branch's fork, which is the base. A row
//! changes only if a page holding it changes, and moving a row into a page writes that page. So a
//! row that differs lies on an owned page in the branch, or on the base version of an owned page, or
//! in a base subtree an owned interior page no longer links (a page freed without being written:
//! free_page's AddToTrunk path writes only the freelist trunk). A subtree linked under one owned
//! interior page and unlinked from another, and not owned, moved without a write (an interior
//! balance): its rows are on both sides, so it cancels with no enumeration; its owned descendants
//! are diffed on their own.
//!
//! # Content semantics (A6.4)
//!
//! A row whose bytes on the branch equal its base bytes is no write at all, whatever statements the
//! branch ran: a branch that writes a row back to its base value cannot conflict.
//!
//! # What is refused (Scope)
//!
//! A changed sqlite_schema row (DDL); a table whose root is now an empty leaf while its base held
//! rows (a clear, or a DML delete of every row, which the pages cannot tell apart); an owned page no
//! tree reaches and no owned leaf's overflow chain names (an incremental blob write on a row whose
//! leaf the branch did not write); any owned index-kind page when the schema has a WITHOUT ROWID
//! table; a page newly linked that was neither owned nor unlinked elsewhere.
//!
//! Every page is parsed by the core's own b-tree page code (`PageInner`, `read_btree_cell`); only an
//! overflow chain (a 4-byte next pointer, then content) is walked here.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::io::Buffer;
use crate::storage::pager::PageInner;
use crate::storage::sqlite3_ondisk::{BTreeCell, PageType};
use crate::{LimboError, Result};

/// One version of the database a derivation reads: the branch's view, the base (the trunk at the
/// branch's fork), or the trunk now.
pub(crate) trait PageSource {
    /// Page `page`'s bytes, or `None` past this version's last page.
    fn read(&mut self, page: u32) -> Result<Option<Arc<Vec<u8>>>>;
}

/// A b-tree root the schema names.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Tree {
    pub(crate) root: u32,
    /// A table b-tree (a rowid table, or sqlite_schema); otherwise an index b-tree (an index, or a
    /// WITHOUT ROWID table).
    pub(crate) table: bool,
    pub(crate) without_rowid: bool,
}

/// One changed row: its base record and its branch record (`None`: absent on that side).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Change {
    pub(crate) base: Option<Vec<u8>>,
    pub(crate) theirs: Option<Vec<u8>>,
}

/// Counters of one derivation (summed into `BranchMergeWork`).
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DeriveCounters {
    pub(crate) pages_read: u64,
    pub(crate) attribution_seeks: u64,
    pub(crate) rows_compared: u64,
    pub(crate) subtrees_enumerated: u64,
    pub(crate) subtrees_cancelled: u64,
    pub(crate) freelist_reads: u64,
}

/// Why a derivation refuses the branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeriveRefusal {
    Ddl,
    ClearOrDeleteAll,
    Unattributed,
    WithoutRowid,
}

impl DeriveRefusal {
    pub(crate) fn scope(self) -> &'static str {
        match self {
            DeriveRefusal::Ddl => "the branch changed the schema",
            DeriveRefusal::ClearOrDeleteAll => {
                "the branch cleared a table or deleted every row of it (not told apart)"
            }
            DeriveRefusal::Unattributed => {
                "the branch owns a page no tree reaches (an incremental blob write, or an unattributed link)"
            }
            DeriveRefusal::WithoutRowid => "the branch wrote an index-kind page and the schema has a WITHOUT ROWID table",
        }
    }
}

/// What a derivation found: every changed row by (table root, rowid), or a refusal.
#[derive(Debug, Default)]
pub(crate) struct Derived {
    pub(crate) changes: BTreeMap<(u32, i64), Change>,
    pub(crate) refusal: Option<DeriveRefusal>,
    pub(crate) counters: DeriveCounters,
}

/// A page source with a cache, counting the pages it actually read.
struct Cached<'a> {
    src: &'a mut dyn PageSource,
    pages: HashMap<u32, Option<Arc<Vec<u8>>>>,
    read: u64,
    /// The page size less the reserved bytes: what a cell's overflow threshold is computed from.
    usable: usize,
}

impl<'a> Cached<'a> {
    fn new(src: &'a mut dyn PageSource, usable: usize) -> Self {
        Self {
            src,
            pages: HashMap::new(),
            read: 0,
            usable,
        }
    }

    fn get(&mut self, page: u32) -> Result<Option<Arc<Vec<u8>>>> {
        if let Some(p) = self.pages.get(&page) {
            return Ok(p.clone());
        }
        let p = self.src.read(page)?;
        self.read += 1;
        self.pages.insert(page, p.clone());
        Ok(p)
    }

    /// The page parsed as a b-tree page, or `None` when it is past the end or is not a b-tree page.
    fn btree(&mut self, page: u32) -> Result<Option<Parsed>> {
        let Some(bytes) = self.get(page)? else {
            return Ok(None);
        };
        Ok(Parsed::new(page, &bytes, self.usable))
    }
}

/// A parsed b-tree page: its kind, its cells (rowid or none, local payload info) and its children.
struct Parsed {
    kind: PageType,
    /// Table cells: (rowid, local payload, full size, first overflow page). Index cells: rowid 0.
    cells: Vec<(i64, Vec<u8>, u64, Option<u32>)>,
    /// Interior pages: every child pointer, rightmost last. Leaves: empty.
    children: Vec<u32>,
    /// Interior table pages: each cell's key, the separator for the child before it.
    keys: Vec<i64>,
}

impl Parsed {
    fn new(page: u32, bytes: &[u8], usable: usize) -> Option<Parsed> {
        let mut inner = PageInner::from_buffer(Buffer::new(bytes.to_vec()));
        inner.id = page as usize;
        let kind = inner.page_type().ok()?;
        let n = inner.cell_count();
        // A page that is not a b-tree page can carry any type byte: its cell count must at least
        // fit the page before any cell pointer is read.
        let header = inner.offset() + if matches!(kind, PageType::TableLeaf | PageType::IndexLeaf) { 8 } else { 12 };
        if header + 2 * n > bytes.len() {
            return None;
        }
        let mut cells = Vec::with_capacity(n);
        let mut children = Vec::new();
        let mut keys = Vec::new();
        for i in 0..n {
            let cell = inner.cell_get(i, usable).ok()?;
            match cell {
                BTreeCell::TableLeafCell(c) => {
                    cells.push((c.rowid, c.payload.to_vec(), c.payload_size, c.first_overflow_page))
                }
                BTreeCell::TableInteriorCell(c) => {
                    children.push(c.left_child_page);
                    keys.push(c.rowid);
                }
                BTreeCell::IndexLeafCell(c) => {
                    cells.push((0, c.payload.to_vec(), c.payload_size, c.first_overflow_page))
                }
                BTreeCell::IndexInteriorCell(c) => {
                    children.push(c.left_child_page);
                    cells.push((0, c.payload.to_vec(), c.payload_size, c.first_overflow_page));
                }
            }
        }
        if let Ok(Some(right)) = inner.rightmost_pointer() {
            children.push(right);
        }
        Some(Parsed {
            kind,
            cells,
            children,
            keys,
        })
    }

    fn is_table(&self) -> bool {
        matches!(self.kind, PageType::TableLeaf | PageType::TableInterior)
    }

    fn is_leaf(&self) -> bool {
        matches!(self.kind, PageType::TableLeaf | PageType::IndexLeaf)
    }

    /// The key a descent from the root follows to reach this page: its first cell's.
    fn first_key(&self) -> Option<i64> {
        if self.kind == PageType::TableLeaf {
            self.cells.first().map(|c| c.0)
        } else {
            self.keys.first().copied()
        }
    }
}

/// A row's full payload: the local part plus its overflow chain (`usable` bytes per page, a 4-byte
/// next pointer first). The chain's pages are added to `chain`.
fn full_payload(
    src: &mut Cached<'_>,
    local: &[u8],
    size: u64,
    first: Option<u32>,
    usable: usize,
    chain: &mut HashSet<u32>,
) -> Result<Vec<u8>> {
    let mut out = local.to_vec();
    let mut next = first;
    let size = size as usize;
    while let Some(page) = next {
        if out.len() >= size {
            break;
        }
        chain.insert(page);
        let bytes = src.get(page)?.ok_or_else(|| {
            LimboError::Corrupt(format!("derive: overflow page {page} is past the end"))
        })?;
        let n = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let take = (size - out.len()).min(usable - 4);
        out.extend_from_slice(&bytes[4..4 + take]);
        next = (n != 0).then_some(n);
    }
    if out.len() != size {
        return Err(LimboError::Corrupt(format!(
            "derive: a payload of {size} bytes assembled to {}",
            out.len()
        )));
    }
    Ok(out)
}

/// Descend the table b-tree rooted at `root` toward `key` and say whether it passes through
/// `target`. Each page read along the way is one the cache counts.
fn reaches(src: &mut Cached<'_>, root: u32, key: i64, target: u32) -> Result<bool> {
    let mut page = root;
    for _ in 0..64 {
        if page == target {
            return Ok(true);
        }
        let Some(p) = src.btree(page)? else {
            return Ok(false);
        };
        if !p.is_table() || p.is_leaf() {
            return Ok(false);
        }
        // A table interior page: keys[i] bounds children[i] from above; the rightmost is last.
        let i = p.keys.iter().position(|&k| key <= k).unwrap_or(p.keys.len());
        page = p.children[i];
    }
    Err(LimboError::Corrupt(format!(
        "derive: no leaf within 64 levels of root {root}"
    )))
}

/// The table root (of `trees`) whose b-tree page `p` is, by descent toward its first key; `None`
/// when no table root reaches it.
fn attribute(
    src: &mut Cached<'_>,
    trees: &[Tree],
    page: u32,
    parsed: &Parsed,
    seeks: &mut u64,
) -> Result<Option<u32>> {
    if trees.iter().any(|t| t.root == page && t.table) {
        return Ok(Some(page));
    }
    let Some(key) = parsed.first_key() else {
        // An empty page that is not a root belongs to no tree.
        return Ok(None);
    };
    for t in trees.iter().filter(|t| t.table) {
        *seeks += 1;
        if reaches(src, t.root, key, page)? {
            return Ok(Some(t.root));
        }
    }
    Ok(None)
}

/// Every row of the base subtree under `page`, into `rows` (a dropped subtree: its rows are deleted
/// unless an owned page holds them again).
fn enumerate(
    src: &mut Cached<'_>,
    page: u32,
    usable: usize,
    rows: &mut BTreeMap<i64, Vec<u8>>,
) -> Result<()> {
    let mut stack = vec![page];
    let mut chain = HashSet::new();
    while let Some(page) = stack.pop() {
        let Some(p) = src.btree(page)? else {
            return Err(LimboError::Corrupt(format!(
                "derive: a dropped subtree names page {page}, which is not a b-tree page"
            )));
        };
        if p.is_leaf() {
            for (rowid, local, size, first) in p.cells {
                let payload = full_payload(src, &local, size, first, usable, &mut chain)?;
                rows.insert(rowid, payload);
            }
        } else {
            stack.extend(p.children.iter().copied());
        }
    }
    Ok(())
}

/// The pages of the branch's freelist that it wrote: the chain's owned prefix (A6.5). `free_page`
/// writes only the head trunk or makes the freed page the new head, and `allocate_page` takes from
/// the head, so the branch's freelist pages sit in a prefix of the chain, and the walk stops at the
/// first trunk page the branch does not own (otherwise it would inherit the trunk's whole freelist,
/// which is not flat in H).
fn owned_freelist(
    src: &mut Cached<'_>,
    owned: &BTreeSet<u32>,
    reads: &mut u64,
) -> Result<HashSet<u32>> {
    let mut free = HashSet::new();
    let Some(header) = src.get(1)? else {
        return Ok(free);
    };
    let mut trunk = u32::from_be_bytes(header[32..36].try_into().unwrap());
    let whole = super::store::mutant("r13_freelist_whole");
    let mut guard = 0u64;
    while trunk != 0 {
        if !whole && !owned.contains(&trunk) {
            break;
        }
        guard += 1;
        if guard > 1 << 32 {
            return Err(LimboError::Corrupt("derive: a freelist chain that does not end".into()));
        }
        let bytes = src.get(trunk)?.ok_or_else(|| {
            LimboError::Corrupt(format!("derive: freelist trunk {trunk} is past the end"))
        })?;
        *reads += 1;
        free.insert(trunk);
        let leaves = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
        for i in 0..leaves {
            let at = 8 + 4 * i;
            if at + 4 > bytes.len() {
                break;
            }
            free.insert(u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()));
        }
        trunk = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    }
    Ok(free)
}

/// Algorithm D: the rows that differ between the branch's view (`theirs`) and its base (`base`),
/// over the pages the branch owns.
pub(crate) fn derive(
    theirs: &mut dyn PageSource,
    base: &mut dyn PageSource,
    owned: &[u32],
    trees: &[Tree],
    usable: usize,
) -> Result<Derived> {
    let mut b = Cached::new(theirs, usable);
    let mut a = Cached::new(base, usable);
    let mut out = Derived::default();
    let owned: BTreeSet<u32> = owned.iter().copied().collect();
    let without_rowid = trees.iter().any(|t| t.without_rowid);

    // The branch's own freelist pages are not b-tree pages of the branch.
    let free = owned_freelist(&mut b, &owned, &mut out.counters.freelist_reads)?;

    // Per table root: the branch side and the base side, and the child links of owned interiors.
    let mut b_rows: HashMap<u32, BTreeMap<i64, Vec<u8>>> = HashMap::new();
    let mut a_rows: HashMap<u32, BTreeMap<i64, Vec<u8>>> = HashMap::new();
    let mut b_links: HashMap<u32, HashMap<u32, Vec<u32>>> = HashMap::new();
    let mut a_links: HashMap<u32, HashMap<u32, Vec<u32>>> = HashMap::new();
    let mut overflow: HashSet<u32> = HashSet::new();
    let mut unattributed: Vec<u32> = Vec::new();
    let mut seeks = 0u64;

    // The branch side: owned pages that are not on its freelist.
    for &page in owned.iter().filter(|p| !free.contains(p)) {
        let Some(parsed) = b.btree(page)? else {
            unattributed.push(page);
            continue;
        };
        if !parsed.is_table() {
            if without_rowid {
                out.refusal = Some(DeriveRefusal::WithoutRowid);
                return Ok(finish(out, &b, &a, seeks));
            }
            // An index of a rowid table: Replay's SQL maintains it.
            continue;
        }
        let Some(root) = attribute(&mut b, trees, page, &parsed, &mut seeks)? else {
            unattributed.push(page);
            continue;
        };
        if parsed.is_leaf() {
            let mut rows = BTreeMap::new();
            for (rowid, local, size, first) in parsed.cells {
                let payload = full_payload(&mut b, &local, size, first, usable, &mut overflow)?;
                rows.insert(rowid, payload);
            }
            b_rows.entry(root).or_default().extend(rows);
        } else {
            b_links.entry(root).or_default().insert(page, parsed.children);
        }
    }
    // An owned page no tree reaches must be on an owned leaf's overflow chain.
    if unattributed.iter().any(|p| !overflow.contains(p)) {
        out.refusal = Some(DeriveRefusal::Unattributed);
        return Ok(finish(out, &b, &a, seeks));
    }

    // The base side: the base version of every owned page (freed ones included).
    let mut base_chain = HashSet::new();
    for &page in &owned {
        let Some(parsed) = a.btree(page)? else {
            continue;
        };
        if !parsed.is_table() {
            continue;
        }
        let Some(root) = attribute(&mut a, trees, page, &parsed, &mut seeks)? else {
            // A base page no table root reaches: an overflow page, or a freelist page of the base.
            continue;
        };
        if parsed.is_leaf() {
            let mut rows = BTreeMap::new();
            for (rowid, local, size, first) in parsed.cells {
                let payload = full_payload(&mut a, &local, size, first, usable, &mut base_chain)?;
                rows.insert(rowid, payload);
            }
            a_rows.entry(root).or_default().extend(rows);
        } else {
            a_links.entry(root).or_default().insert(page, parsed.children);
        }
    }

    // Links: per tree, children an owned interior page linked on one side and not on the other.
    let roots: BTreeSet<u32> = b_rows
        .keys()
        .chain(a_rows.keys())
        .chain(b_links.keys())
        .chain(a_links.keys())
        .copied()
        .collect();
    let cancel = !super::store::mutant("r13_no_relink_cancel");
    for &root in &roots {
        let empty = HashMap::new();
        let bl = b_links.get(&root).unwrap_or(&empty);
        let al = a_links.get(&root).unwrap_or(&empty);
        let mut linked: BTreeSet<u32> = BTreeSet::new();
        let mut unlinked: BTreeSet<u32> = BTreeSet::new();
        for page in bl.keys().chain(al.keys()).copied().collect::<BTreeSet<u32>>() {
            let bc: BTreeSet<u32> = bl.get(&page).map(|v| v.iter().copied().collect()).unwrap_or_default();
            let ac: BTreeSet<u32> = al.get(&page).map(|v| v.iter().copied().collect()).unwrap_or_default();
            linked.extend(bc.difference(&ac));
            unlinked.extend(ac.difference(&bc));
        }
        for &x in &unlinked {
            if owned.contains(&x) {
                continue;
            }
            if cancel && linked.contains(&x) {
                out.counters.subtrees_cancelled += 1;
                continue;
            }
            // Dropped: its rows are on the base side (they are deleted unless an owned page holds
            // them again), and a moved subtree, uncancelled, enumerates on both sides.
            out.counters.subtrees_enumerated += 1;
            enumerate(&mut a, x, usable, a_rows.entry(root).or_default())?;
            if !cancel && linked.contains(&x) {
                enumerate(&mut b, x, usable, b_rows.entry(root).or_default())?;
            }
        }
        for &x in &linked {
            if owned.contains(&x) || unlinked.contains(&x) {
                continue;
            }
            // A page the branch did not write, now linked where its base did not link it, and not
            // unlinked anywhere: no valid tree does that.
            out.refusal = Some(DeriveRefusal::Unattributed);
            return Ok(finish(out, &b, &a, seeks));
        }
    }

    // A clear (or a DML delete of every row): the root, owned, is an empty leaf on the branch and
    // held rows in the base.
    for t in trees.iter().filter(|t| t.table && owned.contains(&t.root)) {
        let Some(bp) = b.btree(t.root)? else {
            continue;
        };
        if bp.kind == PageType::TableLeaf && bp.cells.is_empty() {
            let Some(ap) = a.btree(t.root)? else {
                continue;
            };
            if ap.kind == PageType::TableInterior || !ap.cells.is_empty() {
                out.refusal = Some(DeriveRefusal::ClearOrDeleteAll);
                return Ok(finish(out, &b, &a, seeks));
            }
        }
    }

    // The diff, by content.
    for &root in &roots {
        let empty = BTreeMap::new();
        let br = b_rows.get(&root).unwrap_or(&empty);
        let ar = a_rows.get(&root).unwrap_or(&empty);
        let keys: BTreeSet<i64> = br.keys().chain(ar.keys()).copied().collect();
        for k in keys {
            out.counters.rows_compared += 1;
            let theirs = br.get(&k);
            let before = ar.get(&k);
            if theirs != before {
                if root == 1 {
                    out.refusal = Some(DeriveRefusal::Ddl);
                    return Ok(finish(out, &b, &a, seeks));
                }
                out.changes.insert(
                    (root, k),
                    Change {
                        base: before.cloned(),
                        theirs: theirs.cloned(),
                    },
                );
            }
        }
    }
    Ok(finish(out, &b, &a, seeks))
}

fn finish(mut out: Derived, b: &Cached<'_>, a: &Cached<'_>, seeks: u64) -> Derived {
    out.counters.pages_read = b.read + a.read;
    out.counters.attribution_seeks = seeks;
    out
}

/// The record at (table root, rowid) in a version of the database, by descent: `None` when absent.
/// The Merger's MV4 reads the trunk NOW with it.
pub(crate) fn row_at(
    src: &mut dyn PageSource,
    root: u32,
    rowid: i64,
    usable: usize,
    pages_read: &mut u64,
) -> Result<Option<Vec<u8>>> {
    let mut c = Cached::new(src, usable);
    let mut page = root;
    let mut found = None;
    for _ in 0..64 {
        let Some(p) = c.btree(page)? else {
            break;
        };
        if !p.is_table() {
            break;
        }
        if p.is_leaf() {
            if let Some((_, local, size, first)) = p.cells.into_iter().find(|c| c.0 == rowid) {
                let mut chain = HashSet::new();
                found = Some(full_payload(&mut c, &local, size, first, usable, &mut chain)?);
            }
            break;
        }
        let i = p.keys.iter().position(|&k| rowid <= k).unwrap_or(p.keys.len());
        page = p.children[i];
    }
    *pages_read += c.read;
    Ok(found)
}

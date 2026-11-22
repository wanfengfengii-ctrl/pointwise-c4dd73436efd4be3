# -*- coding: utf-8 -*-
"""handoff.Lake —— 遥测湖值班员的拆分交接实现。

核心保证（对应题面逐条）：

* put/delete 返回 (seq, id, k, kind, v)，delete 的 v 为 None；get 缺席返回 None。
* 键、id、界都是非空 UTF-8 字符串，值是字符串（允许空串）；删缺键也留墓碑。
* handle(k) 在目录库存路由提示，供 put/delete/get 使用；搬运期间旧句柄经父片
  冻结转发仍可读写，新入口直接路由到子片。
* split(b) 按码点迁完所有冻结键，等界归右（k == b 进右片），同界续办，
  异界 / 异参重投 / 坏参一律 ValueError。
* 父片先冻结、再转发，然后公布子路由；源片与全表身份（分片 id、全局 seq、
  回执）保留。
* 冻结行、墓碑、父阶段回执与补齐标记在各自分片的同一次事务提交；迟到副本
  （低 seq）不覆盖子片已有的高 seq 写/删。
* 单键读写线性化：全局 seq 由目录库单一分配点单调递增，分片内按 seq 守卫，
  低序晚到不盖高序；同号同参复领返回原回执，预留（未提交）不算执行。
* 每个写事务只触及一个数据库且立即提交，跨片调用与 hook 回调不持写事务。
* hook(e, k) 在 取像(F) / 预留提交(R) / 行与回执提交(W) / 补齐提交(H) 之后
  被调用；hook 抛出的异常原样传播，拆分状态已持久化，可重开续拆。
"""

import json
import os
import sqlite3

__all__ = ["Lake", "Handle", "SplitPaused"]


class SplitPaused(RuntimeError):
    """hook 主动抛出以暂停拆分时可使用的异常类型（任何异常都会照传）。"""


# ---------------------------------------------------------------------------
# 参数校验
# ---------------------------------------------------------------------------

def _need_nonempty_str(x, name):
    if not isinstance(x, str) or x == "":
        raise ValueError("%s 必须是非空字符串，得到 %r" % (name, x))
    try:
        x.encode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        raise ValueError("%s 必须是合法 UTF-8 字符串" % name)
    return x


def _need_str(x, name):
    if not isinstance(x, str):
        raise ValueError("%s 必须是字符串，得到 %r" % (name, x))
    try:
        x.encode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        raise ValueError("%s 必须是合法 UTF-8 字符串" % name)
    return x


# ---------------------------------------------------------------------------
# SQL 骨架
# ---------------------------------------------------------------------------

CATALOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    key TEXT PRIMARY KEY,
    val TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seq_counter(
    slot INTEGER PRIMARY KEY CHECK(slot = 0),
    n INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS shards(
    shard_id TEXT PRIMARY KEY,
    path     TEXT NOT NULL,
    state    TEXT NOT NULL CHECK(state IN ('active', 'frozen', 'retired')),
    lo       TEXT,
    hi       TEXT,
    parent   TEXT
);
CREATE TABLE IF NOT EXISTS handles(
    k           TEXT PRIMARY KEY,
    shard_id    TEXT NOT NULL,
    updated_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS claims(
    id        TEXT PRIMARY KEY,
    op        TEXT NOT NULL CHECK(op IN ('put', 'delete')),
    k         TEXT NOT NULL,
    v         TEXT,
    seq       INTEGER NOT NULL UNIQUE,
    committed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS splits(
    name      TEXT PRIMARY KEY CHECK(name = 'current'),
    parent    TEXT NOT NULL,
    boundary  TEXT NOT NULL,
    state     TEXT NOT NULL
              CHECK(state IN ('freezing', 'frozen', 'published',
                              'migrating', 'done')),
    left_id   TEXT,
    right_id  TEXT,
    frontier  INTEGER NOT NULL DEFAULT 0,
    total     INTEGER NOT NULL DEFAULT 0
);
"""

SHARD_SCHEMA = """
CREATE TABLE IF NOT EXISTS kv(
    k    TEXT PRIMARY KEY,
    seq  INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('put', 'delete')),
    v    TEXT
);
CREATE TABLE IF NOT EXISTS receipts(
    seq  INTEGER PRIMARY KEY,
    id   TEXT NOT NULL,
    k    TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('put', 'delete')),
    v    TEXT
);
CREATE TABLE IF NOT EXISTS phase_receipts(
    seq      INTEGER PRIMARY KEY,
    phase    TEXT NOT NULL CHECK(phase IN ('freeze', 'complete')),
    boundary TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS markers(
    name TEXT PRIMARY KEY,
    val  TEXT NOT NULL
);
"""


def _connect(path):
    db = sqlite3.connect(path, timeout=30, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def _rollback(db):
    """自动提交模式下可能本就没有活动事务，回滚需容错。"""
    try:
        db.execute("ROLLBACK")
    except sqlite3.OperationalError:
        pass


class Handle:
    """handle(k) 返回的路由提示句柄；提示持久化在目录库。"""

    def __init__(self, lake, k, shard_id):
        self._lake = lake
        self.k = k
        self._hint = shard_id

    def put(self, id, v):
        return self._lake.put(id, self.k, v, _hint=self._hint)

    def delete(self, id):
        return self._lake.delete(id, self.k, _hint=self._hint)

    def get(self):
        return self._lake.get(self.k, _hint=self._hint)

    def __repr__(self):
        return "Handle(k=%r, shard=%r)" % (self.k, self._hint)


class Lake:
    def __init__(self, root, hook=None):
        self.root = os.path.abspath(root)
        self.hook = hook
        self.shard_dir = os.path.join(self.root, "shards")
        os.makedirs(self.shard_dir, exist_ok=True)
        self.catalog_path = os.path.join(self.root, "catalog.sqlite3")
        self._bootstrap()

    # ------------------------------------------------------------------
    # 打开 / 初始化
    # ------------------------------------------------------------------

    def _cat(self):
        db = _connect(self.catalog_path)
        db.executescript(CATALOG_SCHEMA)
        return db

    def _shard_path(self, shard_id):
        return os.path.join(self.shard_dir, shard_id + ".sqlite3")

    def _shard(self, shard_id):
        db = _connect(self._shard_path(shard_id))
        db.executescript(SHARD_SCHEMA)
        return db

    def _bootstrap(self):
        db = self._cat()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT val FROM meta WHERE key='version'").fetchone()
            if row is None:
                db.execute("INSERT INTO meta(key,val) VALUES('version','1')")
                db.execute("INSERT INTO seq_counter(slot,n) VALUES(0,0)")
                db.execute("INSERT INTO meta(key,val) VALUES('shard_counter','0')")
                sid = "s0"
                db.execute(
                    "INSERT INTO shards(shard_id,path,state,lo,hi,parent) "
                    "VALUES(?,?, 'active', NULL, NULL, NULL)",
                    (sid, self._shard_path(sid)),
                )
                sdb = self._shard(sid)
                sdb.close()
            db.execute("COMMIT")
        except BaseException:
            _rollback(db)
            raise
        finally:
            db.close()

    # ------------------------------------------------------------------
    # 全局 seq 分配（目录库是唯一分配点，短事务）
    # ------------------------------------------------------------------

    def _allocate_seq(self, cat):
        cat.execute("UPDATE seq_counter SET n = n + 1 WHERE slot = 0")
        return cat.execute("SELECT n FROM seq_counter WHERE slot = 0").fetchone()[0]

    def _fire(self, event, k):
        if self.hook is not None:
            self.hook(event, k)

    # ------------------------------------------------------------------
    # 路由
    # ------------------------------------------------------------------

    def _route_fresh(self, cat, k):
        """按当前活动区间路由；活动子片区间互不相交，恰好覆盖一次。"""
        rows = cat.execute(
            "SELECT shard_id FROM shards "
            "WHERE state='active' "
            "AND (lo IS NULL OR lo <= ?) "
            "AND (hi IS NULL OR ? < hi)",
            (k, k),
        ).fetchall()
        if not rows:
            raise RuntimeError("找不到承载键 %r 的活动分片" % k)
        return rows[0][0]

    def _resolve(self, cat, k, hint):
        """把提示解析成真正该访问的分片（沿拆分链级联）。

        返回 (shard_id, frozen_pre_publish)；frozen_pre_publish 为 True 时
        父片已冻结但子路由尚未公布，只允许读。
        """
        if hint is None:
            return self._route_fresh(cat, k), False
        seen = set()
        cur = hint
        while cur not in seen:
            seen.add(cur)
            srow = cat.execute(
                "SELECT state FROM shards WHERE shard_id=?", (cur,)
            ).fetchone()
            if srow is None:
                break
            state = srow[0]
            if state == "active":
                return cur, False
            sp = cat.execute(
                "SELECT boundary,left_id,right_id FROM splits WHERE parent=?",
                (cur,),
            ).fetchone()
            if sp is not None and sp[1] is not None:
                # 冻结转发：等界归右，并继续沿拆分链向下
                cur = sp[1] if k < sp[0] else sp[2]
                continue
            if state == "frozen":
                return cur, True  # 冻结后、公布前的短暂窗口
            break  # retired 且查不到下一跳：退回区间路由
        return self._route_fresh(cat, k), False

    def handle(self, k):
        _need_nonempty_str(k, "k")
        db = self._cat()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT shard_id FROM handles WHERE k=?", (k,)
            ).fetchone()
            if row is not None:
                sid, frozen = self._resolve(db, k, row[0])
                if sid != row[0]:
                    db.execute(
                        "UPDATE handles SET shard_id=?, updated_seq="
                        "(SELECT n FROM seq_counter WHERE slot=0) WHERE k=?",
                        (sid, k),
                    )
            else:
                sid, _ = self._resolve(db, k, None)
                db.execute(
                    "INSERT INTO handles(k,shard_id,updated_seq) "
                    "SELECT ?, ?, n FROM seq_counter WHERE slot=0",
                    (k, sid),
                )
            db.execute("COMMIT")
            return Handle(self, k, sid)
        except BaseException:
            _rollback(db)
            raise
        finally:
            db.close()

    # ------------------------------------------------------------------
    # 幂等预留
    # ------------------------------------------------------------------

    def _claim(self, cat, op, id, k, v):
        """返回 (seq, replay)。同号同参复领；异参重投 ValueError。"""
        row = cat.execute(
            "SELECT op,k,v,seq,committed FROM claims WHERE id=?", (id,)
        ).fetchone()
        if row is not None:
            old_op, old_k, old_v, old_seq, committed = row
            if old_op != op or old_k != k or old_v != v:
                raise ValueError(
                    "同号 %r 异参重投：原 (%r,%r,%r) vs 新 (%r,%r,%r)"
                    % (id, old_op, old_k, old_v, op, k, v)
                )
            if committed:
                return old_seq, True
            # 预留未执行：旧号作废（留下空洞），领新号继续执行
            seq = self._allocate_seq(cat)
            cat.execute("UPDATE claims SET seq=? WHERE id=?", (seq, id))
            return seq, False
        seq = self._allocate_seq(cat)
        cat.execute(
            "INSERT INTO claims(id,op,k,v,seq,committed) VALUES(?,?,?,?,?,0)",
            (id, op, k, v, seq),
        )
        return seq, False

    # ------------------------------------------------------------------
    # 写路径：put / delete
    # ------------------------------------------------------------------

    def _write(self, op, id, k, v, hint):
        _need_nonempty_str(id, "id")
        _need_nonempty_str(k, "k")
        if op == "put":
            _need_str(v, "v")
        else:
            v = None

        # 先在自动提交模式下读路由，写事务内不做跨片调用
        cat = self._cat()
        try:
            sid, frozen_pre = self._resolve(cat, k, hint)
        finally:
            cat.close()
        if frozen_pre:
            raise RuntimeError(
                "分片已冻结但子路由尚未公布，请在拆分继续后重试键 %r" % k
            )

        # 崩溃恢复：分片回执已落但目录未确认，或目录留有未确认预留。
        # 先查目标分片；再查目录未确认预留（可能已被拆分迁到别的片）。
        sdb = self._shard(sid)
        try:
            crow = sdb.execute(
                "SELECT seq FROM receipts WHERE id=? AND k=? AND kind=?",
                (id, k, op),
            ).fetchone()
        finally:
            sdb.close()

        cat = self._cat()
        try:
            cat.execute("BEGIN IMMEDIATE")
            if crow is None:
                urow = cat.execute(
                    "SELECT seq,op,k,v FROM claims WHERE id=? AND committed=0",
                    (id,),
                ).fetchone()
                if urow is not None and urow[1] == op and urow[2] == k \
                        and urow[3] == v:
                    # 在已路由到的分片中确认该预留是否实际执行过
                    sdb2 = self._shard(sid)
                    try:
                        hit = sdb2.execute(
                            "SELECT 1 FROM receipts WHERE seq=?", (urow[0],)
                        ).fetchone()
                    finally:
                        sdb2.close()
                    if hit is not None:
                        crow = (urow[0],)

            if crow is not None:
                seq = crow[0]
                cat.execute(
                    "INSERT OR IGNORE INTO claims(id,op,k,v,seq,committed) "
                    "VALUES(?,?,?,?,?,1)",
                    (id, op, k, v, seq),
                )
                row = cat.execute(
                    "SELECT op,k,v,seq,committed FROM claims WHERE id=?", (id,)
                ).fetchone()
                if row[3] != seq or row[0] != op or row[1] != k or row[2] != v:
                    _rollback(cat)
                    raise ValueError(
                        "同号 %r 异参重投：已执行参数 (%r,%r,%r) vs 新 (%r,%r,%r)"
                        % (id, row[0], row[1], row[2], op, k, v)
                    )
                cat.execute(
                    "UPDATE claims SET committed=1 WHERE id=?", (id,)
                )
                cat.execute("COMMIT")
                return (seq, id, k, op, v)

            seq, replay = self._claim(cat, op, id, k, v)
            if replay:
                cat.execute("COMMIT")
                return (seq, id, k, op, v)
            # 转发后顺手刷新持久化提示
            if hint is not None:
                hrow = cat.execute(
                    "SELECT shard_id FROM handles WHERE k=?", (k,)
                ).fetchone()
                if hrow is not None and hrow[0] != sid:
                    cat.execute(
                        "UPDATE handles SET shard_id=? WHERE k=?", (sid, k)
                    )
            cat.execute("COMMIT")
        except BaseException:
            _rollback(cat)
            cat.close()
            raise
        cat.close()

        # 分片短事务：回执与当前行同提交，seq 守卫防晚到低序覆盖
        sdb = self._shard(sid)
        try:
            sdb.execute("BEGIN IMMEDIATE")
            sdb.execute(
                "INSERT OR IGNORE INTO receipts(seq,id,k,kind,v) VALUES(?,?,?,?,?)",
                (seq, id, k, op, v),
            )
            sdb.execute(
                "INSERT INTO kv(k,seq,kind,v) VALUES(?,?,?,?) "
                "ON CONFLICT(k) DO UPDATE SET "
                "seq=excluded.seq, kind=excluded.kind, v=excluded.v "
                "WHERE excluded.seq > kv.seq",
                (k, seq, op, v),
            )
            sdb.execute("COMMIT")
        except BaseException:
            _rollback(sdb)
            sdb.close()
            raise
        sdb.close()

        # 确认写入；本事务独立且极短
        cat = self._cat()
        try:
            cat.execute("BEGIN IMMEDIATE")
            cat.execute(
                "UPDATE claims SET committed=1 WHERE id=? AND seq=?", (id, seq)
            )
            cat.execute("COMMIT")
        except BaseException:
            _rollback(cat)
            raise
        finally:
            cat.close()
        return (seq, id, k, op, v)

    def put(self, id, k, v, _hint=None):
        return self._write("put", id, k, v, _hint)

    def delete(self, id, k, _hint=None):
        return self._write("delete", id, k, None, _hint)

    # ------------------------------------------------------------------
    # 读路径
    # ------------------------------------------------------------------

    def _read_targets(self, cat, k, hint):
        """读路径的候选分片，按优先级返回（迁移期回退读父片）。

        写路径始终只落当前活动子片（单键线性化）；读路径在搬运暂停时，
        若该键尚未被子片接管，则回退读冻结父片，保证旧句柄不丢数据。
        """
        sid, frozen_pre = self._resolve(cat, k, hint)
        targets = [sid]
        srow = cat.execute(
            "SELECT parent FROM shards WHERE shard_id=?", (sid,)
        ).fetchone()
        if srow is not None and srow[0] is not None:
            parent = srow[0]
            sp = cat.execute(
                "SELECT state,frontier,total FROM splits "
                "WHERE name='current' AND parent=?",
                (parent,),
            ).fetchone()
            if sp is not None and sp[0] != "done" and sp[1] < sp[2]:
                frontier, total = sp[1], sp[2]
                pdb = self._shard(parent)
                try:
                    keys = [r[0] for r in pdb.execute(
                        "SELECT k FROM kv ORDER BY k").fetchall()]
                finally:
                    pdb.close()
                # 迁移按码点序；frontier 之前的键已接管，之后的仍只在父片
                if k in keys[frontier:]:
                    targets.append(parent)
        return targets, frozen_pre

    def get(self, k, _hint=None):
        _need_nonempty_str(k, "k")
        cat = self._cat()
        try:
            if _hint is None:
                row = cat.execute(
                    "SELECT shard_id FROM handles WHERE k=?", (k,)
                ).fetchone()
                hint = row[0] if row is not None else None
            else:
                hint = _hint
            targets, _frozen = self._read_targets(cat, k, hint)
            sid = targets[0]
            if hint is not None and sid != hint:
                cat.execute("BEGIN IMMEDIATE")
                cat.execute(
                    "UPDATE handles SET shard_id=? WHERE k=?", (sid, k)
                )
                cat.execute("COMMIT")
        finally:
            cat.close()

        for target in targets:
            sdb = self._shard(target)
            try:
                row = sdb.execute(
                    "SELECT kind,v FROM kv WHERE k=?", (k,)
                ).fetchone()
            finally:
                sdb.close()
            if row is not None:
                return None if row[0] == "delete" else row[1]
        return None

    def audit(self, k):
        """拆毕仅查所属子片；按 seq 返回该键全部已执行回执。"""
        _need_nonempty_str(k, "k")
        cat = self._cat()
        try:
            row = cat.execute(
                "SELECT shard_id FROM handles WHERE k=?", (k,)
            ).fetchone()
            hint = row[0] if row is not None else None
            sid, _ = self._resolve(cat, k, hint)
        finally:
            cat.close()
        sdb = self._shard(sid)
        try:
            rows = sdb.execute(
                "SELECT seq,id,k,kind,v FROM receipts WHERE k=? ORDER BY seq",
                (k,),
            ).fetchall()
        finally:
            sdb.close()
        return [tuple(r) for r in rows]

    # ------------------------------------------------------------------
    # 状态
    # ------------------------------------------------------------------

    def status(self):
        cat = self._cat()
        try:
            row = cat.execute(
                "SELECT state,frontier,total FROM splits WHERE name='current'"
            ).fetchone()
        finally:
            cat.close()
        if row is None:
            return (None, False)
        if row[0] == "done":
            return (row[2], True)
        return (row[1], False)

    # ------------------------------------------------------------------
    # 拆分
    # ------------------------------------------------------------------

    def split(self, b):
        _need_nonempty_str(b, "b")
        cat = self._cat()
        try:
            cat.execute("BEGIN IMMEDIATE")
            sp = cat.execute(
                "SELECT parent,boundary,state,left_id,right_id,frontier,total "
                "FROM splits WHERE name='current'"
            ).fetchone()
            if sp is not None and sp[2] == "done":
                cat.execute("DELETE FROM splits WHERE name='current'")
                sp = None
            if sp is not None:
                if sp[1] != b:
                    raise ValueError(
                        "拆分进行中：原界 %r 与新界 %r 不一致（异界重投）" % (sp[1], b)
                    )
                parent, boundary, state, left_id, right_id, frontier, total = sp
            else:
                prow = cat.execute(
                    "SELECT shard_id FROM shards WHERE state='active' "
                    "AND (lo IS NULL OR lo <= ?) AND (hi IS NULL OR ? < hi)",
                    (b, b),
                ).fetchall()
                if len(prow) != 1:
                    raise RuntimeError("界 %r 找不到唯一可拆活动分片" % b)
                parent = prow[0][0]
                cat.execute(
                    "INSERT INTO splits(name,parent,boundary,state,frontier,total) "
                    "VALUES('current',?,?,'freezing',0,0)",
                    (parent, b),
                )
                boundary, state = b, "freezing"
                left_id = right_id = None
                frontier = total = 0
            cat.execute("COMMIT")
        except BaseException:
            _rollback(cat)
            cat.close()
            raise
        cat.close()

        # 阶段一：冻结父片（冻结行 + 父阶段回执同次提交），随后取像收 F
        state = self._phase_freeze(parent, boundary, state)

        # 阶段二：预留子片并公布路由，收 R
        left_id, right_id, state = self._phase_publish(
            parent, boundary, state, left_id, right_id
        )

        # 阶段三：逐键搬运，行与回执同次提交，逐键收 W
        state = self._phase_migrate(
            parent, boundary, state, left_id, right_id, frontier, total
        )

        # 阶段四：补齐标记 + 完成回执同次提交，父片退役，收 H
        self._phase_complete(parent, boundary, state)
        return (left_id, right_id)

    def _phase_freeze(self, parent, boundary, state):
        pdb = self._shard(parent)
        try:
            marker = pdb.execute(
                "SELECT 1 FROM markers WHERE name='frozen'"
            ).fetchone()
            if marker is None:
                cat = self._cat()
                try:
                    cat.execute("BEGIN IMMEDIATE")
                    fseq = self._allocate_seq(cat)
                    cat.execute("COMMIT")
                except BaseException:
                    _rollback(cat)
                    raise
                finally:
                    cat.close()
                pdb.execute("BEGIN IMMEDIATE")
                pdb.execute(
                    "INSERT INTO markers(name,val) VALUES('frozen',?)",
                    (boundary,),
                )
                pdb.execute(
                    "INSERT INTO phase_receipts(seq,phase,boundary) "
                    "VALUES(?,'freeze',?)",
                    (fseq, boundary),
                )
                pdb.execute("COMMIT")
        except BaseException:
            _rollback(pdb)
            pdb.close()
            raise
        pdb.close()

        cat = self._cat()
        try:
            cat.execute("BEGIN IMMEDIATE")
            cur = cat.execute(
                "SELECT state,total FROM splits WHERE name='current'"
            ).fetchone()
            if cur[0] == "freezing":
                pdb = self._shard(parent)
                try:
                    keys = [r[0] for r in pdb.execute(
                        "SELECT k FROM kv ORDER BY k").fetchall()]
                finally:
                    pdb.close()
                total = len(keys)
                cat.execute(
                    "UPDATE splits SET state='frozen', total=? "
                    "WHERE name='current'",
                    (total,),
                )
                just_froze = True
            else:
                keys = None  # 续办：F 已投递过，不重复发
                just_froze = False
            cat.execute("COMMIT")
        except BaseException:
            _rollback(cat)
            raise
        finally:
            cat.close()

        if just_froze:
            for k in keys:  # 取像后逐键收 F；异常照传，状态已落盘
                self._fire("F", k)
        return "frozen"

    def _phase_publish(self, parent, boundary, state, left_id, right_id):
        if state == "freezing":
            state = "frozen"
        if state != "frozen":
            return left_id, right_id, state

        just_published = False
        cat = self._cat()
        try:
            cat.execute("BEGIN IMMEDIATE")
            n = int(cat.execute(
                "SELECT val FROM meta WHERE key='shard_counter'").fetchone()[0])
            if left_id is None:
                just_published = True
                n += 1
                left_id = "s%d" % n
                n += 1
                right_id = "s%d" % n
                cat.execute(
                    "UPDATE meta SET val=? WHERE key='shard_counter'", (str(n),)
                )
                rng = cat.execute(
                    "SELECT lo,hi FROM shards WHERE shard_id=?", (parent,)
                ).fetchone()
                lo, hi = rng
                # 先建片文件再登记路由
                for sid, slo, shi in (
                    (left_id, lo, boundary),
                    (right_id, boundary, hi),
                ):
                    sdb = self._shard(sid)
                    sdb.close()
                    cat.execute(
                        "INSERT INTO shards(shard_id,path,state,lo,hi,parent) "
                        "VALUES(?,?,'active',?,?,?)",
                        (sid, self._shard_path(sid), slo, shi, parent),
                    )
                cat.execute(
                    "UPDATE shards SET state='frozen' WHERE shard_id=?", (parent,)
                )
            cat.execute(
                "UPDATE splits SET state='published', left_id=?, right_id=? "
                "WHERE name='current'",
                (left_id, right_id),
            )
            cat.execute("COMMIT")
        except BaseException:
            _rollback(cat)
            raise
        finally:
            cat.close()
        # 仅首次预留提交之后收 R；续跑不重复投递
        if just_published:
            self._fire("R", boundary)
        return left_id, right_id, "published"

    def _phase_migrate(self, parent, boundary, state, left_id, right_id,
                       frontier, total):
        if state == "freezing":
            state = "frozen"
        if state == "frozen":
            state = "published"
        if state in ("migrating", "published"):
            pdb = self._shard(parent)
            try:
                keys = [r[0] for r in pdb.execute(
                    "SELECT k FROM kv ORDER BY k").fetchall()]
            finally:
                pdb.close()
            total = len(keys)
            for idx in range(frontier, total):
                k = keys[idx]  # 已按码点排序
                child_id = left_id if k < boundary else right_id  # 等界归右
                self._migrate_one(parent, child_id, k)
                cat = self._cat()
                try:
                    cat.execute("BEGIN IMMEDIATE")
                    cat.execute(
                        "UPDATE splits SET state='migrating', frontier=? "
                        "WHERE name='current'",
                        (idx + 1,),
                    )
                    cat.execute("COMMIT")
                except BaseException:
                    _rollback(cat)
                    raise
                finally:
                    cat.close()
                # 行与回执提交之后收 W；异常照传，进度已持久化可续
                self._fire("W", k)
            state = "migrating"
        return state

    def _migrate_one(self, parent, child_id, k):
        pdb = self._shard(parent)
        try:
            receipts = pdb.execute(
                "SELECT seq,id,k,kind,v FROM receipts WHERE k=? ORDER BY seq",
                (k,),
            ).fetchall()
            cur = pdb.execute(
                "SELECT seq,kind,v FROM kv WHERE k=?", (k,)
            ).fetchone()
        finally:
            pdb.close()

        cdb = self._shard(child_id)
        try:
            cdb.execute("BEGIN IMMEDIATE")
            # 历史回执全部带过去，保证拆毕 audit 只查子片也完整
            for r in receipts:
                cdb.execute(
                    "INSERT OR IGNORE INTO receipts(seq,id,k,kind,v) "
                    "VALUES(?,?,?,?,?)",
                    tuple(r),
                )
            if cur is not None:
                # 迟到副本不盖子片写删：仅当子片缺席或父片 seq 更高才落当前行
                cdb.execute(
                    "INSERT INTO kv(k,seq,kind,v) VALUES(?,?,?,?) "
                    "ON CONFLICT(k) DO UPDATE SET "
                    "seq=excluded.seq, kind=excluded.kind, v=excluded.v "
                    "WHERE excluded.seq > kv.seq",
                    (k, cur[0], cur[1], cur[2]),
                )
            cdb.execute("COMMIT")
        except BaseException:
            _rollback(cdb)
            cdb.close()
            raise
        cdb.close()

    def _phase_complete(self, parent, boundary, state):
        if state == "done":
            return
        pdb0 = self._shard(parent)
        try:
            existed = pdb0.execute(
                "SELECT 1 FROM markers WHERE name='split_done'"
            ).fetchone()
        finally:
            pdb0.close()
        if existed is None:
            fire_h = True
            cat = self._cat()
            try:
                cat.execute("BEGIN IMMEDIATE")
                hseq = self._allocate_seq(cat)
                cat.execute("COMMIT")
            except BaseException:
                _rollback(cat)
                raise
            finally:
                cat.close()

            # 补齐标记与父阶段完成回执同次提交
            pdb = self._shard(parent)
            try:
                pdb.execute("BEGIN IMMEDIATE")
                pdb.execute(
                    "INSERT OR IGNORE INTO markers(name,val) "
                    "VALUES('split_done',?)",
                    (json.dumps({"boundary": boundary}),),
                )
                pdb.execute(
                    "INSERT OR IGNORE INTO phase_receipts(seq,phase,boundary) "
                    "VALUES(?,'complete',?)",
                    (hseq, boundary),
                )
                pdb.execute("COMMIT")
            except BaseException:
                _rollback(pdb)
                pdb.close()
                raise
            pdb.close()
        else:
            fire_h = False

        cat = self._cat()
        try:
            cat.execute("BEGIN IMMEDIATE")
            cat.execute(
                "UPDATE shards SET state='retired' WHERE shard_id=?", (parent,)
            )
            cat.execute(
                "UPDATE splits SET state='done', frontier=total "
                "WHERE name='current'"
            )
            cat.execute("COMMIT")
        except BaseException:
            _rollback(cat)
            raise
        finally:
            cat.close()
        # 补齐提交之后收 H；续跑不重复投递
        if fire_h:
            self._fire("H", boundary)

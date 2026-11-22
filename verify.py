# -*- coding: utf-8 -*-
"""handoff.Lake 的实测回归：以真实文件、真实 argv 子进程验证全部题面保证。

运行：python3 verify.py
任何检查失败以非零退出码退出。
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import handoff  # noqa: E402

FAILURES = []


def check(cond, msg):
    if cond:
        print("  ok - %s" % msg)
    else:
        FAILURES.append(msg)
        print("FAIL - %s" % msg)


def fresh_lake(hook=None):
    d = tempfile.mkdtemp(prefix="lake-verify-")
    return handoff.Lake(d, hook=hook), d


def test_basic():
    print("[1] 基本读写 / 墓碑 / 缺席返 None / 校验")
    lake, d = fresh_lake()
    r = lake.put("id1", "a", "7")
    check(r == (1, "id1", "a", "put", "7"), "put 返回 (seq,id,k,kind,v)：%r" % (r,))
    check(lake.get("a") == "7", "get 命中写值")
    check(lake.get("missing") is None, "缺席键 get 返 None")

    r2 = lake.delete("id2", "a")
    check(r2 == (2, "id2", "a", "delete", None), "delete 返 v=None：%r" % (r2,))
    check(lake.get("a") is None, "墓碑后 get 返 None（删缺键也留墓碑）")
    r3 = lake.delete("id3", "a")  # 删缺键
    check(r3[0] == 3 and r3[4] is None, "再删墓碑键成功且 v=None")
    r4 = lake.put("id4", "a", "")
    check(r4 == (4, "id4", "a", "put", ""), "空串是合法值")

    # 同号同参复领（同号不增 seq）
    r5 = lake.put("id1", "a", "7")
    check(r5 == r, "同号同参复领原回执、不领新号：%r" % (r5,))
    # 异参重投
    for bad in (
        lambda: lake.put("id1", "a", "8"),
        lambda: lake.put("id1", "b", "7"),
        lambda: lake.delete("id1", "a"),
    ):
        try:
            bad()
            check(False, "异参重投应抛 ValueError")
        except ValueError:
            check(True, "异参重投抛 ValueError")

    # 坏参
    bad_args = [
        lambda: lake.put("", "a", "x"), lambda: lake.put("x", "", "x"),
        lambda: lake.put("x", "a", 7), lambda: lake.put("x", "a", None),
        lambda: lake.delete("x", ""), lambda: lake.get(""),
        lambda: lake.split(""), lambda: lake.audit(""),
    ]
    for fn in bad_args:
        try:
            fn()
            check(False, "坏参应抛 ValueError")
        except ValueError:
            check(True, "坏参抛 ValueError")

    # 代理对（无法 UTF-8 编码）拒绝
    try:
        lake.put("x", "a", "sur:\ud800")
        check(False, "代理对应拒绝")
    except ValueError:
        check(True, "非合法 UTF-8 字符串拒绝")

    # seq 全表递增
    seq = lake.put("id5", "z", "1")[0]
    check(seq == 5, "全局 seq 连续递增（=%d）" % seq)
    shutil.rmtree(d)


def test_hook_pause_resume():
    print("[2] hook F/R/W/H 顺序、异常照传、同界续办、异界 ValueError")
    d = tempfile.mkdtemp(prefix="lake-hook-")
    events = []

    def hook(e, k):
        events.append((e, k))

    lake = handoff.Lake(d, hook=hook)
    for i, k in enumerate(["a", "c", "m", "z"]):
        lake.put("p%d" % i, k, str(i))
    lake.split("m")
    kinds = [e for e, _ in events]
    check(kinds[0] == "F" and kinds[-2] == "W" and kinds[-1] == "H",
          "事件序列以 F 开头、H 收尾、中间有 W：%r" % kinds)
    check(("R", "m") in events, "R 以界键 m 触发一次")
    check(events.count(("R", "m")) == 1 and events.count(("H", "m")) == 1,
          "R/H 各一次")
    check([k for e, k in events if e == "F"] == ["a", "c", "m", "z"],
          "F 按码点序对全部冻结键取像")
    check([k for e, k in events if e == "W"] == ["a", "c", "m", "z"],
          "W 按码点序逐键提交")
    check(lake.status() == (4, True), "拆毕 status=(4,True)：%r" % (lake.status(),))

    # hook 异常照传，且可同界续办
    d2 = tempfile.mkdtemp(prefix="lake-pause-")
    state = {"stop": True}

    class Stop(handoff.SplitPaused):
        pass

    def hook2(e, k):
        if state["stop"] and e == "W" and k == "c":
            raise Stop("值班员主动暂停")

    lake2 = handoff.Lake(d2, hook=hook2)
    lake2.put("w1", "a", "1")
    lake2.put("w2", "c", "2")
    lake2.put("w3", "m", "3")
    try:
        lake2.split("m")
        check(False, "hook 异常应照传")
    except Stop:
        check(True, "hook 异常原样传播")
    check(lake2.status() == (2, False),
          "暂停时前沿已落盘（a、c 已提交）：%r" % (lake2.status(),))
    try:
        lake2.split("n")
        check(False, "异界重投应失败")
    except ValueError:
        check(True, "异界重投抛 ValueError")
    state["stop"] = False
    lake2.split("m")  # 同界续办
    check(lake2.status() == (3, True), "续办后完成：%r" % (lake2.status(),))
    check(lake2.get("a") == "1" and lake2.get("m") == "3", "续办后数据完整")
    shutil.rmtree(d)
    shutil.rmtree(d2)


def test_interleave():
    print("[3] 题面交错：暂停期旧句柄删 k、新入口写 m；续拆晚到不盖子片")
    d = tempfile.mkdtemp(prefix="lake-inter-")

    class Pause(handoff.SplitPaused):
        pass

    state = {"pause": True}

    def hook(e, k):
        if state["pause"] and e == "R":
            raise Pause("取 k 旧像后暂停（子路由已公布）")

    lake = handoff.Lake(d, hook=hook)
    lake.put("w7", "k", "7")          # 例：k 写 7
    old_handle = lake.handle("k")     # 旧句柄
    check(old_handle.get() == "7", "旧句柄读到旧像 7")
    try:
        lake.split("m")               # 按 m 拆，公布子路由后暂停
        check(False, "应暂停")
    except Pause:
        check(True, "拆分在预留提交后暂停")
    check(lake.status() == (0, False), "暂停期前沿为 0：%r" % (lake.status(),))

    # 搬运暂停时旧句柄及新入口仍可读写；未迁键回退读冻结父片不丢数据
    check(old_handle.get() == "7", "暂停期旧句柄经父片回退读到旧像 7")
    check(lake.get("k") == "7", "暂停期新入口也能读到未迁键 7")
    r_del = old_handle.delete("old-del")   # 旧句柄删 k（冻结转发）
    check(r_del[2] == "k" and r_del[4] is None, "旧句柄删除经冻结转发落子片")
    check(old_handle.get() is None, "旧句柄随即读到墓碑")
    r_new = lake.put("new-w", "m", "11")        # 新入口写 m=11
    check(r_new[3] == "put" and r_new[4] == "11", "新入口直接写活动子片")
    check(lake.get("m") == "11", "新入口读到 11")

    state["pause"] = False
    lake.split("m")  # 续拆：k 缺席（当前行）但有写删回执，m 归右
    check(lake.status() == (1, True),
          "拆毕 status=(1,True)，当前行只剩 m：%r" % (lake.status(),))
    check(lake.get("k") is None, "k 墓碑未被父片旧像复活（防拆分复活删行）")
    check(lake.get("m") == "11", "m=11 归右且保留")

    # audit 仅查所属子片、按 seq 返全部已执行回执
    ka = lake.audit("k")
    kinds_k = [x[3] for x in ka]
    check(kinds_k == ["put", "delete"], "k 的回执含 put+delete：%r" % (ka,))
    check([x[0] for x in ka] == sorted(x[0] for x in ka), "回执按 seq 升序")
    ma = lake.audit("m")
    check([x[3] for x in ma] == ["put"] and ma[0][4] == "11",
          "m 的子片回执为 put 11：%r" % (ma,))

    # 低序晚到不盖高序：直接模拟父片旧副本迟到
    cat = lake._cat()
    rows = cat.execute(
        "SELECT boundary,left_id,right_id FROM splits WHERE name='current'"
    ).fetchone()
    cat.close()
    left_id, right_id = rows[1], rows[2]
    # 给右片硬塞一条更低 seq 的 k=旧值，模拟迟到副本
    rdb = lake._shard(right_id)
    rdb.execute("BEGIN IMMEDIATE")
    rdb.execute(
        "INSERT INTO kv(k,seq,kind,v) VALUES('k',1,'put','7') "
        "ON CONFLICT(k) DO UPDATE SET seq=excluded.seq,kind=excluded.kind,"
        "v=excluded.v WHERE excluded.seq>kv.seq"
    )
    rdb.execute("COMMIT")
    rdb.close()
    check(lake.get("k") is None, "低序晚到副本未覆盖高序墓碑")
    shutil.rmtree(d)


def test_boundary_and_tombstone():
    print("[4] 等界归右 / 墓碑搬运 / 空湖拆分 / 嵌套拆分与级联句柄")
    d = tempfile.mkdtemp(prefix="lake-bnd-")
    lake = handoff.Lake(d)
    for k, v in [("a", "1"), ("b", "2"), ("m", "3"), ("n", "4")]:
        lake.put("k-" + k, k, v)
    lake.delete("k-del", "b")  # 墓碑也要被搬
    l, r = lake.split("m")     # a,b -> 左；m,n -> 右（等界归右）
    cat = lake._cat()
    lr = cat.execute("SELECT lo,hi FROM shards WHERE shard_id=?", (l,)).fetchone()
    rr = cat.execute("SELECT lo,hi FROM shards WHERE shard_id=?", (r,)).fetchone()
    cat.close()
    check(lr == (None, "m") and rr == ("m", None), "区间 [lo,m) 与 [m,hi)：%r %r" % (lr, rr))
    check(lake.get("a") == "1" and lake.get("m") == "3", "左右片数据可读")
    check(lake.get("b") is None, "墓碑随键搬到左片仍为 None")
    check([x[3] for x in lake.audit("b")] == ["put", "delete"],
          "墓碑键历史回执完整：%r" % (lake.audit("b"),))

    # 旧句柄在拆毕后级联到子片
    hb = lake.handle("b")
    check(hb.get() is None, "旧句柄读墓碑正常")
    hb.put("b-revive", "9")
    check(hb.get() == "9", "旧句柄写经转发落到左片")
    # 嵌套拆分右片
    lake.split("n")  # m -> 新左，n -> 新右
    check(lake.get("m") == "3" and lake.get("n") == "4", "嵌套拆分后数据仍可读")
    hn = lake.handle("n")
    check(hn.get() == "4", "句柄沿两级拆分链级联路由")
    # 二级拆分的父片是原右片，其冻结键为 m、n
    check(lake.status() == (2, True), "二级拆毕 status：%r" % (lake.status(),))

    # 空湖拆分：无冻结键，F 不发，R/H 仍发
    d2 = tempfile.mkdtemp(prefix="lake-empty-")
    ev = []
    lake2 = handoff.Lake(d2, hook=lambda e, k: ev.append(e))
    lake2.split("x")
    check(ev == ["R", "H"], "空湖拆分事件仅 R/H：%r" % ev)
    check(lake2.status() == (0, True), "空湖拆毕 (0,True)")
    shutil.rmtree(d)
    shutil.rmtree(d2)


def test_linearization():
    print("[5] 单键线性化：同键串行写的 seq 与可见值一致")
    d = tempfile.mkdtemp(prefix="lake-lin-")
    lake = handoff.Lake(d)
    last = 0
    for i in range(20):
        r = lake.put("lin%d" % i, "hot", "v%d" % i)
        check(r[0] > last, "seq 严格递增 #%d" % i)
        last = r[0]
        check(lake.get("hot") == "v%d" % i, "最新写立即可见 #%d" % i)
    audit = lake.audit("hot")
    check([x[4] for x in audit] == ["v%d" % i for i in range(20)],
          "回执顺序与写入顺序一致")
    # 预留不算执行：手工制造未确认预留后，audit 不含它
    cat = lake._cat()
    cat.execute("BEGIN IMMEDIATE")
    seq = lake._allocate_seq(cat)
    cat.execute(
        "INSERT INTO claims(id,op,k,v,seq,committed) VALUES('ghost','put',"
        "'hot','ghost',?,0)", (seq,))
    cat.execute("COMMIT")
    cat.close()
    check(all(x[1] != "ghost" for x in lake.audit("hot")), "未确认预留不进回执")
    r = lake.put("ghost", "hot", "ghost")  # 续领新号后执行
    check(r[0] != seq and lake.get("hot") == "ghost",
          "预留续办领新号并成功执行")
    shutil.rmtree(d)


def test_reopen_subprocess():
    print("[6] 重开保确认写入 + 跨进程 argv 续拆（真实子进程实测）")
    here = os.path.dirname(os.path.abspath(__file__))
    d = tempfile.mkdtemp(prefix="lake-proc-")
    env = dict(os.environ, PYTHONPATH=here, PYTHONIOENCODING="utf-8")

    def run(*args):
        p = subprocess.run(
            [sys.executable, "-I", os.path.join(here, "demo.py"), d] + list(args),
            capture_output=True, text=True, env=env,
        )
        if p.returncode != 0:
            print(p.stdout)
            print(p.stderr)
            raise AssertionError("子进程失败: %s" % (args,))
        return json.loads(p.stdout)

    out = run("worker", "w-a", "put", "a", "1")
    check(out["result"][0] == 1, "子进程 put 成功：%r" % out["result"])
    run("worker", "w-m", "put", "m", "11")
    run("worker", "w-z", "put", "z", "99")
    # 新进程重开：确认写入仍在
    check(run("worker", "w-a", "get", "a")["result"] == "1", "重开后读回 a=1")

    # 跨进程拆分暂停（R 处），再由另一进程同界续办
    p = subprocess.run(
        [sys.executable, "-I", os.path.join(here, "demo.py"), d,
         "split-pause", "m"],
        capture_output=True, text=True, env=env,
    )
    check(p.returncode != 0, "暂停子进程以非零退出传播 hook 异常")
    st = run("status")
    check(st["status"] == [0, False], "暂停状态落盘：%r" % st["status"])

    # 暂停期两个独立进程分别经旧句柄 / 新入口读写
    run("handle", "k", "put", "w-k-7", "7")
    run("handle", "k", "delete", "old-del-k")
    run("worker", "w-m2", "put", "m", "22")
    check(run("worker", "w-m2", "get", "m")["result"] == "22",
          "暂停期新入口写 m=22 可读")

    out = run("split-resume", "m")
    check(out["result"] == "done", "跨进程同界续办完成：%r" % out)
    check(run("status")["status"] == [3, True], "最终 status=(3,True)")
    check(run("worker", "x", "get", "a")["result"] == "1", "拆毕 a=1")
    check(run("worker", "x", "get", "m")["result"] == "22", "拆毕 m=22（归右）")
    check(run("worker", "x", "get", "z")["result"] == "99", "拆毕 z=99（归右）")
    check(run("worker", "x", "get", "k")["result"] is None,
          "k 的删除未被复活")
    ka = run("worker", "x", "audit", "k")["result"]
    check([x[3] for x in ka] == ["put", "delete"],
          "跨进程下 k 的回执仍完整：%r" % ka)
    shutil.rmtree(d)


def main():
    tests = [
        test_basic, test_hook_pause_resume, test_interleave,
        test_boundary_and_tombstone, test_linearization, test_reopen_subprocess,
    ]
    for t in tests:
        try:
            t()
        except Exception:
            FAILURES.append(t.__name__ + " 抛异常")
            traceback.print_exc()
    print("\n==== %s：%d 项失败 ====" % (
        "全部通过" if not FAILURES else "有失败", len(FAILURES)))
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())

# -*- coding: utf-8 -*-
"""handoff.Lake 的命令行演示 / 跨进程入口（argv 实测）。

用法：
  demo.py ROOT worker ID put K V      新入口写
  demo.py ROOT worker ID delete K     新入口删
  demo.py ROOT worker ID get K        新入口读
  demo.py ROOT worker ID audit K      新入口查回执
  demo.py ROOT handle K put ID V      旧句柄写（路由提示持久化在目录库）
  demo.py ROOT handle K delete ID     旧句柄删
  demo.py ROOT handle K get           旧句柄读
  demo.py ROOT split-pause B          按界 B 拆，R 后主动暂停（非零退出）
  demo.py ROOT split-resume B         同界续拆并完成
  demo.py ROOT status                 打印 (前沿, 完成否)
  demo.py ROOT scenario               单进程完整演示题面交错故事
所有正常结果以单行 JSON 打印；暂停类异常以非零退出码传播。
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import handoff  # noqa: E402


class PauseAtR(handoff.SplitPaused):
    pass


def pause_hook(event, key):
    if event == "R":
        raise PauseAtR("预留提交、子路由已公布，值班员在此暂停搬运")


def emit(obj):
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True))


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 2
    root, cmd = argv[0], argv[1]
    args = argv[2:]

    if cmd == "scenario":
        return scenario(root)

    lake = handoff.Lake(root)

    if cmd == "worker":
        wid, op, k = args[0], args[1], args[2]
        if op == "put":
            emit({"result": lake.put(wid, k, args[3])})
        elif op == "delete":
            emit({"result": lake.delete(wid, k)})
        elif op == "get":
            emit({"result": lake.get(k)})
        elif op == "audit":
            emit({"result": lake.audit(k)})
        else:
            raise ValueError("未知 op %r" % op)
        return 0

    if cmd == "handle":
        k = args[0]
        sub = args[1]
        h = lake.handle(k)
        if sub == "put":
            emit({"result": h.put(args[2], args[3])})
        elif sub == "delete":
            emit({"result": h.delete(args[2])})
        elif sub == "get":
            emit({"result": h.get()})
        else:
            raise ValueError("未知 handle 子命令 %r" % sub)
        return 0

    if cmd == "split-pause":
        lake.hook = pause_hook
        lake.split(args[0])  # 预期抛 PauseAtR
        return 0

    if cmd == "split-resume":
        emit({"result": "done", "children": lake.split(args[0])})
        return 0

    if cmd == "status":
        emit({"status": list(lake.status())})
        return 0

    print(__doc__)
    return 2


def scenario(root):
    """单进程完整演示：k 写 7 → 按 m 拆并暂停 → 旧删新写 → 续拆归位。"""
    events = []

    def hook(e, k):
        events.append((e, k))

    lake = handoff.Lake(root, hook=hook)

    # 1) 例 k 写 7
    r1 = lake.put("w-k-7", "k", "7")
    print("1. put k=7            ->", r1)
    old = lake.handle("k")
    print("   旧句柄 handle('k') ->", old, "读 =", old.get())

    # 2) 按 m 拆：冻结取像 F、预留公布 R 之后暂停（hook 异常照传）
    state = {"pause": True}

    def hook2(e, k):
        events.append((e, k))
        if state["pause"] and e == "R":
            raise handoff.SplitPaused("取 k 旧像后暂停，子路由已公布")

    lake.hook = hook2
    try:
        lake.split("m")
    except handoff.SplitPaused as exc:
        print("2. split('m') 在 R 后暂停 ->", exc)
    print("   status =", lake.status(), "；旧句柄读 k =", old.get())

    # 3) 暂停期：旧句柄删 k，新入口写 m=11
    r_del = old.delete("old-del-k")
    print("3. 旧句柄 delete k    ->", r_del, "；再读 =", old.get())
    r_new = lake.put("new-w-m", "m", "11")
    print("   新入口 put m=11    ->", r_new, "；get(m) =", lake.get("m"))

    # 4) 同界续拆：k 当前行缺席但有写删回执；m 等界归右
    state["pause"] = False
    lake.hook = hook
    children = lake.split("m")
    print("4. 同界续拆完成       -> 子片", children, "status =", lake.status())
    print("   get(k) =", lake.get("k"), "（墓碑未被旧像复活）")
    print("   get(m) =", lake.get("m"), "（等界归右）")
    print("   audit(k) =", json.dumps(lake.audit("k"), ensure_ascii=False))
    print("   事件序列 =", events)
    emit({"ok": True, "status": list(lake.status()), "events": events})
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except handoff.SplitPaused as exc:
        sys.stderr.write("PAUSED: %s\n" % exc)
        sys.exit(3)

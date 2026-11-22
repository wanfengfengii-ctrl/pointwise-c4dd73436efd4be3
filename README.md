# handoff.Lake —— 遥测湖值班员的拆分交接

纯标准库（`sqlite3`）实现的可拆分、可暂停续办、防删行复活的键值湖。

## 接口

```python
from handoff import Lake, Handle, SplitPaused

lake = Lake(root, hook=None)          # root 为数据目录；hook(e, k) 可选
lake.put(id, k, v)                    # -> (seq, id, k, "put", v)
lake.delete(id, k)                    # -> (seq, id, k, "delete", None)，删缺键也留墓碑
lake.get(k)                           # 缺席或墓碑 -> None
lake.handle(k)                        # -> Handle，路由提示持久化在目录库
lake.split(b)                         # 按码点界 b 拆分，等界归右（k == b 进右片）
lake.status()                         # -> (前沿, 完成否)；无拆分时 (None, False)
lake.audit(k)                         # 拆毕仅查所属子片，按 seq 返已执行回执列表
```

* 键 `k`、`id`、界 `b`：非空合法 UTF-8 字符串；值 `v`：字符串（可空串）。
* 异界重投、异参（同 `id` 不同 op/k/v）重投、坏参一律 `ValueError`。
* `Handle` 提供 `.put/.delete/.get`，拆分后沿拆分链级联转发到当前子片。

## 关键保证

| 题面要求 | 实现方式 |
| --- | --- |
| 全局 seq 递增、单键线性化 | 目录库 `seq_counter` 单一分配点，短事务领号；分片 `kv` 以 `excluded.seq > kv.seq` 守卫，低序晚到不盖高序 |
| 拆分不复活删行 | 迁移只在父片 seq 更高时覆盖子片当前行；暂停期的删除落在子片，父片旧像无法复活墓碑 |
| 同号同参复领、预留不算执行 | 目录库 `claims` 记录预留与确认；未确认预留不进分片 `receipts`，续办领新号执行 |
| 父片先冻结转发再公布子路由 | 冻结标记+冻结回执同事务 → 预留子片+公布路由（R）→ 逐键迁移（W）→ 补齐标记+完成回执（H），父片退役保留源数据 |
| 暂停期旧句柄/新入口皆可读写 | 写只落活动子片；读在键未接管时回退读冻结父片 |
| 跨片调用与回调不持写事务 | 每个事务只触一个库且立即提交，hook 在提交之后调用 |
| hook 异常照传、可续拆 | F/R/W/H 均在对应提交后触发；异常传播而进度已落盘，同界重投 `split` 续办，事件不重复投递 |
| 重开保确认写入 | 所有状态在 SQLite（WAL）持久化；确认前崩溃按分片回执恢复或领新号重试 |

## 事件语义（hook 第二参为相关键，R/H 用界键）

* `F` 取像：父片冻结后对每个冻结键按码点序触发
* `R` 预留提交：子片建好、路由公布后触发一次
* `W` 行与回执提交：每键迁移提交后触发
* `H` 补齐提交：补齐标记与完成回执同事务后触发一次

## 目录布局

```
root/
  catalog.sqlite3      目录库：seq 分配、分片路由、句柄提示、预留、拆分状态
  shards/s0.sqlite3    分片库：kv 当前行（含墓碑）、receipts 回执、阶段回执、标记
  shards/s1.sqlite3 ...
```

## 运行

```bash
python3 -I verify.py    # 114 项实测，含真实子进程的暂停/续办交错，失败以非零退出
python3 -I demo.py <root> scenario   # 演示题面完整故事
```

运行清单见 `.pointwise-runtime.json`（v1 / cli，空 setup）。

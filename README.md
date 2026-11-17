# 水质数仓单实例 PITR 恢复服务

面向"值班员误删样品后，要取回删前核对时点数据、同时保留生产库后续写入"的场景。
基于 **FastAPI + PostgreSQL 16 + Docker Compose**：

- 生产源库可写样品库（样品号 / 站号 / pH），开启 **WAL 持续归档**；
- API 自动制作一份 **物理基准备份**（`pg_basebackup`，源库不停写）；
- 可随时登记 **命名核对点**（`pg_create_restore_point`）；
- 按核对点 **PITR 恢复到独立只读实例** 并查询样品；
- 恢复结果只含核对点前已提交的数据，**跨点后才提交的事务不会出现**；
- 查询只打恢复实例，**绝不用当前源库冒充恢复结果**；
- WAL 未归档齐备时任务显示等待、不开放目标查询；
- 失败或 API 重启后可按原核对点重试；一次只恢复一项；全程不改写生产库。
- **站点 pH 突变质控**：同站按样品 id 排序，首条可供数，与前条 pH 差值
  严格大于 2 标为待复核；判定与依据由源库触发器在样品变更的**原事务**内
  持久化；可查当前源库或指定已就绪恢复任务（核对点时点）的质控结果。

## 快速开始

```bash
docker compose up -d --build        # 启动源库 + API（首次自动做基准备份）
python3 scripts/demo.py             # 端到端演示：更正 / 误删 / 跨点事务 + 恢复校验
python3 scripts/demo_qc.py          # 端到端演示：站点 pH 突变质控（需全新环境）
```

Swagger 文档：<http://localhost:8000/docs>

演示脚本依次验证：

| 场景 | 操作 | 恢复校验 |
| --- | --- | --- |
| 更正 | `UPDATE` 修改 S-2026-0002 的 pH | cp1 中仍是旧值 6.95，cp3 中是新值 7.55 |
| 误删 | `DELETE` 删除 S-2026-0003 | cp1 中查得到，cp3 中已删除 |
| 跨点事务 | 事务在 cp3 前 `INSERT`、cp3 后才 `COMMIT` | cp3 恢复结果中**不含**该行 |
| 源库不受打扰 | 恢复进行期间向源库写入新样品 | 写入成功；恢复实例中也看不到该行（证明没拿源库冒充） |

## 架构

```
                 ┌──────────────────────────── compose ───────────────────────────┐
                 │                                                                  │
  写入/变更 ───▶ │  db (PostgreSQL 16, 可写)                                        │
                 │    archive_mode=on ──WAL 段(原子改名)──▶ [wal_archive 卷]         │
                 │                                          │ (只读挂载)             │
                 │  api (FastAPI + PG16 工具)               ▼                       │
                 │    ① pg_basebackup ──▶ [/backups/base]   核对 WAL 段齐备          │
                 │    ② pg_create_restore_point(核对点) ──▶ 源库（仅 WAL 元数据）    │
                 │    ③ 拷贝备份 + recovery.signal ──▶ [/recoveries/job-N]           │
                 │       restore_command 从归档重放 ─▶ pause@核对点 ─▶ 校验 ─▶ 提升  │
                 │    ④ 只读查询 ◀── 恢复实例 127.0.0.1:55433（独立数据目录）        │
                 │                                                                  │
                 └──────────────────────────────────────────────────────────────────┘
```

## 关键机制与需求对照

| 需求 | 实现 |
| --- | --- |
| 物理基准备份 + 持续归档 | `pg_basebackup -Fp -X stream`（自洽含备份期 WAL）；源库 `archive_command` 先写临时文件再原子 `mv`，API 只会看到完整 WAL 段 |
| 登记命名核对点 | `POST /api/checkpoints` → `pg_create_restore_point` + `pg_switch_wal`（促使该段立即归档） |
| 只含核对点前已提交数据 | `recovery_target_name` + `recovery_target_action=pause`，重放精确停在核对点；跨点后提交的事务不重放 |
| 不用源库冒充恢复 | 查询连接的是 `/recoveries/job-N` 下独立实例（端口 55433），响应中标注实例与 as-of LSN |
| WAL 未齐备显示等待 | 核对点 `waiting_wal` 状态；任务逐段核对"备份起点→核对点"全部 WAL 段，缺段时停留 `waiting_wal` 并列出缺失段 |
| 不把半库报成功 | pause 后校验 `pg_last_wal_replay_lsn() >= 核对点 LSN` 才 promote；再查 `samples`  sanity + `default_transaction_read_only=on` 才置 `ready`；任何异常 → `failed` + 日志 |
| 失败/重启可重试 | 任务状态落 SQLite；`POST /api/recoveries/{id}/retry` 按原点重试；API 重启时进行中任务自动重新排队、ready 实例自动拉起 |
| 一次只恢复一项 | 有进行中任务时 `POST /api/recoveries` 返回 409；新任务就绪前会把旧 ready 实例停掉并标记 `superseded` |
| 源库可写、不被改写 | 基准备份/恢复点均为在线只读操作；归档卷对 API 只读挂载；API 对源库唯一的写操作是运维显式触发的 `POST /api/qc/enable`（见下节） |

## 站点质控（pH 突变）

质控员要找某站 pH 突变样品、给分析组提供可供数记录。判定规则：

> 同站按样品 id 排序：**首条可供数**；其余与前一条 pH 差值**严格大于 2** 的标为**待复核**（差值恰好 2.00 不算突变）。

**启用（一次性，幂等）**：`POST /api/qc/enable` 在源库单事务内完成
建表（`public.sample_qc`）+ 同步触发器 + 已有样品回填（已有样品照此判定）。
这是 API 对源库**唯一**的写操作，且只在运维显式调用时发生；
启用前会 `LOCK TABLE` 短暂阻塞写入，保证回填不漏数。

**原事务内持久化**：启用后，生产库直接新增 / 更正 / 迁站 / 删除样品时，
BEFORE 触发器先取该站点的**事务级咨询锁**（迁站按站名排序取两把，防死锁），
AFTER 触发器在同一事务内重算"自身 + 受影响邻项（同站后继）"的判定与依据
（前值样品、前值 pH、差值、文字说明）写入 `sample_qc`：

- 质控记录随业务事务一起提交/回滚，**回滚不留错配**；
- 同站并发写入被咨询锁串行化，**并发不留错配**；
- 每条质控响应都附 `consistency` 字段：持久化记录 vs 按规则现算的逐样品核对结果。

**查询**：

- `GET /api/qc/stations/{station_no}` — 当前源库实时质控（明确标注非恢复结果）；
- `GET /api/recoveries/{id}/qc/stations/{station_no}` — 指定已就绪恢复任务的
  核对点时点质控。响应含每条样品的判定（可供数/待复核）、前值、依据，
  以及可供数样品清单。

**恢复实例的质控语义**：

- 质控结果与样品一样属于所选核对点：恢复实例由 WAL 重放构建，
  启用后登记的核对点恢复出来自带该时点的质控记录；
- **启用质控前的核对点**：恢复实例中不存在质控表，查询返回 409 并明确提示
  "早于质控启用时点"（样品数据仍可查）；
- **实例切换不混时点**：查询前核对 55433 实例的数据目录与任务记录一致，
  实例被取代后旧任务的质控/样品查询一律 409，不会把新时点数据混进旧任务。

> 说明：单条 DML 语句的并发质控重算已串行化无死锁；跨站点的多语句事务
> 与常规并发更新一样可能被数据库判死锁中止——回滚后重试即可，不会产生错配。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查（源库连通性、已归档 WAL 数） |
| POST | `/api/basebackup?force=` | 制作物理基准备份（默认启动时自动做一次） |
| GET | `/api/basebackup` | 备份状态（起始 LSN、大小、历史） |
| POST | `/api/checkpoints` | 登记命名核对点 `{"name": "cp1"}` |
| GET | `/api/checkpoints` `/api/checkpoints/{name}` | 核对点状态（`waiting_wal`/`ready`/`expired`） |
| POST | `/api/recoveries` | 按核对点发起恢复 `{"checkpoint": "cp1"}`（一次一项） |
| GET | `/api/recoveries` `/api/recoveries/{id}` | 任务状态查询 |
| POST | `/api/recoveries/{id}/retry` | 失败/取消/被取代后按原点重试 |
| DELETE | `/api/recoveries/{id}` | 取消进行中任务 / 停止已就绪实例 |
| GET | `/api/recoveries/{id}/samples` | 查询恢复实例样品（`station_no`/`sample_no` 过滤，仅 `ready` 后开放） |
| POST | `/api/recoveries/{id}/query` | 只读 SQL（演示用，限单条 SELECT，连接强制只读兜底） |
| POST | `/api/qc/enable` | 在源库启用质控（建表 + 触发器 + 已有样品回填，单事务，幂等） |
| GET | `/api/qc/status` | 质控启用状态与启用历史 |
| GET | `/api/qc/stations/{station_no}` | 当前源库的站点质控（实时，含一致性核对） |
| GET | `/api/recoveries/{id}/qc/stations/{station_no}` | 恢复实例的站点质控（仅 `ready` 且该时点已启用质控） |

### 恢复任务状态机

```
queued → waiting_wal → preparing → recovering → verifying → ready
           (等待WAL归档齐备)                            ↘ failed（可 retry）
ready --(新任务就绪前)--> superseded        ready --(DELETE)--> stopped
进行中 --(DELETE)--> cancelled             API 重启：进行中→重新排队；ready→自动拉起
```

## 手动操作示例（curl）

```bash
# 1. 等基准备份完成
curl -s localhost:8000/api/basebackup | jq .latest_done

# 2. 登记核对点（等 status 变 ready，通常几秒）
curl -s -X POST localhost:8000/api/checkpoints -H 'content-type: application/json' -d '{"name":"cp1"}'
curl -s localhost:8000/api/checkpoints/cp1

# 3. 源库继续变更（更正、误删……）
docker compose exec db psql -U postgres -d water -c "DELETE FROM samples WHERE sample_no='S-2026-0003'"

# 4. 发起恢复并等 ready
curl -s -X POST localhost:8000/api/recoveries -H 'content-type: application/json' -d '{"checkpoint":"cp1"}'
curl -s localhost:8000/api/recoveries/1

# 5. 查询删前时点（S-2026-0003 回来了）
curl -s 'localhost:8000/api/recoveries/1/samples?station_no=ST-02'
```

质控操作示例：

```bash
# 1. 启用质控（一次性，幂等；已有样品照规则回填）
curl -s -X POST localhost:8000/api/qc/enable

# 2. 源库直接变更（判定与依据在原事务内持久化）
docker compose exec db psql -U postgres -d water \
  -c "INSERT INTO samples (sample_no, station_no, ph) VALUES ('S-2026-0010','ST-01',9.20)"

# 3. 查当前源库站点质控（突变样品标为待复核，附前值与依据）
curl -s localhost:8000/api/qc/stations/ST-01

# 4. 查恢复实例（核对点时点）的站点质控
curl -s localhost:8000/api/recoveries/1/qc/stations/ST-01
```

## 限制与说明

- 演示环境口令为 `postgres/postgres`（scram-sha-256，复制连接已在 `db/init/02_hba.sh` 放行），生产使用请更换口令并收敛 pg_hba；
- 归档卷只增不减，生产需自行配置 WAL 保留/清理策略（如 `pg_archivecleanup` + 备份周期）；
- 重做基准备份（`force=true`）会使早于新备份起点的核对点过期（`expired`）；
- 恢复实例监听容器内 55433，仅 API 经本机 socket 访问，不对外暴露；
- `/query` 为演示便利，真正兜底是连接级 `default_transaction_read_only=on`，任何写操作都会被数据库拒绝。

## 已验证项（PostgreSQL 16.10 实测）

在 PostgreSQL 16.10 上按 Compose 同拓扑实测通过：

- 冷启动 → 自动基准备份 → `scripts/demo.py` 全部 10 项断言 PASS（更正/误删/跨点事务/恢复期间源库可写/不拿源库冒充）；
- 归档滞后时核对点与任务停留 `waiting_wal`、目标查询返回 409，归档恢复后任务自动续跑到 `ready`；
- 损坏 WAL 段 → 任务如实 `failed` 并附日志，修复后 `retry` 按原点恢复成功；
- API 优雅重启后 `ready` 实例自动拉起可查；API 被 SIGKILL 于恢复中途（遗留孤儿恢复进程）→ 重启后自动清理并按原点重试至 `ready`；
- 并发第二个恢复请求返回 409；新任务就绪时旧实例被 `superseded`；
- 恢复实例直连写入被数据库拒绝（read-only transaction），`/query` 非 SELECT 与多语句被 400 拦截；
- 强制重做基准备份后旧核对点自动 `expired`，恢复过期核对点返回 409。

质控功能（PostgreSQL 16.9/16.15 同拓扑实测，`scripts/demo_qc.py` 全部断言 PASS）：

- 启用后已有样品按规则回填；新增突变样品判待复核并记录前值/差值/依据；
- 更正后自身与邻项联动重算；迁站两站联动；删除后质控记录同步移除；
- 差值恰好 2.00 判可供数（严格大于 2 才待复核）；
- 事务内即可查到质控记录，回滚后无残留；8 进程并发写入 + 更正/迁站/删除/回滚混合后一致性核对通过（多轮无死锁）；
- 启用前核对点恢复后查询质控返回 409 明确提示，样品仍可查；
- 恢复实例质控属于所选核对点时点；实例切换后旧任务质控/样品查询均 409。

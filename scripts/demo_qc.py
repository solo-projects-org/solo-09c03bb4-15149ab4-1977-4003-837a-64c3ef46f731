#!/usr/bin/env python3
"""端到端演示：站点 pH 突变质控（需全新 Compose 环境）。

用法：
    docker compose down -v && docker compose up -d --build
    python3 scripts/demo_qc.py

环境变量：
    API_URL    默认 http://localhost:8000
    DEMO_PSQL  在源库执行 SQL 的命令前缀
               默认 "docker compose exec -T db psql -U postgres -d water"

流程：
  1. 质控未启用时查询 → 409；登记"启用前"核对点 cp-qc0
  2. 启用质控（POST /api/qc/enable）：已有样品照规则回填
  3. 新增突变 / 更正联动 / 迁站 / 删除：判定与依据在原事务内持久化
  4. 回滚不留错配；并发写入不留错配
  5. 登记 cp-qc1 后继续变更；恢复 cp-qc0 → 明确提示不可查询质控；
     恢复 cp-qc1 → 质控结果属于该时点；实例切换后旧任务查询 → 409
"""
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:8000")
DEMO_PSQL = os.environ.get(
    "DEMO_PSQL", "docker compose exec -T db psql -U postgres -d water")

FAILED: list[str] = []


# ------------------------------------------------------------------ 工具

def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def psql(sql: str) -> None:
    subprocess.run(shlex.split(DEMO_PSQL) + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                   check=True, capture_output=True, text=True)


def psql_scalar(sql: str) -> str:
    r = subprocess.run(shlex.split(DEMO_PSQL)
                       + ["-tA", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       check=True, capture_output=True, text=True)
    return r.stdout.strip()


def psql_multi_bg(stmts: list[str]) -> subprocess.Popen:
    """后台 psql 进程：每条 -c 一个独立事务（自动提交）。"""
    args = shlex.split(DEMO_PSQL) + ["-v", "ON_ERROR_STOP=1"]
    for s in stmts:
        args += ["-c", s]
    return subprocess.Popen(args, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def wait_for(desc: str, fn, timeout: int = 300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = fn()
        if r:
            return r
        time.sleep(2)
    raise SystemExit(f"超时：{desc}")


def check(desc: str, cond: bool) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {desc}")
    if not cond:
        FAILED.append(desc)


def create_checkpoint(name: str) -> None:
    code, body = api("POST", "/api/checkpoints", {"name": name})
    if code == 409:
        raise SystemExit(f"核对点 {name} 已存在。请先清理环境："
                         f"docker compose down -v && docker compose up -d --build")
    assert code == 201, f"创建核对点 {name} 失败: {code} {body}"
    wait_for(f"核对点 {name} WAL 归档",
             lambda: api("GET", f"/api/checkpoints/{name}")[1].get("status") == "ready")
    print(f"  核对点 {name} 就绪（LSN={body['restore_lsn']}）")


def restore_and_wait(cp_name: str) -> dict:
    code, job = api("POST", "/api/recoveries", {"checkpoint": cp_name})
    assert code == 202, f"发起恢复 {cp_name} 失败: {code} {job}"
    jid = job["id"]
    wait_for(f"任务 #{jid} 完成",
             lambda: api("GET", f"/api/recoveries/{jid}")[1].get("status") in
             ("ready", "failed", "cancelled"))
    job = api("GET", f"/api/recoveries/{jid}")[1]
    assert job["status"] == "ready", f"恢复失败: {job.get('error')}"
    print(f"  恢复任务 #{jid}（{cp_name}）就绪")
    return job


def qc_live(station: str) -> dict:
    code, body = api("GET", f"/api/qc/stations/{station}")
    assert code == 200, f"实时质控查询失败: {code} {body}"
    return body


def qc_job(job_id: int, station: str) -> tuple[int, dict]:
    return api("GET", f"/api/recoveries/{job_id}/qc/stations/{station}")


def rows_by_no(report: dict) -> dict[str, dict]:
    return {r["sample_no"]: r for r in report["rows"]}


def check_chain_consistent(station: str) -> dict:
    """客户端独立复核链规则 + 服务端一致性核对。"""
    rep = qc_live(station)
    check(f"{station} 服务端一致性核对通过", rep["consistency"]["consistent"])
    rows = rep["rows"]
    ok = True
    for i, row in enumerate(rows):
        if i == 0:
            ok &= row["verdict"] == "usable" and row["prev_sample_id"] is None
        else:
            prev = rows[i - 1]
            diff = abs(row["ph"] - prev["ph"])
            ok &= row["prev_sample_id"] == prev["sample_id"]
            ok &= row["verdict"] == ("review" if diff > 2 else "usable")
    check(f"{station} 客户端独立复核链规则通过（{len(rows)} 条）", ok)
    return rep


# ------------------------------------------------------------------ 主流程

def main() -> int:
    print("== 0. 等待 API 与基准备份 ==")
    wait_for("API 健康", lambda: api("GET", "/api/health")[1].get("status") == "ok")
    wait_for("基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {}).get(
                 "status") == "done")
    initial = psql_scalar("SELECT string_agg(sample_no || ':' || ph, ' ' ORDER BY id)"
                          " FROM samples")
    print("  源库初始数据：", initial)
    if initial != "S-2026-0001:7.10 S-2026-0002:6.95 S-2026-0003:7.40":
        raise SystemExit("源库不是初始状态。请用全新环境运行："
                         "docker compose down -v && docker compose up -d --build")
    if api("GET", "/api/qc/status")[1].get("source_qc_enabled"):
        raise SystemExit("质控已启用过。请用全新环境运行："
                         "docker compose down -v && docker compose up -d --build")

    print("== 1. 质控未启用：查询明确拒绝 ==")
    code, body = api("GET", "/api/qc/status")
    check("质控状态为未启用", code == 200 and body["source_qc_enabled"] is False)
    code, body = api("GET", "/api/qc/stations/ST-01")
    check("未启用时查询返回 409 并提示先启用",
          code == 409 and "启用" in body.get("detail", ""))

    print("== 2. 登记核对点 cp-qc0（启用质控之前） ==")
    create_checkpoint("cp-qc0")

    print("== 3. 启用质控：已有样品照规则回填 ==")
    code, body = api("POST", "/api/qc/enable")
    check("启用成功", code == 200 and body.get("status") == "enabled")
    check("回填 3 条已有样品", body.get("backfilled_samples") == 3)
    rep = qc_live("ST-01")
    rows = rows_by_no(rep)
    check("S-2026-0001 首条可供数、无前值",
          rows["S-2026-0001"]["verdict"] == "usable"
          and rows["S-2026-0001"]["prev_ph"] is None)
    check("S-2026-0002 差值 0.15 可供数",
          rows["S-2026-0002"]["verdict"] == "usable"
          and abs(rows["S-2026-0002"]["ph_diff"] - 0.15) < 1e-9)
    check("回填后一致性核对通过", rep["consistency"]["consistent"])

    print("== 4. 新增 pH 突变样品 → 待复核（返回判定、前值、可供数样品） ==")
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0010', 'ST-01', 9.20)")
    rep = qc_live("ST-01")
    row = rows_by_no(rep)["S-2026-0010"]
    check("突变样品判定待复核", row["verdict"] == "review"
          and row["verdict_label"] == "待复核")
    check("前值为 6.95、差值 2.25 写入依据",
          abs(row["prev_ph"] - 6.95) < 1e-9 and abs(row["ph_diff"] - 2.25) < 1e-9
          and "严格大于 2" in row["basis"])
    check("可供数样品清单不含突变样品",
          all(s["sample_no"] != "S-2026-0010" for s in rep["usable_samples"])
          and len(rep["usable_samples"]) == 2)

    print("== 5. 更正：自身与邻项联动重算 ==")
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-2026-0011', 'ST-01', 8.50)")
    psql("UPDATE samples SET ph = 4.00 WHERE sample_no = 'S-2026-0010'")
    rows = rows_by_no(qc_live("ST-01"))
    check("S-2026-0010 更正后差值 2.95 待复核",
          rows["S-2026-0010"]["verdict"] == "review"
          and abs(rows["S-2026-0010"]["ph_diff"] - 2.95) < 1e-9)
    check("邻项 S-2026-0011 随前值联动为待复核（差值 4.50）",
          rows["S-2026-0011"]["verdict"] == "review"
          and abs(rows["S-2026-0011"]["ph_diff"] - 4.50) < 1e-9
          and rows["S-2026-0011"]["prev_sample_id"] == rows["S-2026-0010"]["sample_id"])

    print("== 6. 迁站：S-2026-0011 迁入 ST-02 ==")
    psql("UPDATE samples SET station_no = 'ST-02' WHERE sample_no = 'S-2026-0011'")
    rep2 = qc_live("ST-02")
    rows2 = rows_by_no(rep2)
    check("迁入后前值为 ST-02 原末条（pH 7.40），差值 1.10 可供数",
          rows2["S-2026-0011"]["verdict"] == "usable"
          and abs(rows2["S-2026-0011"]["prev_ph"] - 7.40) < 1e-9)
    check("ST-01 中已无 S-2026-0011",
          "S-2026-0011" not in rows_by_no(qc_live("ST-01")))

    print("== 7. 删除：质控记录同步移除 ==")
    psql("DELETE FROM samples WHERE sample_no = 'S-2026-0010'")
    rep = qc_live("ST-01")
    check("删除后 ST-01 只剩 2 条质控记录", rep["total"] == 2)
    check("删除后一致性核对通过", rep["consistency"]["consistent"])

    print("== 8. 原事务内持久化 + 回滚不留错配 ==")
    in_tx = psql_scalar(
        "BEGIN; INSERT INTO samples (sample_no, station_no, ph)"
        " VALUES ('S-2026-0020', 'ST-01', 13.00);"
        " SELECT 'QC_IN_TX=' || count(*) FROM sample_qc"
        " WHERE sample_no = 'S-2026-0020';"
        " ROLLBACK;")
    check("事务内即可查到质控记录（原事务内持久化）", "QC_IN_TX=1" in in_tx)
    gone = psql_scalar("SELECT count(*) FROM sample_qc WHERE sample_no = 'S-2026-0020'")
    check("回滚后质控记录一并撤销", gone == "0")
    check_chain_consistent("ST-01")

    print("== 9. 并发写入 + 混合操作：不留错配 ==")
    procs = []
    for k in range(6):  # 6 个写入进程，交替 pH 制造突变链
        stmts = []
        for j in range(4):
            n = 100 + k * 10 + j
            ph = "6.00" if (k + j) % 2 == 0 else "9.50"
            stmts.append(f"INSERT INTO samples (sample_no, station_no, ph)"
                         f" VALUES ('S-2026-0{n}', 'ST-09', {ph})")
        procs.append(psql_multi_bg(stmts))
    for m in (80, 90):  # 2 个混合进程：更正 / 迁站 / 删除 / 回滚
        procs.append(psql_multi_bg([
            f"INSERT INTO samples (sample_no, station_no, ph)"
            f" VALUES ('S-2026-00{m}0', 'ST-08', 7.00)",
            f"INSERT INTO samples (sample_no, station_no, ph)"
            f" VALUES ('S-2026-00{m}1', 'ST-08', 7.10)",
            f"UPDATE samples SET ph = 9.80 WHERE sample_no = 'S-2026-00{m}1'",
            f"UPDATE samples SET station_no = 'ST-09'"
            f" WHERE sample_no = 'S-2026-00{m}1'",
            f"DELETE FROM samples WHERE sample_no = 'S-2026-00{m}0'",
            f"BEGIN; INSERT INTO samples (sample_no, station_no, ph)"
            f" VALUES ('S-2026-00{m}2', 'ST-09', 1.00); ROLLBACK",
        ]))
    for p in procs:
        out, _ = p.communicate()
        assert p.returncode == 0, f"并发写入进程失败: {out}"
    rep9 = check_chain_consistent("ST-09")
    print(f"  ST-09 共 {rep9['total']} 条：可供数 {rep9['usable_count']}，"
          f"待复核 {rep9['review_count']}")
    rolled = psql_scalar("SELECT count(*) FROM samples"
                         " WHERE sample_no IN ('S-2026-00802','S-2026-00902')")
    check("回滚的样品无残留", rolled == "0")

    print("== 10. 登记核对点 cp-qc1（质控启用之后） ==")
    create_checkpoint("cp-qc1")

    print("== 11. 继续变更（供时点对比） ==")
    psql("UPDATE samples SET ph = 9.99 WHERE sample_no = 'S-2026-0002'")
    rows = rows_by_no(qc_live("ST-01"))
    check("实时源库中 S-2026-0002 变为 9.99 待复核",
          rows["S-2026-0002"]["verdict"] == "review"
          and abs(rows["S-2026-0002"]["ph"] - 9.99) < 1e-9)

    print("== 12. 恢复 cp-qc0（启用质控前的时点） ==")
    job0 = restore_and_wait("cp-qc0")
    code, body = qc_job(job0["id"], "ST-01")
    check("启用前核对点查询质控返回 409 并明确提示",
          code == 409 and "早于质控启用时点" in body.get("detail", ""))
    code, body = api("GET", f"/api/recoveries/{job0['id']}/samples")
    check("启用前核对点样品仍可查询", code == 200 and body["row_count"] == 3)

    print("== 13. 恢复 cp-qc1：质控结果属于该时点 ==")
    job1 = restore_and_wait("cp-qc1")
    code, rep = qc_job(job1["id"], "ST-01")
    assert code == 200, f"恢复实例质控查询失败: {code} {rep}"
    rows = rows_by_no(rep)
    check("cp-qc1 中 S-2026-0002 仍为 6.95 可供数（时点正确）",
          rows["S-2026-0002"]["verdict"] == "usable"
          and abs(rows["S-2026-0002"]["ph"] - 6.95) < 1e-9)
    check("cp-qc1 中已删除的 S-2026-0010 无质控记录",
          "S-2026-0010" not in rows)
    check("cp-qc1 质控一致性核对通过", rep["consistency"]["consistent"])
    code, rep9r = qc_job(job1["id"], "ST-09")
    check("cp-qc1 含并发写入的 ST-09 链", code == 200 and rep9r["total"] > 0
          and rep9r["consistency"]["consistent"])
    code, live = api("GET", "/api/qc/stations/ST-01")
    check("实时源库仍是 9.99（恢复查询未拿源库冒充）",
          abs(rows_by_no(live)["S-2026-0002"]["ph"] - 9.99) < 1e-9)

    print("== 14. 实例切换：旧任务查询被拒绝 ==")
    job0b = restore_and_wait("cp-qc0")  # 新任务就绪后 job1 被取代
    old = api("GET", f"/api/recoveries/{job1['id']}")[1]
    check("cp-qc1 任务已被取代", old["status"] == "superseded")
    code, body = qc_job(job1["id"], "ST-01")
    check("旧任务质控查询返回 409（不混入其他时点）", code == 409)
    code, body = api("GET", f"/api/recoveries/{job1['id']}/samples")
    check("旧任务样品查询同样返回 409", code == 409)

    print()
    if FAILED:
        print(f"共 {len(FAILED)} 项校验失败：")
        for f in FAILED:
            print(f"  - {f}")
        return 1
    print("全部校验通过 ✅")
    print(f"Swagger 文档：{API}/docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())

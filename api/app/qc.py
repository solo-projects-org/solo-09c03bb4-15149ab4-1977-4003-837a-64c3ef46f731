"""站点 pH 突变质控。

判定规则（同站按样品 id 排序）：
- 首条样品：可供数；
- 其余样品：与前一条 pH 差值严格大于 2 → 待复核，否则可供数。

判定与依据由源库触发器在样品新增 / 更正 / 迁站 / 删除的**原事务**内写入
public.sample_qc：并发写入按站点持咨询锁串行化重算，回滚连同质控记录
一起撤销，不留错配。本模块负责：在源库启用质控（幂等、单事务）、
查询（当前源库或恢复实例）以及"持久化记录 vs 按规则现算"的一致性核对。
"""
from . import source, state

RULE_TEXT = ("同站按样品 id 排序：首条可供数；"
             "其余与前条 pH 差值严格大于 2 的标为待复核")

VERDICT_LABELS = {"usable": "可供数", "review": "待复核"}

# ---------------------------------------------------------------- 启用 SQL
# 全部幂等；enable_on_source 在单个事务内依次执行，要么全部生效要么全部回滚。

_ENABLE_LOCK = ("SELECT pg_advisory_xact_lock(727272)  -- 串行化并发的启用请求")

# 启用期间阻塞并发写入：保证回填不会漏掉"启用事务提交前一刻写入"的样品
_LOCK_SAMPLES = "LOCK TABLE public.samples IN SHARE ROW EXCLUSIVE MODE"

_QC_TABLE = """
CREATE TABLE IF NOT EXISTS public.sample_qc (
    sample_id      bigint PRIMARY KEY REFERENCES public.samples(id) ON DELETE CASCADE,
    sample_no      text NOT NULL,
    station_no     text NOT NULL,
    ph             numeric(4,2) NOT NULL,
    verdict        text NOT NULL CHECK (verdict IN ('usable','review')),
    prev_sample_id bigint,
    prev_ph        numeric(4,2),
    ph_diff        numeric(5,2),
    basis          text NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
)
"""

_QC_INDEX = ("CREATE INDEX IF NOT EXISTS idx_sample_qc_station"
             " ON public.sample_qc (station_no, sample_id)")

# 单样品质控重算：样品不存在则清理其质控记录；否则按规则重算并 upsert。
_QC_RECOMPUTE_FN = """
CREATE OR REPLACE FUNCTION public.qc_recompute_one(p_sample_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $func$
DECLARE
    s         public.samples%ROWTYPE;
    p         RECORD;
    v_verdict text;
    v_diff    numeric;
    v_prev_id bigint;
    v_prev_ph numeric;
    v_basis   text;
BEGIN
    SELECT * INTO s FROM public.samples WHERE id = p_sample_id;
    IF NOT FOUND THEN
        DELETE FROM public.sample_qc WHERE sample_id = p_sample_id;
        RETURN;
    END IF;

    SELECT id, sample_no, ph INTO p
      FROM public.samples
     WHERE station_no = s.station_no AND id < s.id
     ORDER BY id DESC
     LIMIT 1;

    IF NOT FOUND THEN
        v_verdict := 'usable';
        v_diff    := NULL;
        v_prev_id := NULL;
        v_prev_ph := NULL;
        v_basis   := '同站首条样品（按样品 id 排序），无前值可比，判定可供数';
    ELSE
        v_prev_id := p.id;
        v_prev_ph := p.ph;
        v_diff    := abs(s.ph - p.ph);
        IF v_diff > 2 THEN
            v_verdict := 'review';
            v_basis   := format('与前条样品 id=%s（%s，pH=%s）的 pH 差值 %s 严格大于 2，判定待复核',
                                p.id, p.sample_no, p.ph, v_diff);
        ELSE
            v_verdict := 'usable';
            v_basis   := format('与前条样品 id=%s（%s，pH=%s）的 pH 差值 %s 未超过 2，判定可供数',
                                p.id, p.sample_no, p.ph, v_diff);
        END IF;
    END IF;

    INSERT INTO public.sample_qc
        (sample_id, sample_no, station_no, ph, verdict,
         prev_sample_id, prev_ph, ph_diff, basis, updated_at)
    VALUES
        (s.id, s.sample_no, s.station_no, s.ph, v_verdict,
         v_prev_id, v_prev_ph, v_diff, v_basis, now())
    ON CONFLICT (sample_id) DO UPDATE SET
        sample_no      = EXCLUDED.sample_no,
        station_no     = EXCLUDED.station_no,
        ph             = EXCLUDED.ph,
        verdict        = EXCLUDED.verdict,
        prev_sample_id = EXCLUDED.prev_sample_id,
        prev_ph        = EXCLUDED.prev_ph,
        ph_diff        = EXCLUDED.ph_diff,
        basis          = EXCLUDED.basis,
        updated_at     = EXCLUDED.updated_at;
END;
$func$
"""

# BEFORE 行级触发器：在任何行锁/级联发生之前，先取站点咨询锁。
# 并发安全的关键：咨询锁必须先于本语句的一切行效应获取，否则
# "语句自身行锁 → 等待咨询锁" 与 "持咨询锁 → 等待他人行锁" 会构成死锁环。
# 锁为事务级，随提交/回滚释放；回滚则质控改动一并撤销，不留错配。
_QC_LOCK_FN = """
CREATE OR REPLACE FUNCTION public.samples_qc_lock()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM pg_advisory_xact_lock(hashtext(NEW.station_no));
    ELSIF TG_OP = 'UPDATE' THEN
        IF NEW.station_no <> OLD.station_no THEN
            -- 迁站涉及两个站点：按站名排序加锁，避免循环等待死锁
            PERFORM pg_advisory_xact_lock(hashtext(least(OLD.station_no, NEW.station_no)));
            PERFORM pg_advisory_xact_lock(hashtext(greatest(OLD.station_no, NEW.station_no)));
        ELSE
            PERFORM pg_advisory_xact_lock(hashtext(NEW.station_no));
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM pg_advisory_xact_lock(hashtext(OLD.station_no));
        RETURN OLD;  -- BEFORE 行级触发器：返回 NULL 会静默跳过本行操作
    END IF;
    RETURN NEW;
END;
$func$
"""

# AFTER 行级触发器：在样品变更的原事务内，重算"自身 + 受影响邻项（同站后继）"。
# 此时本事务已持有站点咨询锁（见 samples_qc_lock），同站并发重算被串行化。
_QC_SYNC_FN = """
CREATE OR REPLACE FUNCTION public.samples_qc_sync()
RETURNS trigger
LANGUAGE plpgsql
AS $func$
DECLARE
    v_succ bigint;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM public.qc_recompute_one(NEW.id);
        -- 防御：显式指定 id 插队时，后继样品的前值随之变化
        SELECT id INTO v_succ FROM public.samples
         WHERE station_no = NEW.station_no AND id > NEW.id
         ORDER BY id LIMIT 1;
        IF v_succ IS NOT NULL THEN
            PERFORM public.qc_recompute_one(v_succ);
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        PERFORM public.qc_recompute_one(NEW.id);
        SELECT id INTO v_succ FROM public.samples
         WHERE station_no = NEW.station_no AND id > NEW.id
         ORDER BY id LIMIT 1;
        IF v_succ IS NOT NULL THEN
            PERFORM public.qc_recompute_one(v_succ);
        END IF;
        IF NEW.station_no <> OLD.station_no THEN
            -- 旧站中被抽走位置的后继，其前值发生变化
            SELECT id INTO v_succ FROM public.samples
             WHERE station_no = OLD.station_no AND id > OLD.id
             ORDER BY id LIMIT 1;
            IF v_succ IS NOT NULL THEN
                PERFORM public.qc_recompute_one(v_succ);
            END IF;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM public.sample_qc WHERE sample_id = OLD.id;
        SELECT id INTO v_succ FROM public.samples
         WHERE station_no = OLD.station_no AND id > OLD.id
         ORDER BY id LIMIT 1;
        IF v_succ IS NOT NULL THEN
            PERFORM public.qc_recompute_one(v_succ);
        END IF;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$func$
"""

_QC_TRIGGER_DROP1 = "DROP TRIGGER IF EXISTS trg_samples_qc_sync ON public.samples"
_QC_TRIGGER_DROP2 = "DROP TRIGGER IF EXISTS trg_samples_qc_lock ON public.samples"

# BEFORE 锁触发器 + AFTER 重算触发器（同一迁移内成对创建）
_QC_TRIGGER_CREATE = """
CREATE TRIGGER trg_samples_qc_lock
BEFORE INSERT OR UPDATE OR DELETE ON public.samples
FOR EACH ROW EXECUTE FUNCTION public.samples_qc_lock()
"""

_QC_TRIGGER_CREATE2 = """
CREATE TRIGGER trg_samples_qc_sync
AFTER INSERT OR UPDATE OR DELETE ON public.samples
FOR EACH ROW EXECUTE FUNCTION public.samples_qc_sync()
"""

# 已有样品照此判定：按 id 顺序逐条重算（与触发器同一套逻辑）
_QC_BACKFILL = """
DO $backfill$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT id FROM public.samples ORDER BY id LOOP
        PERFORM public.qc_recompute_one(r.id);
    END LOOP;
END;
$backfill$
"""

QC_INSTALL_STATEMENTS = [
    _ENABLE_LOCK,
    _LOCK_SAMPLES,
    _QC_TABLE,
    _QC_INDEX,
    _QC_RECOMPUTE_FN,
    _QC_LOCK_FN,
    _QC_SYNC_FN,
    _QC_TRIGGER_DROP1,
    _QC_TRIGGER_DROP2,
    _QC_TRIGGER_CREATE,
    _QC_TRIGGER_CREATE2,
    _QC_BACKFILL,
]


# ---------------------------------------------------------------- 启用与状态

def available(con) -> bool:
    """该库（源库或恢复实例）是否已启用质控（存在 sample_qc 表）。"""
    row = con.execute(
        "SELECT to_regclass('public.sample_qc') AS qc_table").fetchone()
    val = row["qc_table"] if isinstance(row, dict) else row[0]
    return val is not None


def source_qc_enabled() -> bool:
    try:
        with source.query_conn() as con:
            return available(con)
    except Exception:
        return False


def enable_on_source() -> dict:
    """在源库启用质控：建表 + 触发器 + 已有样品回填（单事务，幂等）。

    这是 API 对源库唯一的写操作，且只在运维人员显式调用时发生；
    启用后质控记录由触发器在用户的业务事务内维护，API 不再写源库。
    """
    with source.admin_conn() as con:
        for stmt in QC_INSTALL_STATEMENTS:
            con.execute(stmt)
        con.commit()
    lsn = source.current_wal_lsn()
    with source.query_conn() as con:
        rows = con.execute("SELECT count(*) AS n FROM public.sample_qc"
                           ).fetchone()["n"]
    state.execute(
        "INSERT INTO qc_enable_log (enabled_at, lsn, detail) VALUES (?,?,?)",
        (state.now(), lsn,
         f"质控已启用：sample_qc 表 + 同步触发器；回填 {rows} 条已有样品"),
    )
    return {
        "status": "enabled",
        "source_wal_lsn": lsn,
        "backfilled_samples": rows,
        "rule": RULE_TEXT,
        "note": "此后样品的新增/更正/迁站/删除会在原事务内持久化质控判定及依据；"
                "启用前登记的核对点恢复后不含质控记录",
    }


# ---------------------------------------------------------------- 查询与核对

def fetch_persisted(con, station_no: str) -> list[dict]:
    """读取持久化的质控记录（触发器在原事务内维护的权威结果）。"""
    rows = con.execute(
        "SELECT sample_id, sample_no, station_no, ph::float8 AS ph, verdict,"
        "       prev_sample_id, prev_ph::float8 AS prev_ph,"
        "       ph_diff::float8 AS ph_diff, basis, updated_at"
        "  FROM public.sample_qc WHERE station_no = %s ORDER BY sample_id",
        (station_no,),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_expected(con, station_no: str) -> list[dict]:
    """按规则对 samples 现算一遍（numeric 精确运算），用于核对持久化结果。"""
    rows = con.execute(
        """
        WITH ordered AS (
            SELECT id, sample_no, station_no, ph,
                   lag(id) OVER w AS prev_sample_id,
                   lag(ph) OVER w AS prev_ph
              FROM public.samples
             WINDOW w AS (PARTITION BY station_no ORDER BY id)
        )
        SELECT id AS sample_id, sample_no, ph::float8 AS ph,
               prev_sample_id, prev_ph::float8 AS prev_ph,
               CASE WHEN prev_sample_id IS NULL THEN NULL
                    ELSE abs(ph - prev_ph)::float8 END AS ph_diff,
               CASE WHEN prev_sample_id IS NULL THEN 'usable'
                    WHEN abs(ph - prev_ph) > 2 THEN 'review'
                    ELSE 'usable' END AS verdict
          FROM ordered
         WHERE station_no = %s
         ORDER BY id
        """,
        (station_no,),
    ).fetchall()
    return [dict(r) for r in rows]


def _num_eq(a, b, tol: float = 1e-9) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) <= tol


def find_mismatches(persisted: list[dict], expected: list[dict]) -> list[dict]:
    """逐样品比对持久化质控记录与按规则现算的结果。"""
    mismatches = []
    p_by_id = {r["sample_id"]: r for r in persisted}
    e_by_id = {r["sample_id"]: r for r in expected}
    for sid in sorted(set(p_by_id) | set(e_by_id)):
        p, e = p_by_id.get(sid), e_by_id.get(sid)
        if p is None:
            mismatches.append({"sample_id": sid, "issue": "缺少质控记录"})
            continue
        if e is None:
            mismatches.append({"sample_id": sid,
                               "issue": "样品已不存在但质控记录残留"})
            continue
        diffs = []
        if p["verdict"] != e["verdict"]:
            diffs.append(f"判定 持久化={p['verdict']} 应为={e['verdict']}")
        if p["prev_sample_id"] != e["prev_sample_id"]:
            diffs.append(f"前值样品 持久化={p['prev_sample_id']}"
                         f" 应为={e['prev_sample_id']}")
        if not _num_eq(p["ph"], e["ph"]):
            diffs.append(f"pH 持久化={p['ph']} 应为={e['ph']}")
        if not _num_eq(p["prev_ph"], e["prev_ph"]):
            diffs.append(f"前值pH 持久化={p['prev_ph']} 应为={e['prev_ph']}")
        if not _num_eq(p["ph_diff"], e["ph_diff"]):
            diffs.append(f"pH差值 持久化={p['ph_diff']} 应为={e['ph_diff']}")
        if diffs:
            mismatches.append({"sample_id": sid, "issue": "；".join(diffs)})
    return mismatches


def build_report(con, station_no: str) -> dict:
    """站点质控报告：判定、前值、可供数样品 + 一致性核对。"""
    persisted = fetch_persisted(con, station_no)
    expected = fetch_expected(con, station_no)
    mismatches = find_mismatches(persisted, expected)
    rows = [{**r, "verdict_label": VERDICT_LABELS.get(r["verdict"], r["verdict"])}
            for r in persisted]
    usable = [{"sample_id": r["sample_id"], "sample_no": r["sample_no"],
               "ph": r["ph"]} for r in rows if r["verdict"] == "usable"]
    return {
        "station_no": station_no,
        "rule": RULE_TEXT,
        "total": len(rows),
        "usable_count": len(usable),
        "review_count": len(rows) - len(usable),
        "rows": rows,
        "usable_samples": usable,
        "consistency": {
            "consistent": not mismatches,
            "checked": "持久化质控记录 vs 按规则现算",
            "mismatches": mismatches[:20],
        },
    }

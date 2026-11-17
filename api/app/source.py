"""对生产源库的操作。

默认只读：连通性检查、命名恢复点（WAL 记录）、WAL 切换、参数读取、
质控结果查询（连接级只读兜底）。
唯一的写入口是 qc.enable_on_source 显式触发的质控启用（DDL + 回填，
单事务提交），见 admin_conn；此外绝不执行任何 DML/DDL。
"""
import psycopg
from psycopg.rows import dict_row

from . import config


def _conn() -> psycopg.Connection:
    return psycopg.connect(
        host=config.SOURCE_HOST,
        port=config.SOURCE_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=True,
        connect_timeout=5,
        application_name="pitr-api",
    )


def ping() -> bool:
    try:
        with _conn() as con:
            con.execute("SELECT 1")
        return True
    except Exception:
        return False


def create_restore_point(name: str) -> tuple[str, str]:
    """在源库写入命名恢复点并切换 WAL 段，使该点所在段尽快被归档。

    返回 (restore_lsn, wal_file)。这只是 WAL 级元数据记录，不修改任何业务数据。
    """
    with _conn() as con:
        lsn = con.execute(
            "SELECT pg_create_restore_point(%s)::text", (name,)
        ).fetchone()[0]
        wal_file = con.execute(
            "SELECT pg_walfile_name(%s::pg_lsn)", (lsn,)
        ).fetchone()[0]
        # 切换段，让包含恢复点的段立即闭合进入归档队列
        con.execute("SELECT pg_switch_wal()")
    return lsn, wal_file


def wal_segment_size() -> int:
    with _conn() as con:
        raw = con.execute("SHOW wal_segment_size").fetchone()[0]
    return _parse_size(str(raw))


def _parse_size(s: str) -> int:
    s = s.strip()
    for unit, mult in (("GB", 1 << 30), ("MB", 1 << 20), ("kB", 1 << 10), ("B", 1)):
        if s.endswith(unit):
            return int(float(s[: -len(unit)]) * mult)
    return int(s)


def query_conn() -> psycopg.Connection:
    """当前源库的只读查询连接（质控查询用）。

    连接级 default_transaction_read_only=on 兜底：任何写操作都会被
    数据库直接拒绝，API 不可能经此连接改写生产数据。
    """
    return psycopg.connect(
        host=config.SOURCE_HOST,
        port=config.SOURCE_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=True,
        connect_timeout=5,
        application_name="pitr-api-qc-query",
        options="-c default_transaction_read_only=on -c statement_timeout=15000",
        row_factory=dict_row,
    )


def admin_conn() -> psycopg.Connection:
    """源库管理连接：仅用于显式的 POST /api/qc/enable（DDL + 回填，单事务）。"""
    return psycopg.connect(
        host=config.SOURCE_HOST,
        port=config.SOURCE_PORT,
        dbname=config.SOURCE_DB,
        user=config.SOURCE_USER,
        password=config.SOURCE_PASSWORD,
        autocommit=False,
        connect_timeout=5,
        application_name="pitr-api-qc-admin",
    )


def current_wal_lsn() -> str:
    with _conn() as con:
        return con.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]

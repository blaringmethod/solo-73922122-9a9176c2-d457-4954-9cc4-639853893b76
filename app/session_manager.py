"""会话注册表与快照生命周期管理。

设计要点
========
1. 每个会话独占一条 PostgreSQL 连接，并在其上开启
   ``START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY``。
   连接以 "idle in transaction" 状态保持，事务快照在整个会话期间不变；
   源端其它连接上的增/改/删照常提交，本会话各页（含重复取页）读到的
   两张表始终是开启那一刻的同一快照（MVCC，读写互不阻塞）。
2. 会话状态机：``open -> closing -> closed/expired``。关闭请求受理的
   第一时间即把状态翻成 ``closing``，此后尚未开始响应的取页一律失败；
   已经在执行中的取页允许跑完，随后回滚事务、归还连接、释放名额。
3. 名额（默认 2）在内存注册表中控制，会话状态全部保存在本进程内；
   进程重启或与数据库的连接丢失后旧会话不可恢复，符合
   "不得换快照续取" 的要求。
4. 源端写操作与停机交接（本模块的 ``run_write``）：

   - 停机受理后（``begin_shutdown``）立即关闭写入口，新的写请求
     在**执行任何 SQL 之前**被明确拒绝（503，不会写入）。
   - 已经借到连接的写请求不会被中断：它跑完自己的事务，
     **提交成功才返回成功**，异常则回滚并明确失败；之后才归还连接。
   - 正在等待连接的写请求会结束等待并失败，绝不会在停机/重启后
     才拿到连接意外提交。
   - ``stop()`` 在关闭连接池之前等待在途写请求全部结束（有上限），
     避免"已提交却返回失败"或"池先关闭导致归还连接报错"。
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional, TypeVar

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolClosed, PoolTimeout

from .config import Settings

T = TypeVar("T")


class SessionError(Exception):
    """会话层面的可预期错误，路由层据此映射 HTTP 状态码。"""

    def __init__(self, code: str, message: str, http_status: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class CapacityReached(SessionError):
    def __init__(self, max_sessions: int):
        super().__init__(
            "capacity_reached",
            f"会话名额已满（最多 {max_sessions} 个），请关闭其它会话后重试",
            http_status=429,
        )


class SessionNotFound(SessionError):
    def __init__(self, session_id: str):
        super().__init__(
            "session_not_found",
            f"会话 {session_id} 不存在或已失效（服务重启/连接丢失后旧会话不可续取）",
            http_status=404,
        )


class SessionUnavailable(SessionError):
    """会话处于 closing/closed/expired，取页请求被拒绝。"""


class ServiceUnavailable(SessionError):
    """服务正在停机：写请求不会被执行。"""


@dataclass
class _WriteTracker:
    """停机与源端写请求之间的交接闸门。

    ``admitted`` 是已经越过准入、可能正在执行的写请求数；
    ``waiting`` 是仍在连接池里排队等连接、尚未执行任何 SQL 的写请求数。
    两者合起来是"未完成写请求"总数，``stop()`` 关闭连接池前要等它归零。
    """

    lock: threading.Lock = field(default_factory=threading.Lock)
    accepting: bool = True
    admitted: int = 0
    waiting: int = 0
    idle: threading.Condition = field(init=False)

    def __post_init__(self) -> None:
        self.idle = threading.Condition(self.lock)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Session:
    session_id: str
    region: str
    conn: psycopg.Connection
    opened_at: datetime
    expires_at: datetime
    # state_lock 保护 status / 计数器 / 时间戳；
    # io_lock 串行化同一会话上的查询，保证取页与关闭不会并发使用连接。
    state_lock: threading.RLock = field(default_factory=threading.RLock)
    io_lock: threading.Lock = field(default_factory=threading.Lock)
    status: str = "open"
    pages_served: int = 0
    last_page_at: Optional[datetime] = None
    in_flight: int = 0
    snapshot_time: Optional[datetime] = None
    snapshot_id: Optional[str] = None

    def is_open(self) -> bool:
        with self.state_lock:
            return self.status == "open" and _utcnow() < self.expires_at


class SessionManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._registry: dict[str, Session] = {}
        self._registry_lock = threading.RLock()
        self._pool: Optional[ConnectionPool] = None
        self._stop_event = threading.Event()
        self._sweeper: Optional[threading.Thread] = None
        self._writes = _WriteTracker()
        # 保证 start/stop 不会交叉执行（lifespan 线程与 sweeper 错误路径）
        self._lifecycle_lock = threading.RLock()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    @staticmethod
    def _reset_connection(conn: psycopg.Connection) -> None:
        """连接归还池时：回滚残留事务并恢复自动提交，供下一个借用者干净使用。"""
        try:
            if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                conn.rollback()
        except psycopg.Error:
            pass
        try:
            conn.autocommit = True
        except psycopg.Error:
            pass

    def start(self) -> None:
        # 池默认自动提交：admin 写语句执行即提交；
        # 会话连接借出后显式 START TRANSACTION 长期持有快照事务。
        with self._lifecycle_lock:
            if self._pool is None or self._pool.closed:
                self._pool = ConnectionPool(
                    self._settings.database_url,
                    min_size=self._settings.pool_min_size,
                    max_size=self._settings.pool_max_size,
                    kwargs={"row_factory": dict_row, "autocommit": True},
                    # 连接归还池时恢复自动提交并回滚任何残留事务
                    reset=self._reset_connection,
                    open=True,
                )
            # 重新启动后写入口必须重新放行（stop 时会关闭）
            with self._writes.lock:
                self._writes.accepting = True
            # 每次 start 都确保清理线程在运行（stop 后重新 start 时需要重建）
            if self._sweeper is None or not self._sweeper.is_alive():
                self._stop_event = threading.Event()
                self._sweeper = threading.Thread(
                    target=self._sweep_loop, name="session-sweeper", daemon=True
                )
                self._sweeper.start()

    def begin_shutdown(self) -> None:
        """停机受理：立即关闭写入口，唤醒所有等连接的写请求。

        幂等；只关写入口，不触碰快照会话与连接池，因此已开启会话的
        分页/关闭仍可继续，直到 :meth:`stop` 真正收尾。
        """
        with self._writes.lock:
            self._writes.accepting = False
            self._writes.idle.notify_all()

    def stop(self) -> None:
        """有序停机：先关写入闸门，等在途写结束，最后才关闭连接池。

        顺序很关键：
        1. 关闭写入口：此后新的写请求在执行 SQL 前就被拒绝；
           正在等连接的写请求被唤醒并失败（不会在重启后才拿到连接）。
        2. 等待已借到连接的写请求提交/回滚并归还连接。
        3. 结束快照会话、关闭连接池 —— 此时不会再有写请求持有连接。
        """
        with self._lifecycle_lock:
            # 1) 停机受理：不再开始新的写入
            self.begin_shutdown()
            # 2) 等待在途写请求结束（有上限，超时后仍继续停机流程）
            self._drain_writes()

            self._stop_event.set()
            if self._sweeper is not None:
                self._sweeper.join(timeout=5)
            self._sweeper = None
            with self._registry_lock:
                session_ids = list(self._registry.keys())
            for sid in session_ids:
                try:
                    self.close(sid, reason="shutdown")
                except SessionError:
                    pass
            # 3) 连接池兜底关闭。此时写请求与快照会话都已归还连接；
            #    若 drain 超时仍有写请求在跑，pool.close() 只会在其归还
            #    连接时关闭该连接，不会打断已在执行的提交。
            if self._pool is not None:
                self._pool.close()
                self._pool = None
            # 进程重启语义：内存注册表全部丢弃，旧会话 id 不可再查、不可续取
            with self._registry_lock:
                self._registry.clear()

    def _drain_writes(self) -> None:
        """等待在途写请求提交/回滚完成；超时则记录并继续停机。"""
        deadline = time.monotonic() + self._settings.shutdown_drain_timeout_seconds
        with self._writes.lock:
            while self._writes.admitted > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    print(
                        "[shutdown] 等待在途写请求超时，仍有 "
                        f"{self._writes.admitted} 个写请求在执行",
                        flush=True,
                    )
                    return
                self._writes.idle.wait(remaining)

    # ------------------------------------------------------------------
    # 开启会话
    # ------------------------------------------------------------------
    def open_session(self, region: str) -> Session:
        # 停机后不再开启新会话；与 stop() 的池关闭互斥，避免从已关闭池取连接。
        with self._lifecycle_lock:
            with self._registry_lock:
                # 名额检查与注册表写入在同一把锁内完成，避免并发超额。
                active = [
                    s for s in self._registry.values()
                    if s.status in ("open", "closing")
                ]
                if len(active) >= self._settings.max_sessions:
                    raise CapacityReached(self._settings.max_sessions)

                pool = self._pool
                if pool is None or pool.closed:
                    raise ServiceUnavailable(
                        "service_shutting_down",
                        "服务正在停机，不再开启新会话",
                        http_status=503,
                    )
                try:
                    # 限时取连接：停机在等 _lifecycle_lock 时不会无限期卡住；
                    # 也避免连接耗尽时请求悬挂。
                    conn = pool.getconn(
                        timeout=self._settings.write_acquire_timeout_seconds
                    )
                except PoolTimeout:
                    raise SessionError(
                        "connection_unavailable",
                        "数据库连接暂时不可用，请稍后重试",
                        http_status=503,
                    )
                except PoolClosed:
                    raise ServiceUnavailable(
                        "service_shutting_down",
                        "服务正在停机，不再开启新会话",
                        http_status=503,
                    )
                now = _utcnow()
                session = Session(
                    session_id=uuid.uuid4().hex,
                    region=region,
                    conn=conn,
                    opened_at=now,
                    expires_at=now
                    + timedelta(seconds=self._settings.session_ttl_seconds),
                )
                try:
                    # 保持 autocommit=True：直接发 START TRANSACTION 由服务端
                    # 显式开启 RR 只读事务（若先关 autocommit，psycopg 会隐式
                    # 发 BEGIN，再发 START TRANSACTION 会产生重复事务警告）。
                    # 事务在首条语句冻结快照，连接随会话停留在
                    # "idle in transaction" 状态，快照保持不变。
                    conn.execute(
                        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                    # pg_export_snapshot 返回当前事务快照 ID，
                    # 同时确认快照已在事务内建立（RR 下首条语句即冻结快照）。
                    row = conn.execute(
                        "SELECT now() AS snapshot_time, "
                        "pg_export_snapshot() AS snapshot_id"
                    ).fetchone()
                    session.snapshot_time = row["snapshot_time"]
                    session.snapshot_id = row["snapshot_id"]
                except Exception:
                    conn.rollback()
                    pool.putconn(conn)
                    raise
                self._registry[session.session_id] = session
                return session

    # ------------------------------------------------------------------
    # 源端写请求（与停机交接）
    # ------------------------------------------------------------------
    def run_write(self, operation: Callable[[psycopg.Connection], T]) -> T:
        """在一个池连接上执行一次源端写操作，并返回其结果。

        与停机的交接规则：

        * 停机受理后不再开始新的写入：本方法在**执行 SQL 之前**抛
          :class:`ServiceUnavailable`（HTTP 503），数据库不会产生任何变更。
        * 已经借到连接的写入不会被中断：``operation`` 在**单个显式事务**中
          执行，整体成功才 COMMIT，随后归还连接并返回成功。
        * 若 ``operation`` 抛错，事务整笔回滚（异常向外传播），调用方收到
          明确失败——不会出现"报错却已写入"，即使错误发生在语句执行之后。
        * 正在等连接的写入遇到停机：等待被立即打断，尚未执行任何 SQL，
          抛 :class:`ServiceUnavailable`（HTTP 503，code 为
          ``write_interrupted_by_shutdown``），重试也不会造成重复补录；
          也不会在停机结束后才拿到连接意外提交。
        """
        deadline = time.monotonic() + self._settings.write_acquire_timeout_seconds
        # 1) 准入检查与取连接（此处不持锁等待，停机可随时打断）
        with self._writes.lock:
            if not self._writes.accepting:
                raise self._shutting_down_error()
            self._writes.waiting += 1
        try:
            conn = self._acquire_for_write(deadline)
        except BaseException:
            self._finish_waiting()
            raise
        # 2) 取到连接后与停机受理原子交接：
        #    若此刻已停机，立即归还连接、不执行任何 SQL。
        with self._writes.lock:
            self._writes.waiting -= 1
            if not self._writes.accepting:
                self._writes.idle.notify_all()
                reject = True
            else:
                self._writes.admitted += 1
                reject = False
        if reject:
            self._return_conn(conn)
            raise self._shutting_down_error()
        # 3) 已登记为在途写入：跑完事务后再归还连接并通知停机流程。
        try:
            return self._execute_write(conn, operation)
        finally:
            with self._writes.lock:
                self._writes.admitted -= 1
                self._writes.idle.notify_all()

    def _finish_waiting(self) -> None:
        with self._writes.lock:
            self._writes.waiting -= 1
            self._writes.idle.notify_all()

    @staticmethod
    def _shutting_down_error() -> SessionError:
        return ServiceUnavailable(
            "service_shutting_down",
            "服务正在停机，写请求未被执行（未写入任何数据），"
            "可安全地在服务恢复后重试",
            http_status=503,
        )

    def _acquire_for_write(self, deadline: float) -> psycopg.Connection:
        """分片等待连接；停机时立刻结束等待，而不是一直等到拿到连接。"""
        while True:
            with self._writes.lock:
                if not self._writes.accepting:
                    raise ServiceUnavailable(
                        "write_interrupted_by_shutdown",
                        "服务停机打断了等待连接的写请求，未执行任何 SQL、"
                        "未写入任何数据，可安全地在服务恢复后重试",
                        http_status=503,
                    )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SessionError(
                    "write_acquire_timeout",
                    "等待数据库连接超时，写请求未执行（未写入任何数据）",
                    http_status=503,
                )
            pool = self._pool
            if pool is None or pool.closed:
                raise ServiceUnavailable(
                    "write_interrupted_by_shutdown",
                    "服务正在停机，写请求未执行（未写入任何数据）",
                    http_status=503,
                )
            try:
                return pool.getconn(timeout=min(0.1, remaining))
            except PoolTimeout:
                # 连接紧张：继续分片等待，期间随时响应停机
                continue
            except PoolClosed:
                raise ServiceUnavailable(
                    "write_interrupted_by_shutdown",
                    "服务停机关闭了连接池，写请求未执行（未写入任何数据）",
                    http_status=503,
                )

    def _execute_write(
        self,
        conn: psycopg.Connection,
        operation: Callable[[psycopg.Connection], T],
    ) -> T:
        """在已借到的连接上以单事务执行写操作：成功才提交，失败必回滚。

        不能依赖连接的 autocommit：若 ``operation`` 里先执行了语句再抛错，
        autocommit 下语句已经落库，回滚也撤不回来 —— 那就成了"报错却已写入"。
        因此这里显式开事务，只有 ``operation`` 整体成功才 COMMIT。
        """
        try:
            with conn.transaction():
                result = operation(conn)
        except BaseException:
            # transaction() 退出时已回滚；再兜底清一次残留事务状态。
            try:
                conn.rollback()
            except psycopg.Error:
                pass
            raise
        finally:
            # 仅在"连接已归还"这一步失败时吞掉异常：此时 SQL 已经提交，
            # 归还失败不能反过来把一次成功的写入报成失败。
            try:
                self._return_conn(conn)
            except Exception as exc:  # pragma: no cover - 防御性
                print(f"[write] 归还连接失败（写入已提交）: {exc!r}", flush=True)
        return result

    def _return_conn(self, conn: psycopg.Connection) -> None:
        pool = self._pool
        if pool is not None and not pool.closed:
            try:
                pool.putconn(conn)
                return
            except Exception:
                # 池在竞态中被关闭：回落到直接关闭连接
                pass
        try:
            conn.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 取页
    # ------------------------------------------------------------------
    def fetch_page(self, session_id: str, page: int, page_size: int):
        """在会话快照上取第 ``page`` 页（从 1 开始），claim_id 升序。"""
        session = self._get(session_id)

        with session.state_lock:
            if session.status != "open":
                raise SessionUnavailable(
                    "session_not_open",
                    f"会话 {session_id} 状态为 {session.status}，"
                    "关闭已受理或会话已结束，取页失败且不返回明细",
                    http_status=409,
                )
            if _utcnow() >= session.expires_at:
                # 到期瞬间、清理线程尚未接手：直接拒绝，清理稍后完成。
                raise SessionUnavailable(
                    "session_expired",
                    f"会话 {session_id} 已到期，取页失败且不返回明细",
                    http_status=410,
                )
            session.in_flight += 1

        try:
            # io_lock 保证关闭流程与查询不会同时操作同一条连接。
            with session.io_lock:
                # 拿到连接使用权后再复核一次状态：关闭可能在等锁期间受理。
                with session.state_lock:
                    if session.status != "open":
                        raise SessionUnavailable(
                            "session_not_open",
                            f"会话 {session_id} 关闭已受理，"
                            "该取页尚未开始响应，已失败且不返回明细",
                            http_status=409,
                        )
                rows, has_more = self._query_page(session, page, page_size)
                with session.state_lock:
                    session.pages_served += 1
                    session.last_page_at = _utcnow()
                return rows, has_more
        except SessionUnavailable:
            raise
        except psycopg.Error as exc:
            # 连接丢失等数据库错误：会话作废，绝不换连接/换快照续取。
            self._teardown(session, reason="connection_lost")
            raise SessionError(
                "session_broken",
                f"数据库连接或快照失效，会话 {session_id} 已作废，"
                f"不会换快照续取：{exc.sqlstate_name if hasattr(exc, 'sqlstate_name') else exc}",
                http_status=503,
            )
        finally:
            with session.state_lock:
                session.in_flight -= 1

    def _query_page(self, session: Session, page: int, page_size: int):
        # 多取一行判断 has_more；OFFSET 分页基于快照内固定结果集，
        # 重复取同一页得到完全相同的数据。
        offset = (page - 1) * page_size
        sql = """
            SELECT c.id          AS claim_id,
                   c.policy_id   AS policy_id,
                   p.region      AS region,
                   c.amount      AS amount
              FROM claims c
              JOIN policies p ON p.id = c.policy_id
             WHERE p.region = %s
             ORDER BY c.id ASC
             LIMIT %s OFFSET %s
        """
        rows = session.conn.execute(
            sql, (session.region, page_size + 1, offset)
        ).fetchall()
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]
        return rows, has_more

    # ------------------------------------------------------------------
    # 关闭 / 到期
    # ------------------------------------------------------------------
    def close(self, session_id: str, reason: str = "closed") -> Session:
        """受理关闭：先翻状态拒绝新取页，再停事务、还连接、释放名额。"""
        session = self._get(session_id)
        with session.state_lock:
            if session.status in ("closed", "expired"):
                return session
            # 第一时间受理：此后任何取页请求都失败。
            # 已在 closing 的会话不再重复受理，直接去完成一次终止流程。
            if session.status == "open":
                session.status = "closing"
        # 等待在途取页结束（它们持有 io_lock），然后独占连接做清理。
        self._teardown(session, reason=reason)
        return session

    def _teardown(self, session: Session, reason: str) -> None:
        with session.io_lock:
            with session.state_lock:
                if session.status in ("closed", "expired"):
                    return
            try:
                # 停止该会话自身的 SQL：回滚即结束快照事务。
                session.conn.rollback()
            except psycopg.Error:
                pass
            # 释放快照连接，归还连接池供其它会话/新会话使用。
            # 停机时池可能已先行关闭(正常不会)：此直接关闭连接兜底。
            self._return_conn(session.conn)
            with session.state_lock:
                session.status = "expired" if reason == "expired" else "closed"
        # 终态记录保留在注册表中以便查询会话状态；名额只统计
        # open/closing 会话（见 open_session），故终态记录不占名额。

    def _sweep_loop(self) -> None:
        while not self._stop_event.wait(self._settings.sweeper_interval_seconds):
            try:
                self._sweep_once()
            except Exception as exc:
                # 清理线程永不因单次异常（含数据库连接丢失）退出。
                print(f"[sweeper] 本轮清理失败，将继续重试: {exc!r}", flush=True)

    def _sweep_once(self) -> None:
        now = _utcnow()
        with self._registry_lock:
            due = [
                s for s in list(self._registry.values())
                if s.status == "open" and now >= s.expires_at
            ]
            # 终态记录保留 10 分钟供状态查询，之后回收注册表项。
            stale = [
                s for s in list(self._registry.values())
                if s.status in ("closed", "expired")
                and now - s.expires_at > timedelta(minutes=10)
            ]
        for session in due:
            try:
                self.close(session.session_id, reason="expired")
            except SessionError:
                pass
            except Exception as exc:
                # 连接丢失等情况下，先在注册表内标记终态释放名额，
                # 失效连接留给连接池自身的健康检查回收。
                print(f"[sweeper] 会话 {session.session_id} 到期清理失败: {exc!r}",
                      flush=True)
                with session.state_lock:
                    if session.status == "open":
                        session.status = "expired"
        with self._registry_lock:
            for session in stale:
                self._registry.pop(session.session_id, None)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def _get(self, session_id: str) -> Session:
        with self._registry_lock:
            session = self._registry.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        return session

    def get_status(self, session_id: str) -> Session:
        return self._get(session_id)

    def is_shutting_down(self) -> bool:
        with self._writes.lock:
            return not self._writes.accepting

    def pending_writes(self) -> int:
        with self._writes.lock:
            return self._writes.admitted + self._writes.waiting

    def list_sessions(self) -> list[Session]:
        with self._registry_lock:
            return list(self._registry.values())

    def pool(self) -> ConnectionPool:
        if self._pool is None:
            raise SessionError(
                "not_started", "服务尚未启动", http_status=503
            )
        return self._pool

    def ttl_seconds(self) -> int:
        return self._settings.session_ttl_seconds

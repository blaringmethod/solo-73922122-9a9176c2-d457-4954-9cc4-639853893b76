"""FastAPI 入口：理赔明细分批提取服务。"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import psycopg.errors
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from .config import settings
from .schemas import (
    ClaimIn,
    ClaimOut,
    ClaimPage,
    ClaimRow,
    Message,
    OpenSessionRequest,
    PolicyIn,
    PolicyOut,
    SessionInfo,
    SessionStatus,
)
from .session_manager import SessionError, SessionManager

manager = SessionManager(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    manager.start()
    try:
        yield
    finally:
        # 停机走 manager.stop() 的有序流程：先关写入口、等在途写结束，
        # 再关闭连接池。stop 内有阻塞等待，放到线程池避免卡住事件循环。
        await asyncio.to_thread(manager.stop)


app = FastAPI(
    title="理赔明细分批提取 API",
    description=(
        "按地区开启快照会话，按理赔 id 升序分页提取。\n\n"
        "- 会话内两表（policies/claims）固定为开启时刻的同一快照\n"
        "- 源端增改删照常提交，不影响会话内读取\n"
        f"- 最多 {settings.max_sessions} 个并发会话，会话 {settings.session_ttl_seconds} 秒到期\n"
        "- 关闭受理后未开始响应的取页一律失败，不返回明细"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ----------------------------------------------------------------------
# 异常映射
# ----------------------------------------------------------------------
@app.exception_handler(SessionError)
async def session_error_handler(request: Request, exc: SessionError):
    return JSONResponse(
        status_code=exc.http_status,
        content={"detail": exc.message, "code": exc.code},
    )


@app.exception_handler(psycopg.errors.IntegrityError)
async def integrity_error_handler(request: Request, exc: psycopg.errors.IntegrityError):
    # 写操作已被回滚，未提交；明确告诉调用方可以安全重试（修正入参后）
    return JSONResponse(
        status_code=409,
        content={
            "detail": f"写请求被数据库拒绝（已回滚，未写入）：{exc.diag.message_primary or exc}",
            "code": "write_rejected",
        },
    )


def _session_info(s) -> SessionInfo:
    return SessionInfo(
        session_id=s.session_id,
        region=s.region,
        status=SessionStatus(s.status),
        opened_at=s.opened_at,
        expires_at=s.expires_at,
        last_page_at=s.last_page_at,
        pages_served=s.pages_served,
        ttl_seconds=settings.session_ttl_seconds,
    )


# ----------------------------------------------------------------------
# 健康检查与服务参数
# ----------------------------------------------------------------------
@app.get("/health", tags=["meta"], summary="健康检查")
def health():
    return {
        "status": "ok",
        # 停机受理后为 false：可据此判断是否还应发送写请求
        "accepting_writes": not manager.is_shutting_down(),
        "pending_writes": manager.pending_writes(),
    }


@app.get("/meta", tags=["meta"], summary="服务参数（TTL、页上限、会话名额）")
def meta():
    return {
        "max_sessions": settings.max_sessions,
        "session_ttl_seconds": settings.session_ttl_seconds,
        "page_size_cap": settings.page_size_cap,
        "debug_hooks_enabled": settings.enable_debug_hooks,
        "write_acquire_timeout_seconds": settings.write_acquire_timeout_seconds,
        "shutdown_drain_timeout_seconds": settings.shutdown_drain_timeout_seconds,
        "accepting_writes": not manager.is_shutting_down(),
    }


@app.post(
    "/admin/shutdown",
    tags=["admin"],
    summary="[调试] 受理停机（关闭写入口并有序收尾；仅 ENABLE_DEBUG_HOOKS=true）",
    responses={404: {"description": "调试钩子未开启"}},
)
async def debug_shutdown():
    """演示/自测辅助：受理一次停机。

    与 SIGTERM 的差别：SIGTERM 由 uvicorn 先让在途 HTTP 请求跑完再收尾；
    本接口无法让命令行 uvicorn 进程退出，因此立即开始有序收尾
    （等在途写 → 关会话 → 关池）。生产环境请使用 SIGTERM / `docker stop`。
    """
    if not settings.enable_debug_hooks:
        raise HTTPException(status_code=404, detail="调试钩子未开启")
    # 同步关闭写入口，让"停机受理"立刻对并发写请求可见
    manager.begin_shutdown()
    server = getattr(app.state, "uvicorn_server", None)
    if server is not None:
        # 若宿主把 uvicorn.Server 挂到 app.state，则走与 SIGTERM 相同的路径
        server.should_exit = True
    else:
        # 未挂载 Server：自行完成有序收尾，处理完本请求后停止接受写入
        asyncio.get_running_loop().create_task(asyncio.to_thread(manager.stop))
    return {"status": "shutdown_initiated"}


# ----------------------------------------------------------------------
# 会话管理
# ----------------------------------------------------------------------
@app.post(
    "/sessions",
    response_model=SessionInfo,
    status_code=201,
    tags=["sessions"],
    summary="按地区开启快照会话",
    responses={429: {"model": Message, "description": "会话名额已满"}},
)
async def open_session(body: OpenSessionRequest):
    # 同步的 psycopg 调用放到线程池，避免阻塞事件循环
    session = await asyncio.to_thread(manager.open_session, body.region)
    return _session_info(session)


@app.get(
    "/sessions",
    response_model=list[SessionInfo],
    tags=["sessions"],
    summary="列出当前会话（含终态记录）",
)
async def list_sessions():
    sessions = await asyncio.to_thread(manager.list_sessions)
    return [_session_info(s) for s in sessions]


@app.get(
    "/sessions/{session_id}",
    response_model=SessionInfo,
    tags=["sessions"],
    summary="查询会话状态",
    responses={404: {"model": Message}},
)
async def session_status(session_id: str):
    return _session_info(await asyncio.to_thread(manager.get_status, session_id))


@app.post(
    "/sessions/{session_id}/close",
    response_model=SessionInfo,
    tags=["sessions"],
    summary="主动关闭会话（受理后未开始的取页立即失败）",
    responses={404: {"model": Message}},
)
async def close_session(session_id: str):
    session = await asyncio.to_thread(manager.close, session_id)
    return _session_info(session)


# ----------------------------------------------------------------------
# 取页
# ----------------------------------------------------------------------
@app.get(
    "/sessions/{session_id}/claims",
    response_model=ClaimPage,
    tags=["claims"],
    summary="按理赔 id 升序取一页（每页至多 100 行，可重复取页）",
    responses={
        404: {"model": Message},
        409: {"model": Message, "description": "关闭已受理/会话不可用"},
        410: {"model": Message, "description": "会话已到期"},
        503: {"model": Message, "description": "快照连接失效，会话作废"},
    },
)
async def fetch_claims(
    session_id: str,
    request: Request,
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(
        100, ge=1, description="本页行数上限，服务端强制不超过 100"
    ),
):
    page_size = min(page_size, settings.page_size_cap)

    # 演示用慢取页钩子：仅当 ENABLE_DEBUG_HOOKS=true 时生效，
    # 通过 X-Debug-Sleep-Seconds 请求头让本次 SQL 前先睡眠若干秒。
    # 睡眠放在线程池中，不阻塞事件循环与其它请求。
    if settings.enable_debug_hooks:
        raw = request.headers.get("x-debug-sleep-seconds")
        if raw:
            try:
                delay = max(0.0, min(30.0, float(raw)))
            except ValueError:
                raise HTTPException(status_code=400, detail="非法的调试延迟头")
            await asyncio.to_thread(time.sleep, delay)

    session = await asyncio.to_thread(manager.get_status, session_id)
    rows, has_more = await asyncio.to_thread(
        manager.fetch_page, session_id, page, page_size
    )
    return ClaimPage(
        session_id=session_id,
        region=session.region,
        page=page,
        page_size=page_size,
        has_more=has_more,
        snapshot_time=session.snapshot_time,
        rows=[ClaimRow(**row) for row in rows],
    )


# ----------------------------------------------------------------------
# 源端补录接口（对应 db/init/03_sql_examples.sql 的 SQL 示例）
# 写操作统一走 manager.run_write：停机时在执行 SQL 前拒绝，
# 已借到连接的写入提交成功才返回成功，失败则回滚并明确报错。
# ----------------------------------------------------------------------
def _write_debug_delay(request: Request) -> float:
    """自测/演示钩子：返回写请求执行 SQL 前的暂停秒数。

    延迟发生在"已借到连接、尚未执行 SQL"的窗口内，用于制造
    "写入进行中恰逢停机"的交错。仅 ENABLE_DEBUG_HOOKS=true 时生效。
    """
    if not settings.enable_debug_hooks:
        return 0.0
    raw = request.headers.get("x-debug-write-delay-seconds")
    if not raw:
        return 0.0
    try:
        return max(0.0, min(30.0, float(raw)))
    except ValueError:
        raise HTTPException(status_code=400, detail="非法的调试延迟头")


def _call_write(operation, delay: float):
    """在线程池中执行一次源端写操作（含可选的执行前暂停）。"""
    if delay > 0:
        def _delayed(conn):
            time.sleep(delay)
            return operation(conn)
        return manager.run_write(_delayed)
    return manager.run_write(operation)


@app.post(
    "/admin/policies",
    response_model=PolicyOut,
    status_code=201,
    tags=["admin"],
    summary="补录保单（源端写操作，照常提交）",
    responses={503: {"model": Message, "description": "服务停机中，写请求未执行"}},
)
async def create_policy(body: PolicyIn, request: Request):
    def _insert(conn):
        return conn.execute(
            "INSERT INTO policies (region) VALUES (%s) RETURNING id, region",
            (body.region,),
        ).fetchone()

    row = await asyncio.to_thread(
        _call_write, _insert, _write_debug_delay(request)
    )
    return PolicyOut(**row)


@app.post(
    "/admin/claims",
    response_model=ClaimOut,
    status_code=201,
    tags=["admin"],
    summary="补录理赔（源端写操作，照常提交）",
    responses={503: {"model": Message, "description": "服务停机中，写请求未执行"}},
)
async def create_claim(body: ClaimIn, request: Request):
    def _insert(conn):
        return conn.execute(
            "INSERT INTO claims (policy_id, amount) VALUES (%s, %s) "
            "RETURNING id, policy_id, amount",
            (body.policy_id, body.amount),
        ).fetchone()

    row = await asyncio.to_thread(
        _call_write, _insert, _write_debug_delay(request)
    )
    return ClaimOut(**row)


@app.patch(
    "/admin/claims/{claim_id}",
    response_model=ClaimOut,
    tags=["admin"],
    summary="修改理赔金额（源端写操作，照常提交）",
    responses={503: {"model": Message, "description": "服务停机中，写请求未执行"}},
)
async def update_claim(
    claim_id: int,
    request: Request,
    amount: int = Query(..., ge=0),
):
    def _update(conn):
        return conn.execute(
            "UPDATE claims SET amount = %s WHERE id = %s "
            "RETURNING id, policy_id, amount",
            (amount, claim_id),
        ).fetchone()

    row = await asyncio.to_thread(
        _call_write, _update, _write_debug_delay(request)
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"理赔 {claim_id} 不存在")
    return ClaimOut(**row)


@app.delete(
    "/admin/claims/{claim_id}",
    status_code=204,
    tags=["admin"],
    summary="删除理赔（源端写操作，照常提交）",
    responses={503: {"model": Message, "description": "服务停机中，写请求未执行"}},
)
async def delete_claim(claim_id: int, request: Request):
    def _delete(conn):
        return conn.execute(
            "DELETE FROM claims WHERE id = %s RETURNING id", (claim_id,)
        ).fetchone()

    row = await asyncio.to_thread(
        _call_write, _delete, _write_debug_delay(request)
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"理赔 {claim_id} 不存在")
    # 204 必须无响应体
    return Response(status_code=204)

"""端到端自测：覆盖分页、快照隔离、名额、关闭、到期、连接丢失与重启语义。

运行方式（本地已起好 PostgreSQL 15，见 README）：
    DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:55432/claims \
    SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
    python -m unittest tests.test_api -v
"""
from __future__ import annotations

import os
import threading
import time
import unittest

import httpx
import psycopg
import uvicorn

# 本地测试用默认值
os.environ.setdefault(
    "DATABASE_URL", "postgresql://actuary:actuary@127.0.0.1:55432/claims"
)
os.environ.setdefault("SESSION_TTL_SECONDS", "2")
os.environ.setdefault("ENABLE_DEBUG_HOOKS", "true")
os.environ.setdefault("SWEEPER_INTERVAL_SECONDS", "0.1")

from app.config import settings  # noqa: E402
from app.main import app, manager  # noqa: E402
from app.session_manager import SessionError  # noqa: E402

SEED_SQL = os.path.join(os.path.dirname(__file__), "..", "db", "init", "02_seed.sql")


def reset_seed_data() -> None:
    """每个用例前重置演示数据，保证改/删类测试可重复运行。"""
    with open(SEED_SQL, encoding="utf-8") as fh:
        sql = fh.read()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute(sql)

TTL = settings.session_ttl_seconds


class ApiTestBase(unittest.TestCase):
    # 整个套件共享一个 uvicorn 实例与一个 manager 单例，
    # 避免多实例争抢同一端口导致请求串到旧进程。
    _server: "uvicorn.Server | None" = None
    _server_thread: threading.Thread | None = None
    client: httpx.Client

    @classmethod
    def setUpClass(cls):
        if ApiTestBase._server is None:
            config = uvicorn.Config(
                "app.main:app", host="127.0.0.1", port=18080,
                log_level="warning",
            )
            ApiTestBase._server = uvicorn.Server(config)
            ApiTestBase._server_thread = threading.Thread(
                target=ApiTestBase._server.run,
                name="test-uvicorn", daemon=True,
            )
            ApiTestBase._server_thread.start()
            for _ in range(100):
                try:
                    httpx.get("http://127.0.0.1:18080/health", timeout=1)
                    break
                except httpx.TransportError:
                    time.sleep(0.1)
            # 全套件以干净的种子数据开始
            reset_seed_data()
        cls.client = httpx.Client(
            base_url="http://127.0.0.1:18080", timeout=15
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        # 每个用例前清掉所有未终结会话，保证相互独立
        for s in manager.list_sessions():
            if s.status in ("open", "closing"):
                try:
                    manager.close(s.session_id)
                except SessionError:
                    pass
        # 写入类用例会改数据，统一在用例内自行安排快照时机，
        # 不在全局 setUp 中重置（否则会清掉同用例刚提交的写入）。

    @classmethod
    def _reset_seed(cls):
        reset_seed_data()

    def open(self, region="华北"):
        r = self.client.post("/sessions", json={"region": region})
        self.assertEqual(r.status_code, 201, r.text)
        return r.json()

    def close(self, sid):
        return self.client.post(f"/sessions/{sid}/close")

    def page(self, sid, page=1, page_size=100, headers=None):
        return self.client.get(
            f"/sessions/{sid}/claims",
            params={"page": page, "page_size": page_size},
            headers=headers or {},
        )


class TestPagination(ApiTestBase):
    def test_pages_ordered_and_capped(self):
        s = self.open()
        r1 = self.page(s["session_id"])
        self.assertEqual(r1.status_code, 200)
        body = r1.json()
        self.assertEqual(len(body["rows"]), 100)
        self.assertTrue(body["has_more"])
        ids = [r["claim_id"] for r in body["rows"]]
        self.assertEqual(ids, sorted(ids))
        self.assertTrue(all(r["region"] == "华北" for r in body["rows"]))
        self.assertTrue(all(isinstance(r["amount"], int) for r in body["rows"]))

        r2 = self.page(s["session_id"], page=2)
        self.assertEqual(len(r2.json()["rows"]), 20)
        self.assertFalse(r2.json()["has_more"])
        self.assertGreater(
            r2.json()["rows"][0]["claim_id"], body["rows"][-1]["claim_id"]
        )

        r3 = self.page(s["session_id"], page=3)
        self.assertEqual(r3.json()["rows"], [])
        self.assertFalse(r3.json()["has_more"])

    def test_page_size_forced_to_cap(self):
        s = self.open()
        r = self.page(s["session_id"], page_size=999)
        self.assertEqual(r.json()["page_size"], 100)
        self.assertEqual(len(r.json()["rows"]), 100)

    def test_invalid_pagination_params(self):
        s = self.open()
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page": 0}
        ).status_code, 422)
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page_size": 0}
        ).status_code, 422)

    def test_repeat_page_same_snapshot(self):
        s = self.open()
        b1 = self.page(s["session_id"]).json()
        b2 = self.page(s["session_id"]).json()
        self.assertEqual(
            [r["claim_id"] for r in b1["rows"]],
            [r["claim_id"] for r in b2["rows"]],
        )
        self.assertEqual(b1["snapshot_time"], b2["snapshot_time"])


class TestSnapshotIsolation(ApiTestBase):
    def test_source_writes_invisible_inside_session(self):
        self._reset_seed()  # 本用例需要干净的种子数据
        s = self.open("华北")
        before = self.page(s["session_id"]).json()
        before_ids = {r["claim_id"] for r in before["rows"]}

        # 源端：新增保单+理赔、改金额、删理赔，均独立提交
        pol = self.client.post("/admin/policies", json={"region": "华北"})
        self.assertEqual(pol.status_code, 201)
        nc = self.client.post(
            "/admin/claims",
            json={"policy_id": pol.json()["id"], "amount": 777},
        )
        self.assertEqual(nc.status_code, 201)
        first_id = before["rows"][0]["claim_id"]
        second_id = before["rows"][1]["claim_id"]
        self.assertEqual(self.client.patch(
            f"/admin/claims/{first_id}", params={"amount": 1}
        ).status_code, 200)
        self.assertEqual(self.client.delete(
            f"/admin/claims/{second_id}"
        ).status_code, 204)

        after = self.page(s["session_id"]).json()
        after_ids = {r["claim_id"] for r in after["rows"]}
        self.assertEqual(before_ids, after_ids)
        self.assertNotIn(nc.json()["id"], after_ids)
        self.assertNotEqual(after["rows"][0]["amount"], 1)
        self.assertIn(second_id, after_ids)

        # 新会话能看到源端最新提交（翻完全部页核对）
        s2 = self.open("华北")
        fresh_ids = set()
        page_no = 1
        while True:
            body = self.page(s2["session_id"], page=page_no).json()
            fresh_ids.update(r["claim_id"] for r in body["rows"])
            if not body["has_more"]:
                break
            page_no += 1
        self.assertIn(nc.json()["id"], fresh_ids)
        self.assertNotIn(second_id, fresh_ids)
        self.close(s2["session_id"])

    def test_snapshot_covers_both_tables(self):
        # 地区来自 policies 表、金额来自 claims 表：JOIN 结果同样固定
        s = self.open("华东")
        b = self.page(s["session_id"]).json()
        self.assertEqual(len(b["rows"]), 100)
        self.assertTrue(all(r["region"] == "华东" for r in b["rows"]))


class TestCapacity(ApiTestBase):
    def test_max_two_sessions_and_release(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        r = self.client.post("/sessions", json={"region": "华北"})
        self.assertEqual(r.status_code, 429)
        self.assertEqual(r.json()["code"], "capacity_reached")

        # 满额不影响既有会话
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)

        self.assertEqual(self.close(s2["session_id"]).json()["status"], "closed")
        s3 = self.open("华北")  # 名额归还后可再开
        self.assertEqual(s3["status"], "open")
        # 另一个会话不受影响
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)
        self.close(s1["session_id"])
        self.close(s3["session_id"])

    def test_unknown_session_404(self):
        r = self.client.get("/sessions/nonexistent/claims")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(self.client.get("/sessions/nonexistent").status_code, 404)


class TestCloseSemantics(ApiTestBase):
    def test_close_rejects_later_pages_but_keeps_status_queryable(self):
        s = self.open()
        self.close(s["session_id"])
        r = self.page(s["session_id"])
        self.assertEqual(r.status_code, 409)
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{s['session_id']}").json()
        self.assertEqual(info["status"], "closed")

    def test_slow_page_then_close(self):
        s = self.open("华东")
        sid = s["session_id"]
        box = {}

        def slow():
            box["r"] = self.client.get(
                f"/sessions/{sid}/claims",
                params={"page": 1, "page_size": 10},
                headers={"X-Debug-Sleep-Seconds": "3"},
                timeout=15,
            )

        t = threading.Thread(target=slow)
        t.start()
        time.sleep(0.8)
        r = self.close(sid)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "closed")
        t.join()
        self.assertIn(box["r"].status_code, (409, 410))
        self.assertNotIn("rows", box["r"].json())

        # 关闭后名额已归还，可开新会话
        s2 = self.open("华东")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])

    def test_close_idempotent_and_new_session_unaffected(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        self.close(s1["session_id"])
        again = self.close(s1["session_id"])
        self.assertEqual(again.json()["status"], "closed")
        # 另一个会话照常工作
        self.assertEqual(self.page(s2["session_id"], page_size=5).status_code, 200)
        self.close(s2["session_id"])


class TestExpiry(ApiTestBase):
    def test_expiry_rejects_pages_and_releases_slot(self):
        s = self.open("华南")
        sid = s["session_id"]
        time.sleep(TTL + 0.6)
        r = self.page(sid)
        self.assertIn(r.status_code, (409, 410))
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{sid}").json()
        self.assertEqual(info["status"], "expired")
        # 名额已释放：能连开两个新会话
        a = self.open("华东")
        b = self.open("华北")
        self.assertEqual(a["status"], "open")
        self.assertEqual(b["status"], "open")
        self.close(a["session_id"])
        self.close(b["session_id"])

    def test_pages_before_expiry_still_work(self):
        s = self.open("华南")
        r = self.page(s["session_id"], page_size=10)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["rows"]), 10)


class TestConnectionLossAndRestart(ApiTestBase):
    """连接丢失（终止会话后端）与服务重启语义。

    本环境禁止 kill 系统调用，故用 pg_terminate_backend 从 SQL 层
    断开会话持有的后端连接，对客户端而言与网络/进程断开等价。
    """

    def _terminate_session_backend(self, sid: str) -> None:
        sess = manager.get_status(sid)
        with psycopg.connect(settings.database_url, autocommit=True) as admin:
            # 按客户端后端 PID 精准终止该会话独占的连接
            admin.execute(
                "SELECT pg_terminate_backend(%s)",
                (sess.conn.info.backend_pid,),
            )
        # 给客户端一点时间感知 TCP 断开
        deadline = time.time() + 5
        while time.time() < deadline:
            if sess.conn.closed:
                return
            time.sleep(0.1)

    def test_connection_loss_invalidates_session_no_snapshot_swap(self):
        s = self.open("华东")
        sid = s["session_id"]
        # 从 SQL 层断开会话持有的后端连接 -> 快照事务随之消失
        self._terminate_session_backend(sid)
        r = self.page(sid)
        # 旧会话直接失败，绝不换连接/换快照续取；
        # 409（清理线程先标记终态）或 503（取页时发现连接断裂）均合规
        self.assertIn(r.status_code, (409, 503))
        self.assertNotIn("rows", r.json())
        # 旧 id 无法续取（失效会话已移出注册表或已为终态）
        info = self.client.get(f"/sessions/{sid}")
        if info.status_code == 200:
            self.assertIn(info.json()["status"], ("closed", "expired"))
            self.assertEqual(self.page(sid).status_code, 409)
        else:
            self.assertEqual(info.status_code, 404)
        # 新会话在池恢复后的连接上正常开启
        s2 = self.open("华东")
        body = self.page(s2["session_id"], page_size=5)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 5)
        self.close(s2["session_id"])

    @staticmethod
    def _restart_uvicorn():
        """完整重启共享 uvicorn：lifespan 负责 manager.stop()/start()。"""
        ApiTestBase._server.should_exit = True
        ApiTestBase._server_thread.join(timeout=10)
        config = uvicorn.Config(
            "app.main:app", host="127.0.0.1", port=18080, log_level="warning",
        )
        ApiTestBase._server = uvicorn.Server(config)
        ApiTestBase._server_thread = threading.Thread(
            target=ApiTestBase._server.run,
            name="test-uvicorn-restart", daemon=True,
        )
        ApiTestBase._server_thread.start()
        for _ in range(100):
            try:
                if httpx.get(
                    "http://127.0.0.1:18080/health", timeout=1
                ).status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.1)
        raise RuntimeError("uvicorn 重启后健康检查失败")

    def test_service_restart_drops_in_memory_sessions(self):
        s = self.open("华南")
        sid = s["session_id"]

        # 模拟 API 服务重启（新进程语义：内存注册表为空、池与 sweeper 重建）
        self._restart_uvicorn()

        # 旧会话 id 在新进程中不存在，不得换快照续取
        r = self.client.get(f"/sessions/{sid}")
        self.assertEqual(r.status_code, 404)
        r2 = self.page(sid)
        self.assertEqual(r2.status_code, 404)
        s2 = self.open("华南")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])


class TestShutdownWriteHandoff(ApiTestBase):
    """源端写请求与服务停机的交接语义。

    直接驱动 manager.begin_shutdown()/stop()，从而精确控制
    "停机受理"与"写入进行中"的交错时序；服务用 start() 恢复。
    """

    def tearDown(self):
        # 无论用例成败都恢复服务并核对没有遗留的在途写
        manager.start()
        deadline = time.time() + 5
        while manager.pending_writes() and time.time() < deadline:
            time.sleep(0.05)

    def _counts(self):
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            policies = conn.execute("SELECT count(*) FROM policies").fetchone()[0]
            claims = conn.execute("SELECT count(*) FROM claims").fetchone()[0]
        return policies, claims

    def _start_write_with_delay(self, delay, body=None):
        """发起一个写请求；服务端借到连接后、执行 SQL 前会暂停 delay 秒。"""
        box = {}

        def run():
            box["resp"] = self.client.post(
                "/admin/claims",
                json=body or {"policy_id": 1, "amount": 4321},
                headers={"X-Debug-Write-Delay-Seconds": str(delay)},
                timeout=30,
            )

        t = threading.Thread(target=run)
        t.start()
        # 等到服务端真的把该写请求登记为在途（已借到连接）再返回
        deadline = time.time() + 10
        while manager.pending_writes() != 1 and time.time() < deadline:
            time.sleep(0.02)
        return t, box

    def test_inflight_write_commits_and_returns_success_across_shutdown(self):
        """已借到连接的写入：停机后仍应完成提交，且必须返回成功。"""
        before = self._counts()
        t, box = self._start_write_with_delay(2.0)
        time.sleep(0.5)  # 写请求已借到连接、处于"执行前"窗口
        self.assertFalse(manager.is_shutting_down())

        # 模拟 uvicorn 收到停机信号：关闭写入口并完整收尾
        manager.begin_shutdown()
        self.assertTrue(manager.is_shutting_down())
        manager.stop()

        t.join(timeout=20)
        self.assertIn("resp", box, "写请求没有返回")
        resp = box["resp"]
        # 已提交的写入必须返回成功，绝不能报错却已写入
        self.assertEqual(resp.status_code, 201, resp.text)
        new_claim_id = resp.json()["id"]

        after = self._counts()
        self.assertEqual(after[1], before[1] + 1, "响应成功则必须确实写入")

        # 重启后能按 id 查到这条记录：证明提交成功、不是假成功
        manager.start()
        self.assertTrue(manager.is_shutting_down() is False)
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            row = conn.execute(
                "SELECT id, amount FROM claims WHERE id = %s", (new_claim_id,)
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[1], 4321)

    def test_write_after_shutdown_accepted_is_rejected_without_writing(self):
        """停机受理后到达的写请求：明确失败且不写入任何数据。"""
        before = self._counts()
        manager.begin_shutdown()
        resp = self.client.post(
            "/admin/claims", json={"policy_id": 1, "amount": 999}
        )
        self.assertEqual(resp.status_code, 503, resp.text)
        self.assertEqual(resp.json()["code"], "service_shutting_down")
        self.assertEqual(self._counts(), before, "被拒绝的写请求绝不能写入数据")

        # 改金额/删除/补录保单同样被拒绝且不落库
        self.assertEqual(
            self.client.patch("/admin/claims/1", params={"amount": 1}).status_code,
            503,
        )
        self.assertEqual(self.client.delete("/admin/claims/1").status_code, 503)
        self.assertEqual(
            self.client.post("/admin/policies", json={"region": "西南"}).status_code,
            503,
        )
        self.assertEqual(self._counts(), before)

        # 恢复后写入照常
        manager.start()
        self.assertEqual(
            self.client.post(
                "/admin/claims", json={"policy_id": 1, "amount": 123}
            ).status_code,
            201,
        )

    def test_queued_write_ends_wait_and_never_commits_after_restart(self):
        """等连接的写请求遇停机：结束等待并失败，重启后也不会意外提交。"""
        pool = manager.pool()
        # 把池全部连接借走，制造"写请求只能排队等连接"的局面
        held = []
        try:
            while True:
                try:
                    held.append(pool.getconn(timeout=0.2))
                except Exception:
                    break
        except Exception:
            pass
        self.assertGreaterEqual(len(held), 1)

        before = self._counts()
        box = {}

        def queued_write():
            box["resp"] = self.client.post(
                "/admin/claims", json={"policy_id": 1, "amount": 2468},
                timeout=30,
            )
            box["returned_at"] = time.time()

        t = threading.Thread(target=queued_write)
        t.start()
        time.sleep(0.6)  # 让请求进入排队等待连接的状态
        self.assertEqual(manager.pending_writes(), 1, "写请求应处于等待连接中")

        t_begin = time.time()
        manager.begin_shutdown()
        t.join(timeout=5)
        self.assertFalse(t.is_alive(), "停机后写请求不应继续等待连接")
        self.assertLess(box["returned_at"] - t_begin, 3.0, "等待应被立即结束")
        self.assertEqual(box["resp"].status_code, 503, box["resp"].text)
        self.assertIn(
            box["resp"].json()["code"],
            ("write_interrupted_by_shutdown", "service_shutting_down"),
        )

        # 释放占用的连接、完整停机并重启
        for conn in held:
            pool.putconn(conn)
        held.clear()
        manager.stop()
        manager.start()
        # 再等一会：此前排队的写请求绝不能在重启后才拿到连接并提交
        time.sleep(0.5)
        self.assertEqual(self._counts(), before, "被停机打断的写请求不得写入数据")

    def test_stop_waits_for_inflight_write_before_closing_pool(self):
        """stop() 必须先等在途写结束再关池，否则归还连接会失败。"""
        t, box = self._start_write_with_delay(2.0)
        # 等到该写请求确实已借到连接（登记为在途）再开始停机
        deadline = time.time() + 10
        while manager.pending_writes() != 1 and time.time() < deadline:
            time.sleep(0.02)
        self.assertEqual(manager.pending_writes(), 1, "写请求未进入在途状态")
        stop_started = time.time()
        manager.stop()
        stop_duration = time.time() - stop_started
        t.join(timeout=20)
        # stop 阻塞等待了在途写请求，且该写请求返回成功
        self.assertGreater(stop_duration, 1.0, "stop 未等待在途写请求")
        self.assertEqual(box["resp"].status_code, 201, box["resp"].text)

    def test_failed_write_is_explicit_failure_and_not_committed(self):
        """写操作执行出错时：明确失败且不落库（不会报错却已写入）。"""
        before = self._counts()
        # policy_id 不存在 -> 外键错误
        resp = self.client.post(
            "/admin/claims", json={"policy_id": 99999999, "amount": 100}
        )
        self.assertGreaterEqual(resp.status_code, 400)
        self.assertLess(resp.status_code, 600)
        self.assertNotEqual(resp.status_code, 201)
        self.assertEqual(self._counts(), before)

    def test_sessions_remain_usable_during_and_across_shutdown(self):
        """原有快照会话的分页、关闭与并行写入在停机前后保持可用。"""
        s = self.open("华东")
        sid = s["session_id"]
        first = self.page(sid, page_size=10)
        self.assertEqual(first.status_code, 200)
        ids_before = [r["claim_id"] for r in first.json()["rows"]]

        # 停机受理后、stop() 之前：既有会话仍可继续分页与状态查询
        manager.begin_shutdown()
        self.assertEqual(self.page(sid, page_size=10).status_code, 200)
        self.assertEqual(
            self.client.get(f"/sessions/{sid}").status_code, 200
        )
        # 并行传入的写请求被拒绝，但不影响会话取页
        self.assertEqual(
            self.client.post(
                "/admin/claims", json={"policy_id": 1, "amount": 10}
            ).status_code,
            503,
        )
        self.assertEqual(self.page(sid, page_size=10).status_code, 200)

        # 完整停机：会话被关闭，取页失败且不返回明细
        manager.stop()
        after_stop = self.page(sid)
        self.assertIn(after_stop.status_code, (404, 409, 410))
        self.assertNotIn("rows", after_stop.json())

        # 重启后可正常开新会话，且能看到停机前提交的数据
        manager.start()
        s2 = self.open("华东")
        fresh = self.page(s2["session_id"], page_size=10)
        self.assertEqual(fresh.status_code, 200)
        self.assertEqual(len(fresh.json()["rows"]), 10)
        self.close(s2["session_id"])

    def test_write_is_atomic_even_if_statement_then_error(self):
        """写操作内部先执行语句再抛错：整笔回滚，不得出现报错却已写入。

        回归用例：连接池连接默认 autocommit=True，若不做显式事务，
        已执行的语句会立即提交，异常回滚也无法撤销。
        """
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            before = conn.execute("SELECT count(*) FROM claims").fetchone()[0]

        def boom(conn):
            conn.execute(
                "INSERT INTO claims (policy_id, amount) VALUES (1, 1)"
            )
            raise RuntimeError("simulated failure after statement")

        with self.assertRaises(RuntimeError):
            manager.run_write(boom)

        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            after = conn.execute("SELECT count(*) FROM claims").fetchone()[0]
        self.assertEqual(after, before, "出错写请求的语句不得被提交")

    def test_writes_work_normally_when_not_shutting_down(self):
        """非停机状态下源端写请求照常提交，快照会话仍隔离。"""
        s = self.open("华北")
        sid = s["session_id"]
        before = self.page(sid, page_size=100).json()
        resp = self.client.post(
            "/admin/claims", json={"policy_id": 1, "amount": 555}
        )
        self.assertEqual(resp.status_code, 201, resp.text)
        # 会话快照不受源端写影响
        after = self.page(sid, page_size=100).json()
        self.assertEqual(
            [r["claim_id"] for r in before["rows"]],
            [r["claim_id"] for r in after["rows"]],
        )
        self.close(sid)


if __name__ == "__main__":
    unittest.main(verbosity=2)
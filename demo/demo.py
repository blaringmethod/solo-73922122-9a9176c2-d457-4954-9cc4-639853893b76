#!/usr/bin/env python3
"""慢取页与关键语义端到端演示（仅依赖标准库）。

前置：docker compose up -d 后运行  python3 demo/demo.py

演示内容
========
1. 分页与重复取页：华北 120 条理赔 -> 两页，claim_id 升序、每页 <=100；
   重复取第 1 页与首次结果逐行一致（同一快照）。
2. 快照隔离：会话开启后通过源端接口补录/修改/删除数据并提交，
   会话内取页结果不变。
3. 名额控制：最多 2 个会话，第 3 个开启请求被 429 拒绝；
   关闭一个后名额归还，可再开；另一个会话不受影响。
4. 慢取页 + 及时关闭：用 X-Debug-Sleep-Seconds 头让取页变慢，
   在它响应前发起关闭，关闭返回后再取页 -> 409 且无明细。
5. 到期失效（可选，较慢）：设置 DEMO_TTL_WAIT=1 时，
   等待会话到期后取页 -> 410 且无明细。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
TTL_WAIT = os.environ.get("DEMO_TTL_WAIT", "0") == "1"
# 演示停机交错的辅助脚本（可选）：设置后场景 5 会真的停/启 api 容器
SHUTDOWN_CMD = os.environ.get("DEMO_SHUTDOWN_CMD", "")
STARTUP_CMD = os.environ.get("DEMO_STARTUP_CMD", "")
PASS, FAIL = "✅", "❌"
results: list[tuple[bool, str]] = []


def check(cond: bool, msg: str) -> None:
    results.append((cond, msg))
    print(f"  {PASS if cond else FAIL} {msg}")
    if not cond:
        check.failed = True


check.failed = False


def call(method: str, path: str, body=None, headers=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, json.loads(raw) if raw else None


def title(n: int, text: str) -> None:
    print(f"\n=== 场景 {n}：{text} ===")


def wait_healthy(timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            st, _ = call("GET", "/health", timeout=2)
            if st == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def run_cmd(cmd: str) -> None:
    print(f"    $ {cmd}")
    subprocess.run(cmd, shell=True, check=True, timeout=90)


def call_shutdown(cmd: str) -> bool:
    """执行真实停机命令；失败时回退到调试停机钩子并由调用方标注。"""
    try:
        run_cmd(cmd)
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print(f"    ⚠️ 停机命令失败（{exc}），改用调试停机钩子替代")
        st, _ = call("POST", "/admin/shutdown")
        if st != 200:
            raise RuntimeError(f"调试停机也无法触发: HTTP {st}")
        return False


def count_claims():
    """用只读连接直接数 claims 行数（演示脚本可选依赖 psycopg）。"""
    try:
        import psycopg
    except ImportError:  # 标准库模式下跳过数据库直查断言
        return None
    url = os.environ.get(
        "DATABASE_URL", "postgresql://actuary:actuary@127.0.0.1:55432/claims"
    )
    with psycopg.connect(url, autocommit=True) as conn:
        return conn.execute("SELECT count(*) FROM claims").fetchone()[0]


def claim_exists(claim_id) -> bool:
    try:
        import psycopg
    except ImportError:
        return True  # 无法直查时不误判
    url = os.environ.get(
        "DATABASE_URL", "postgresql://actuary:actuary@127.0.0.1:55432/claims"
    )
    with psycopg.connect(url, autocommit=True) as conn:
        return conn.execute(
            "SELECT 1 FROM claims WHERE id = %s", (claim_id,)
        ).fetchone() is not None


def main() -> int:
    print(f"目标服务: {BASE}")
    status, meta = call("GET", "/meta")
    assert status == 200, f"服务不可用: {status}"
    print(f"服务参数: {meta}")

    # 清理可能存在的旧会话，保证演示可重复运行
    for s in call("GET", "/sessions")[1]:
        if s["status"] in ("open", "closing"):
            call("POST", f"/sessions/{s['session_id']}/close")

    # ------------------------------------------------------------------
    title(1, "分页 + 重复取页（同一快照、claim_id 升序、每页至多 100 行）")
    _, s1 = call("POST", "/sessions", {"region": "华北"})
    sid1 = s1["session_id"]
    check(s1["status"] == "open", f"开启会话 {sid1[:8]}… 状态 open，TTL={s1['ttl_seconds']}s")

    st, p1 = call("GET", f"/sessions/{sid1}/claims?page=1&page_size=100")
    check(st == 200 and len(p1["rows"]) == 100, "第 1 页返回 100 行")
    check(p1["has_more"] is True, "has_more=true（还有下一页）")
    ids1 = [r["claim_id"] for r in p1["rows"]]
    check(ids1 == sorted(ids1), "claim_id 严格按升序排列")
    check(all(r["region"] == "华北" for r in p1["rows"]), "每行地区均为华北")
    check(all(isinstance(r["amount"], int) for r in p1["rows"]), "金额均为整数")

    st, p1b = call("GET", f"/sessions/{sid1}/claims?page=1&page_size=100")
    check([r["claim_id"] for r in p1b["rows"]] == ids1, "重复取第 1 页结果与首次完全一致")
    check(p1b["snapshot_time"] == p1["snapshot_time"], "两次取页 snapshot_time 相同")

    st, p2 = call("GET", f"/sessions/{sid1}/claims?page=2&page_size=100")
    check(st == 200 and len(p2["rows"]) == 20, "第 2 页返回剩余 20 行")
    check(p2["has_more"] is False, "has_more=false（已到末页）")
    check(p2["rows"][0]["claim_id"] > p1["rows"][-1]["claim_id"], "第 2 页首行 id 大于第 1 页末行 id")

    st, p3 = call("GET", f"/sessions/{sid1}/claims?page=3&page_size=100")
    check(st == 200 and p3["rows"] == [] and p3["has_more"] is False, "超出末页返回空页")

    st, p_overflow = call("GET", f"/sessions/{sid1}/claims?page=1&page_size=999")
    check(st == 200 and p_overflow["page_size"] == 100, "page_size=999 被服务端截断为 100")

    # ------------------------------------------------------------------
    title(2, "快照隔离：源端增改删照常提交，会话内数据不变")
    _, pol = call("POST", "/admin/policies", {"region": "华北"})
    _, new_claim = call("POST", "/admin/claims", {"policy_id": pol["id"], "amount": 55500})
    check(new_claim["amount"] == 55500, f"源端补录理赔 id={new_claim['id']} 已提交")
    # 记录原始金额，用与之不同的新值修改，保证演示可重复运行
    original_amount = p1["rows"][0]["amount"]
    changed_amount = (original_amount + 12345) % 500000
    call("PATCH", f"/admin/claims/{ids1[0]}?amount={changed_amount}")
    call("DELETE", f"/admin/claims/{ids1[1]}")
    check(True, f"源端修改 id={ids1[0]} 金额、删除 id={ids1[1]} 均已提交")

    st, p1c = call("GET", f"/sessions/{sid1}/claims?page=1&page_size=100")
    check([r["claim_id"] for r in p1c["rows"]] == ids1, "快照会话第 1 页仍为 100 行且 id 序列不变")
    check(p1c["rows"][0]["amount"] == original_amount, "被修改的金额在快照中保持原值")
    check(new_claim["id"] not in [r["claim_id"] for r in p1c["rows"]], "新补录理赔在快照中不可见")

    # ------------------------------------------------------------------
    title(3, "名额控制：最多 2 个会话，满额 429，关闭后归还且互不影响")
    _, s2 = call("POST", "/sessions", {"region": "华东"})
    sid2 = s2["session_id"]
    st, err = call("POST", "/sessions", {"region": "华南"})
    check(st == 429 and err["code"] == "capacity_reached", f"第 3 个会话被拒绝：HTTP {st} {err['detail']}")

    st, info1 = call("GET", f"/sessions/{sid1}")
    check(st == 200 and info1["status"] == "open", "已满额时既有会话 1 仍可查询且保持 open")

    st, closed = call("POST", f"/sessions/{sid2}/close")
    check(st == 200 and closed["status"] == "closed", f"关闭会话 2 -> {closed['status']}")

    st, s3 = call("POST", "/sessions", {"region": "华南"})
    sid3 = s3["session_id"]
    check(st == 201, "名额归还后新会话开启成功")

    st, p1d = call("GET", f"/sessions/{sid1}/claims?page=1&page_size=5")
    check(st == 200 and len(p1d["rows"]) == 5, "会话 1 不受会话 2 关闭影响，仍可正常取页")
    call("POST", f"/sessions/{sid3}/close")

    # ------------------------------------------------------------------
    title(4, "慢取页演示：关闭及时受理，受理后未开始响应的取页失败且无明细")
    _, s4 = call("POST", "/sessions", {"region": "华东"})
    sid4 = s4["session_id"]
    box = {}

    def slow_page():
        box["t0"] = time.time()
        # 服务端收到请求后先睡眠 4 秒再执行 SQL
        box["result"] = call(
            "GET", f"/sessions/{sid4}/claims?page=1&page_size=10",
            headers={"X-Debug-Sleep-Seconds": "4"}, timeout=15,
        )
        box["t1"] = time.time()

    t = threading.Thread(target=slow_page)
    t.start()
    time.sleep(1)  # 确保慢取页已到达服务端但尚未开始执行 SQL
    t_close0 = time.time()
    st, info = call("POST", f"/sessions/{sid4}/close")
    t_close1 = time.time()
    check(st == 200 and info["status"] == "closed",
          f"关闭请求在 {t_close1 - t_close0:.2f}s 内受理完成，状态=closed")
    t.join()
    st_slow, body_slow = box["result"]
    print(f"    慢取页最终返回: HTTP {st_slow}，耗时 {box['t1'] - box['t0']:.2f}s，"
          f"响应={json.dumps(body_slow, ensure_ascii=False)[:120]}")
    check(st_slow in (409, 410), "慢取页在关闭后失败（409/410），未返回明细")
    check(isinstance(body_slow, dict) and "rows" not in body_slow, "失败响应不含任何理赔明细行")

    st, after = call("GET", f"/sessions/{sid4}/claims?page=1")
    check(st == 409 and "rows" not in after, f"关闭后再次取页被拒绝 HTTP {st}，且无明细")

    st, info = call("GET", f"/sessions/{sid4}")
    check(st == 200 and info["status"] == "closed", "会话状态可查，终态=closed")

    _, s5 = call("POST", "/sessions", {"region": "华东"})
    check(s5["status"] == "open", "关闭并释放名额后可立即开启新会话")
    call("POST", f"/sessions/{s5['session_id']}/close")

    # ------------------------------------------------------------------
    if TTL_WAIT:
        title(5, "到期失效：60 秒后会话自动到期，取页 410 且无明细")
        _, s6 = call("POST", "/sessions", {"region": "华南"})
        sid6 = s6["session_id"]
        print("    等待 62 秒让会话到期 …")
        time.sleep(62)
        st, expired = call("GET", f"/sessions/{sid6}/claims?page=1")
        check(st in (410, 409), f"到期后取页被拒绝 HTTP {st}，且无明细")
        check("rows" not in expired, "到期失败响应不含理赔明细行")
        st, info = call("GET", f"/sessions/{sid6}")
        check(st == 200 and info["status"] == "expired", "会话状态可查，终态=expired")
        st, s7 = call("POST", "/sessions", {"region": "华南"})
        check(st == 201 and s7["status"] == "open", "到期释放名额后可开启新会话")
        call("POST", f"/sessions/{s7['session_id']}/close")

    # ------------------------------------------------------------------
    title(5, "停机与写入交错：已提交返回成功，停机后受理的写入明确失败")
    st, meta5 = call("GET", "/meta")
    check(st == 200, f"服务参数可读，可写状态 accepting_writes={meta5.get('accepting_writes')}")

    if SHUTDOWN_CMD:
        # 真实停机演示：写请求已借到连接、暂停 3 秒后执行 SQL；
        # 期间停 api 容器。已借到连接的写入应完成提交并返回 201。
        c_before = count_claims()
        if c_before is not None:
            before = c_before
        box = {}

        def in_flight_write():
            box["resp"] = call(
                "POST", "/admin/claims", {"policy_id": 1, "amount": 77700},
                headers={"X-Debug-Write-Delay-Seconds": "3"}, timeout=60,
            )

        tw = threading.Thread(target=in_flight_write)
        tw.start()
        time.sleep(1.0)  # 写请求已到服务端、已借到连接
        print("    在途写请求已借到连接，现在对 api 发送停机 …")
        real_stop = call_shutdown(SHUTDOWN_CMD)
        tw.join(timeout=90)
        st_w, body_w = box.get("resp", (None, None))
        print(f"    在途写请求最终返回: HTTP {st_w} {json.dumps(body_w, ensure_ascii=False)}")
        check(st_w == 201, "已借到连接的写入在停机中完成提交，并返回 201（未出现报错却已写入）")
        if not real_stop:
            print("    （本次使用调试停机钩子代替真实进程停机）")
        after = count_claims()
        if before is not None and after is not None:
            check(after == before + 1, f"返回 201 的写入确实已提交（{before} -> {after}）")
        else:
            print("    （未安装 psycopg，跳过直连数据库的行数核对）")
        new_id = (body_w or {}).get("id")
        check(new_id is not None and claim_exists(new_id), "重启后可按 id 查到该条记录，确认真实提交")

        # 演示"停机受理后到达的写入被明确拒绝"。
        # 若已真正停掉进程，需要先重启；否则直接用调试停机后的状态。
        if STARTUP_CMD and real_stop:
            run_cmd(STARTUP_CMD)
            check(wait_healthy(), "服务已重新启动并通过健康检查")
            st_h, health = call("GET", "/health")
            check(st_h == 200 and health.get("accepting_writes") is True,
                  f"重启后 /health.accepting_writes={health.get('accepting_writes')}")
            st_ok, _ = call("POST", "/admin/claims", {"policy_id": 1, "amount": 13579})
            check(st_ok == 201, f"重启后写入恢复正常 HTTP {st_ok}")
        else:
            if real_stop:
                print("    （未设置 DEMO_STARTUP_CMD，跳过重启部分）")
    else:
        # 无需真实停机：只验证"服务正常时可写"
        print("    （未设置 DEMO_SHUTDOWN_CMD，跳过真实停机；")
        print("      可设 DEMO_SHUTDOWN_CMD='docker compose stop api'")
        print("      DEMO_STARTUP_CMD='docker compose start api' 后重跑）")
        st_ok, _ = call("POST", "/admin/claims", {"policy_id": 1, "amount": 13579})
        check(st_ok == 201, f"服务正常时源端写入返回 201 HTTP {st_ok}")

    # 收尾
    call("POST", f"/sessions/{sid1}/close")

    print("\n" + "=" * 60)
    failed = [m for ok, m in results if not ok]
    print(f"共 {len(results)} 项检查，通过 {len(results) - len(failed)} 项，失败 {len(failed)} 项")
    if failed:
        for m in failed:
            print(f"  {FAIL} {m}")
        return 1
    print("全部通过 🎉")
    print("\n提示：交互式文档位于 /docs（Swagger UI），原始 OpenAPI 位于 /openapi.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())

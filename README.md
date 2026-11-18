# 理赔明细分批提取服务（FastAPI + PostgreSQL）

保险精算离线测算场景：按地区开启**快照会话**，按理赔 id 升序分页提取明细。
会话期间保单/理赔两表固定为开启时刻的同一快照，源端的增改删照常提交、
互不影响；会话有数量上限与 TTL，可主动关闭，关闭/到期后未开始响应的取页
一律失败且不返回明细。

## 一、快速启动（Docker Compose）

```bash
docker compose up --build
```

- API：<http://localhost:8000>
- Swagger 交互文档：<http://localhost:8000/docs>
- OpenAPI 原文：<http://localhost:8000/openapi.json>
- PostgreSQL：localhost:55432（库 `claims`，用户/密码 `actuary/actuary`）

首次启动时 `db/init/` 下脚本按文件名顺序自动执行：

| 文件 | 作用 |
| --- | --- |
| `01_schema.sql` | 建表：`policies(id, region)`、`claims(id, policy_id, amount)`，金额为非负整数（单位：分） |
| `02_seed.sql` | 演示数据：3 个地区、260 条理赔（华北 120 / 华东 100 / 华南 40） |
| `03_sql_examples.sql` | SQL 补录示例：新增保单/理赔、改金额、删除、按地区汇总 |

## 二、核心语义与实现方式

| 需求 | 实现 |
| --- | --- |
| 两表同一快照、后页/重复页一致 | 每会话独占一条连接，开启 `START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`，连接保持 idle-in-transaction，快照事务全程不结束 |
| 源端增改删照常提交 | 写操作走连接池内其它自动提交连接，MVCC 下与快照只读事务互不阻塞 |
| 不预存结果全集 | 每次取页现场执行带 `LIMIT/OFFSET` 的 SQL，服务端不缓存明细 |
| claim_id 升序、每页至多 100 行 | `ORDER BY c.id ASC LIMIT n+1 OFFSET ...`，多取 1 行判断 `has_more`；`page_size` 服务端强制截断为 100 |
| 最多 2 个会话，满额拒绝 | 内存注册表 + 名额计数，超额返回 `429 capacity_reached`（可用 `MAX_SESSIONS` 调整） |
| 开启 60 秒到期 | 后台 sweeper 线程定时清理（`SESSION_TTL_SECONDS` 可调），到期取页返回 410/409 且无明细 |
| 主动关闭及时受理 | 关闭请求先把状态翻为 `closing`（新取页立即失败），等待在途取页结束后回滚事务、归还连接、释放名额，再回报 `closed` |
| 不影响另一会话/后续新会话 | 名额只统计 open/closing 会话；终态不占名额，连接归还池后立即可复用 |
| 连接丢失/服务重启后旧会话失效 | 会话状态只存进程内存；连接断裂的会话作废返回 503 且绝不换快照续取；重启后旧 id 查询 404 |

### 停机与源端写入的交接

写入接口统一经 `manager.run_write()`，用"准入闸门 + 在途计数"与停机交接：

| 时序 | 行为 |
| --- | --- |
| 停机受理**后**到达的写请求 | 在执行任何 SQL **之前**即被拒绝，返回 `503 service_shutting_down`，数据库无任何变更，可安全重试 |
| 停机时**已借到连接**的写请求 | 不被打断：执行完自己的语句并确认已提交，**返回成功后才归还连接**；绝不出现"报错却已写入" |
| 写操作执行出错 | 显式回滚，返回明确失败（外键等约束冲突为 `409 write_rejected`），不会写入 |
| 停机时**正在排队等连接**的写请求 | 等待被立即打断，返回 `503 write_interrupted_by_shutdown`，未执行 SQL；**不会在重启后才拿到连接意外提交** |
| `stop()` 收尾顺序 | ① 关写入口 → ② 等已借到连接的写请求提交/回滚并归还（上限 `SHUTDOWN_DRAIN_TIMEOUT_SECONDS`）→ ③ 关闭快照会话 → ④ 最后关闭连接池 |

`stop()` 一定在关闭连接池**之前**等在途写结束，因此归还连接时池仍然可用；
即使 drain 超时，`pool.close()` 也只会在连接归还时关闭它，不会打断已开始的提交。

> 与容器停机的配合：Dockerfile 里 uvicorn 使用 `--timeout-graceful-shutdown 30`，
> compose 里 `stop_grace_period: 40s`，均大于 `SHUTDOWN_DRAIN_TIMEOUT_SECONDS`，
> 保证 `docker stop` / `compose down` 不会在写请求提交前用 SIGKILL 打断。
> uvicorn 收到 SIGTERM 会先让在途 HTTP 请求跑完，再进入 lifespan shutdown（即 `manager.stop()`）。

服务健康检查 `/health` 返回 `accepting_writes` 与 `pending_writes`，可在停机受理后
据此停止发送新的写请求。

### 会话状态机

```
open ──主动关闭──▶ closing ──回滚事务/还连接/释放名额──▶ closed
  │                                                  ▲
  └──TTL 到期（sweeper）─────────────────────────── expired
```

终态记录保留 10 分钟供状态查询，之后从注册表回收；终态不占用会话名额。

## 三、API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sessions` | 按地区开启快照会话，body：`{"region": "华北"}` |
| GET | `/sessions` | 列出会话 |
| GET | `/sessions/{id}` | 查询会话状态 |
| POST | `/sessions/{id}/close` | 主动关闭 |
| GET | `/sessions/{id}/claims?page=1&page_size=100` | 快照内分页取理赔明细 |
| POST | `/admin/policies` | 源端补录保单 |
| POST | `/admin/claims` | 源端补录理赔 |
| PATCH | `/admin/claims/{id}?amount=999` | 源端改金额 |
| DELETE | `/admin/claims/{id}` | 源端删除理赔 |
| GET | `/health`、`/meta` | 健康检查（含 `accepting_writes`/`pending_writes`）、服务参数 |
| POST | `/admin/shutdown` | **[调试]** 受理一次停机（关闭写入口并有序收尾）；仅 `ENABLE_DEBUG_HOOKS=true` 时可用，生产请用 SIGTERM |

取页失败响应只含 `detail`/`code`，不含任何明细行：

```json
HTTP 409
{"detail": "会话 ... 关闭已受理或会话已结束，取页失败且不返回明细",
 "code": "session_not_open"}
```

写入失败同样只含 `detail`/`code`，并明确区分"未执行"与"已回滚"：

```json
HTTP 503
{"detail": "服务正在停机，写请求未被执行（未写入任何数据），可安全地在服务恢复后重试",
 "code": "service_shutting_down"}
```

## 四、停机与写入交错演示

镜像默认 `ENABLE_DEBUG_HOOKS=true`：

- `X-Debug-Sleep-Seconds`：让本次**取页**在执行 SQL 前先睡眠，演示"关闭及时受理"。
- `X-Debug-Write-Delay-Seconds`：让**写请求**在已借到连接、执行 SQL 前暂停，
  用于制造"写入进行中恰逢停机"的交错。

```bash
python3 demo/demo.py                 # 完整端到端演示（标准库即可运行）
BASE_URL=http://localhost:8000 python3 demo/demo.py
DEMO_TTL_WAIT=1 python3 demo/demo.py # 追加等待到期场景（约多花 1 分钟）
```

演示覆盖：分页与重复取页一致性、源端写入的快照隔离、名额满 429、
慢取页期间关闭（在途请求结束后失败、无明细）、到期失效，以及
**停机与写入交错**（场景 5）。

### 真实停机交错（docker compose）

```bash
# 终端 A：启动服务
docker compose up --build

# 终端 B：跑演示，并在场景 5 真实停/启 api 容器
pip install psycopg[binary]          # 可选：用于直连数据库核对行数
DEMO_SHUTDOWN_CMD="docker compose stop api" \
DEMO_STARTUP_CMD="docker compose start api" \
DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:55432/claims \
python3 demo/demo.py
```

场景 5 做三件事，并逐项断言：

1. 发起一个"已借到连接、3 秒后执行 SQL"的写请求；
2. 在其执行期间 `docker compose stop api`（SIGTERM → 优雅停机）；
3. 校验：该写请求返回 **201**，且重启后按返回的 id 能查到记录（确认真实提交）；
   停机后再发的写请求返回 **503** 且行数不变。

若 `DEMO_SHUTDOWN_CMD` 执行失败（如无 docker 环境），演示会改用
`POST /admin/shutdown` 触发同一套停机收尾流程，并明确标注为替代方式
（该接口无法让命令行 uvicorn 进程退出，仅关闭写入口并收尾；真实演示请用
`docker compose stop`）。**注意**：`/admin/shutdown` 是调试接口，
生产环境请用 SIGTERM / `docker stop`。

## 五、本地自测（无 Docker 时）

仓库测试以真实 uvicorn + PostgreSQL 运行：

```bash
pip install -r requirements.txt
# 准备一个 PostgreSQL 15，并执行 db/init/01_schema.sql、02_seed.sql
DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:5432/claims \
SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
python -m unittest tests.test_api -v
```

22 个用例覆盖：分页/上限/参数校验、重复取页同快照、两表 JOIN 快照、
源端增改删不可见、名额与释放、关闭幂等、慢取页+关闭竞争、
到期清理、连接丢失作废不续取、服务重启旧会话 404，以及停机交接：
在途写入跨停机提交并返回成功、停机后写入被拒绝且不落库、
排队写入被打断且重启后不提交、`stop()` 先等在途写再关池、
写失败回滚不落库、停机前后快照会话仍可用。

## 六、配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://actuary:actuary@db:5432/claims` | 数据库连接串 |
| `MAX_SESSIONS` | `2` | 并发会话上限 |
| `SESSION_TTL_SECONDS` | `60` | 会话存活秒数 |
| `PAGE_SIZE_CAP` | `100` | 单页行数硬上限 |
| `ENABLE_DEBUG_HOOKS` | `false`（compose 中为 true） | 是否允许慢取页/慢写入/停机调试钩子 |
| `SWEEPER_INTERVAL_SECONDS` | `0.25` | 到期检查间隔 |
| `WRITE_ACQUIRE_TIMEOUT_SECONDS` | `10` | 写请求借连接的最长等待；超时明确失败（未执行 SQL） |
| `SHUTDOWN_DRAIN_TIMEOUT_SECONDS` | `15` | 停机时等待在途写请求提交/回滚的上限 |

> 说明：服务固定单 uvicorn worker——会话注册表与名额是进程内状态，
> 多 worker 会各自持有独立状态，破坏会话语义。
>
> 停机相关：uvicorn 的 `--timeout-graceful-shutdown`（Dockerfile 中 30s）与
> compose 的 `stop_grace_period`（40s）都应大于
> `SHUTDOWN_DRAIN_TIMEOUT_SECONDS`，否则进程可能在写请求提交前被 SIGKILL。

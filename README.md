# 制品元数据熔断网关

部署系统读取制品元数据时，上游故障会引发密集重试。本项目是一个 **FastAPI + SQLite
的双 worker 熔断网关**：按制品名把 `GET` 查询转发给本地目录服务，慢查询不会阻塞其他
查询，两个 worker 进程共享同一份熔断状态，重启后状态仍然保留。

熔断与恢复还会产生**可靠告警**：状态事件先落入网关的 SQLite 出盒（与状态变更同事务），
再由后台派发器投递到本地告警接收器；接收器拒收或网关重启都不会丢事件，重试也不会在
接收器留下重复记录。

打开 <http://localhost:8000/> 即可使用网页：并发查询、查看元数据与熔断状态、切换示例
上游行为（正常 / 延迟 / 503）、切换告警接收器（接收 / 拒收）并查看待发与已收告警，
并对比「网关转发次数」与「上游实际访问次数」。

## 快速启动

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/
```

需要 Docker 与 Docker Compose v2。Compose 会构建三个服务：

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `upstream` | 示例本地目录服务，提供 `/artifacts/{name}`、模式控制与访问计数 | 仅集群内 9000 |
| `receiver` | 本地告警接收器，幂等存储状态事件，可切换拒收演练 | 仅集群内 9001 |
| `gateway` | 熔断网关，`uvicorn --workers 2`，SQLite 位于命名卷 `breaker-data` | 宿主机 8000 |

`gateway` 使用命名卷挂载 `/data`（告警出盒与熔断状态同在 `breaker.db`），`receiver`
使用命名卷 `receiver-data`；容器重启或重建后熔断状态、待发事件与已收记录都在。

### 不使用 Docker 的本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export UPSTREAM_URL=http://127.0.0.1:9000 RECEIVER_URL=http://127.0.0.1:9001 GATEWAY_DB=./data/breaker.db
python -m uvicorn upstream:app --port 9000 &
python -m uvicorn receiver:app --port 9001 &
python -m uvicorn app:app --port 8000 --workers 2
```

## 熔断语义

状态按**制品名**独立维护，存放在 SQLite（WAL、写事务 `BEGIN IMMEDIATE`），因此对两个
worker 进程共享，并对重启持久。

* **CLOSED**：转发请求。失败（连接失败 / 超时 / 5xx）按**完成顺序**连续计数；
  其它响应（2xx/3xx/4xx）把计数清零。连续第 3 次失败立即熔断。
* **OPEN**：所有请求直接返回 **503**（带 `Retry-After`），**不访问上游**。
  冷却 **2 秒**后，恰好一个请求通过原子 CAS 成为唯一的**恢复探测**；其余请求继续 503。
* **HALF_OPEN**：只有一个探测在飞。探测成功 → CLOSED 并清零；探测失败或超时 →
  重新 OPEN 并重新计算 2 秒冷却。探测进程崩溃后，探测槽位在租约到期 / 重启时释放，
  可以再次探测。
* 每次调用总时长上限 **1 秒**（`asyncio.wait_for`），连接、读取、解析合计计算。

### 旧结果不会覆盖新结论

* 准入时记录该次调用所属的 **epoch**。若调用期间熔断已打开（epoch 变化），这次结果
  被视为过期：**熔断前发出、熔断后才成功的慢请求既不会清零失败数，也不会让熔断恢复**
  （响应中标记 `stale_success_ignored`）。
* 探测结果携带 **probe generation**。旧代次探测迟到时被丢弃（`stale_probe_ignored`），
  不会覆盖新代次的判定。
* 探测者进程退出后，`HALF_OPEN` 记录被识别为无主，冷却结束后可再次探测。

### 并发与阻塞

* 每个 worker 是独立的 asyncio 事件循环，上游调用全程异步；慢查询不会阻塞同 worker
  内的其他查询。
* 上游 HTTP 连接在进程内复用连接池。
* SQLite 写事务放在线程池执行；常态放行与熔断拒绝都是只读快路径，不占用写锁。

## 告警事件与可靠投递

值班工程师关心的三种状态变化会生成**状态事件**（制品名、故障代次、原因）：

| 事件 | 触发 | 原因内容 |
| --- | --- | --- |
| `opened` | 连续失败达到阈值**首次熔断** | 连续失败次数与最后一次错误 |
| `recovered` | 恢复探测成功、熔断关闭 | 探测结果 |
| `reset` | 人工重置时该制品仍处于熔断/半开 | `manual reset` |

* **故障代次**：每个制品的故障代次单调递增（`fault_counter` 表，重置不清零）。失败的
  探测仍属原故障，重新打开不会产生新事件；并发请求竞争时，只有真正完成状态翻转的那
  个事务生成事件，因此同一故障只有一条 `opened`。
* **旧代次不告警**：迟到的旧探测代次 / 熔断前发出的慢成功会被状态机忽略（见上文），
  被忽略的结果不生成任何事件。
* **出盒模式**：事件与熔断状态变更在**同一个 SQLite 事务**提交。状态已提交而接收器
  拒收、不可达或网关重启时，事件仍在出盒中，后台派发器按指数退避持续重试，直到接收
  器确认。
* **幂等投递**：事件 ID 为 `制品#代次:类型`，接收器以此去重。接收器已收到但确认丢失
  时，重试只更新同一条记录的 `deliveries` 计数，不会新增记录。
* **顺序**：同一制品的事件按发生顺序送达——只有该制品最早的未送达事件可被投递，
  恢复事件不会越过仍在待发的熔断事件；不同制品的投递互不影响。
* **单派发者**：两个 worker 通过 `dispatcher_lease` 租约选出一个派发者，租约到期
  （进程消失）后另一个接管，不会并发重复派发。

网页的「告警接收器」区可切换接收器 **接收 / 拒收**（演练拒收），并查看**待发事件**
（出盒）、**接收器已收记录**与**最近已送达**。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/meta/{artifact}` | 经熔断器查询元数据。成功 200，上游失败 502，熔断 503 |
| GET | `/api/breakers` | 所有制品的熔断状态、最近事件、转发计数 |
| POST | `/api/reset` | 清空熔断状态与计数（演示用）；对仍处故障的制品生成 `reset` 告警 |
| GET/POST | `/api/upstream-mode` | 读取 / 设置示例上游模式：`normal`、`delay`、`503`，可带 `delay` 秒数 |
| GET | `/api/upstream-stats` | 上游实际收到的 `/artifacts` 请求数（地面真值） |
| POST | `/api/upstream-stats/reset` | 清零上游计数 |
| GET | `/api/alerts` | 告警出盒（待发 / 最近已送达）+ 接收器视图（模式、已收记录） |
| POST | `/api/receiver-mode` | 切换接收器 `accept` / `reject`（拒收演练） |
| POST | `/api/receiver-reset` | 清空接收器已收记录（演示用） |
| GET | `/healthz` | 健康检查 |

接收器自身（集群内 9001）：`POST /alerts` 幂等接收、`GET /alerts` 已收列表、
`GET/POST /control/mode`、`POST /control/reset`、`GET /healthz`。

说明：`/api/meta` 的 4xx（例如制品不存在）视为上游**健康**响应，原样返回并清零计数；
仅连接失败、超时和 5xx 计入熔断失败。

示例制品：`example-app.json`、`shared-lib.json`、`reports-tool.json`
（位于 `artifacts/`；请求任意不存在的名字会得到 404，可用来观察“健康 4xx”行为）。

## 建议的手动验证

1. 将上游切到 **503**，连续查询同一制品 3 次：前两次 502，第三次之后变 503（熔断）。
   再查几次，观察「上游实际访问次数」不再增长。此时「接收器已收记录」出现一条
   `opened` 告警（制品名、故障代次、原因齐全）。
2. 把上游切回**正常**，等待 2 秒后查询：该请求作为“恢复探测”成功，熔断恢复，接收器
   追加一条同代次的 `recovered` 告警，且顺序在 `opened` 之后。
3. 切到**延迟**并设置为 `0.8`，用“并发请求数”发起多次查询：全部在 1 秒内成功，
   说明慢查询之间互不阻塞；把延迟设为 `3` 则每次查询约 1 秒超时并计为失败。
4. 熔断后在浏览器刷新页面，或 `docker compose restart gateway`：熔断状态仍为 open。
5. 在「告警接收器」区切到**拒收告警**，再制造一次熔断：事件留在「待发告警」中并持续
   重试（可见尝试次数与退避）；切回**接收告警**后事件送达，已收记录只有一条。
   拒收状态下 `docker compose restart gateway`，待发事件仍在，恢复接收后继续送达。
6. 熔断未恢复时点击「重置熔断状态」：接收器收到该制品同代次的 `reset` 告警。

## 自动化测试

```bash
pip install -r requirements-dev.txt
# 需要先启动 gateway、upstream 与 receiver（Compose 或本机进程）
GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/
```

* `tests/test_unit.py`：直接驱动状态机，覆盖失败计数/清零、熔断、单探测选举、
  探测失败重冷却、慢成功不能恢复、旧探测代次不能覆盖、无主探测释放、超时与错误分类。
* `tests/test_alerts_unit.py`：告警出盒与派发器的进程内测试：首次熔断/恢复/重置各生成
  一条事件、失败探测不生成新事件、并发不重复、代次单调、旧代次迟到不告警、拒收重试、
  确认丢失重投同一记录、恢复不越过待发熔断、租约单派发、重启后续投、接收器幂等去重。
* `tests/test_breaker.py`：通过 HTTP 覆盖真实双 worker 行为，包括两个 worker 都被用到、
  超时上限、并发探测只放行一个、慢查询并发，以及（设置 `GATEWAY_RESTART_CMD` 时）
  重启后状态保留。
* `tests/test_alerts.py`：通过 HTTP 覆盖告警链路：熔断/恢复/重置告警送达接收器、
  拒收后重投、顺序保证、并发去重，以及（设置 `GATEWAY_RESTART_CMD` 时）重启后续投。

## 可调环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `UPSTREAM_URL` | `http://upstream:9000` | 上游目录服务地址 |
| `RECEIVER_URL` | `http://receiver:9001` | 本地告警接收器地址；置空则只累积出盒不派发 |
| `GATEWAY_DB` | `./data/breaker.db` | 网关 SQLite 路径（熔断状态 + 告警出盒） |
| `RECEIVER_DB` | `./data/receiver.db` | 接收器 SQLite 路径（已收记录） |
| `REQUEST_TIMEOUT` | `1.0` | 单次上游调用总超时（秒） |
| `COOLDOWN` | `2.0` | 熔断冷却（秒） |
| `FAILURE_THRESHOLD` | `3` | 连续失败阈值 |
| `DISPATCH_INTERVAL` | `0.5` | 派发器扫描间隔（秒） |
| `ALERT_DELIVERY_TIMEOUT` | `2.0` | 单次告警投递超时（秒） |

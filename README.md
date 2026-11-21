# 双路 RTP 丢包补位中继（Dual-path RTP Relay）

直播链路在两路网络上同时传输同一节目（RTP 序号相同）。本服务从两个 UDP
端口收包，按 16 位可回绕序号去重、排序，把**原包**（不重封装、不改载荷）
依序转发给播出机；一路丢包时用另一路的同号包补位，两路都丢则在等待后跳过。

服务还支持**启动时开启的延播模式**（默认关闭）：合流后的每个原包先进入
延播队列，按入队顺序分配**单调递增、永不复用的待播编号**，在各自入队满
8 秒（可调）后才允许发往播出机。值班员可通过 API 查看待播编号与 RTP 序号
的对应关系，并整段撤下尚未发出的连续编号区间。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 两个 UDP 端口收同一节目，Python asyncio | `app/udp_transport.py` 用 `create_datagram_endpoint` 在同一事件循环监听 A/B 两个端口，回调统一交给 `RelayEngine` |
| 以首包为起点 | 第一个有效 RTP 包的序号锚定为待发序号（延播模式下改为入队），此前不缓存任何包 |
| 可回绕序号去重 | `app/rtp.py::seq_delta` 做模 2^16 前向距离比较；同序号首份保留，其余计入 `duplicates` |
| 原包依序转发 | 直接 `sendto` 收到的原始 datagram；引擎只在序号连续时出队，保证严格升序、无乱序 |
| 只接纳待发序号起 64 个位置内的包 | 前向距离 `d` 满足 `0 <= d < 64` 才入缓冲，`d >= 64` 计入 `out_of_window` 丢弃（窗口可用 `RELAY_WINDOW` 调） |
| 缺包等 80ms，从首个后续包到达起算，后续到包不延长 | 缓冲中出现队头缺口时，以**最早到包时间 + 80ms** 为截止时刻重新挂 `call_later` 定时器；新到包不改变截止时刻（`GAP_TIMEOUT_MS` 可调） |
| 两路静默，到期也跳过并继续 | 等待完全由事件循环定时器驱动，不依赖任何新输入；到期跳过缺号，之后到包继续按序转发 |
| FastAPI 查询最近 64 个跳过序号 | `GET /skipped` 返回有界 `deque(maxlen=64)`，新者在前；另有 `/status`、`/health` |
| 启动时开启的 8 秒延播模式 | `DELAY_ENABLED=true` 开启（`DELAY_SECONDS` 默认 8）；关闭时走原有立即转发路径，行为完全不变 |
| 合流后按入队顺序连续编号，回绕也不重用待播编号 | `app/delay_queue.py` 的待播编号是独立于 16 位 RTP 序号的进程内单调整数，从 1 开始只增不减；RTP 回绕一整圈后两个同 RTP 序号的包仍拿到不同待播编号 |
| 值班员查看待播编号及对应 RTP 序号 | `GET /pending` 返回按待播编号升序的 `queue_id` / `rtp_seq` / 剩余排期毫秒 |
| 提交连续编号区间撤播，整段原子校验 | `POST /retract {"start","end"}`（闭区间）；任一编号已发送 / 已撤下 / 尚未入队则整段拒绝（HTTP 409，带原因与首个问题编号），任何包都不改变状态 |
| 撤下的包不得发出，其余包按原排期在入队满 8 秒后依次发送 | 撤播只把条目标记为 `retracted` 并从待播队列摘除；定时器按每个包**自己的入队时刻 + 延时**触发，撤播不改变其他包的截止时刻 |
| 上游停流也照常排空 | 发送只由 `call_later` 定时器驱动，队列里已有的包在停流后仍会全部到期发出 |
| `/status` 的 forwarded 只计已交给 UDP 发送的包 | `RelayEngine._egress_send` 是唯一出口，`sendto` 调用即计入 `forwarded`（异常另计 `send_errors`）；延播队列内部计数与之同源 |
| 主动撤播不计入 `/skipped` | 撤播计入独立的 `stats.retracted`，拒绝次数计入 `retraction_rejections`；`/skipped` 与 `stats.skipped` 只反映上游缺包超时 |
| Docker Compose 启动 + 本地演示 | `docker-compose.yml`（仅服务）、`docker-compose.demo.yml`（补位/跳过演示）、`docker-compose.delay-demo.yml`（延播撤播演示）；无 Docker 时用 `demo/run_local_demo.sh` 与 `demo/run_local_delay_demo.sh` |

### 延播模式语义说明

- **两个编号空间**：RTP 序号是包头里 16 位、会回绕的字段；待播编号
  （queue id）是中继自己分配的单调整数，从 1 开始、只增不减、永不复用。
  值班员的撤播请求只针对待播编号，因此即使 RTP 序号回绕也不会产生歧义。
- **首包也延时**：第一个包同样入队等待完整延时时长，不做立即发送特例，
  保证撤播窗口对所有包一致。
- **整段原子拒绝**：`/retract` 先完整校验区间内每个编号，再一次性撤播。
  拒绝原因分三类：`already_sent`（已发送）、`already_retracted`（已撤下）、
  `not_enqueued`（尚未入队，含超过当前最大编号的请求）。
- **撤播不是跳过**：`/skipped` 只记录两路同丢、等待超时后中继放弃的上游
  缺号；值班员主动撤下的包都已完整收到，计入 `retracted`，两者互不污染。
- **缺口超时与延播正交**：合流阶段的 80ms 缺口等待发生在入队之前；缺号
  被跳过后，实际收到的包照常入队编号，待播编号因此只对真实存在的包连续。
- **停流排空**：队列发送只依赖事件循环定时器，上游彻底静默后队列仍会
  把所有待播包按各自排期发完（撤掉的除外）。

### 关键语义说明（补位/跳过）

- **补位与"恢复"计数**：若另一路的同号包在缺口成为*队头缺口*之前就已到达，
  引擎会在连续投递时直接用上，输出无缺号；只有在队头等待期间被补回的缺口
  才计入 `gaps_recovered`。两路同丢、等待超时才计入 `gaps_timed_out` 并进入
  `/skipped`。
- **跳过列表**：`deque(maxlen=64)` 保证最多保留最近 64 个跳过序号；
  `stats.skipped` 是不截断的累计计数。
- 非 RTP datagram（版本号≠2 或短于 12 字节）计入 `invalid` 并丢弃。

## 目录结构

```
app/
  config.py        环境变量配置与校验（含 DELAY_ENABLED / DELAY_SECONDS）
  rtp.py           RTP 头解析、序号回绕运算、演示用建包
  relay.py         核心引擎：去重/窗口/80ms 等待/跳过/统计，延播模式接入
  delay_queue.py   延播队列：单调待播编号、8s 定时发送、原子撤播
  udp_transport.py 双端口 ingress 与转发 socket
  api.py           FastAPI 路由（/pending、/retract 等）
  main.py          单事件循环启动中继 + uvicorn
demo/
  demo_source.py     双路流量发生器（可配丢包/延迟/静默）
  demo_sink.py       播出机侧接收器与缺口/撤播核对
  demo_operator.py   值班员撤播操作演示（只走公开 HTTP API）
  run_local_demo.sh       无 Docker 的本地补位/跳过演示
  run_local_delay_demo.sh 无 Docker 的本地延播撤播演示
tests/             21 个原有测试 + 20 个延播单元测试 + 6 个延播端到端测试
Dockerfile, docker-compose.yml, docker-compose.demo.yml,
docker-compose.delay-demo.yml
```

## 运行

### 方式一：Docker Compose（延播撤播演示，推荐）

```bash
docker compose -f docker-compose.delay-demo.yml up --build
```

四个容器共享网络命名空间：`source` 发两路无损流量 → `relay`（开启 8 秒
延播）合并并缓存 → `operator` 通过 API 查看 `/pending`、撤下 3 个仍待播的
连续包并演示拒绝路径 → `sink` 收流并核对缺口恰好等于撤播区间。中继持续
运行，可另行查询：

```bash
curl -s localhost:8000/pending
curl -s localhost:8000/status
curl -s -X POST localhost:8000/retract \
     -H 'Content-Type: application/json' -d '{"start": 10, "end": 12}'
```

缩短延时时长可临时覆盖，例如 `DELAY_SECONDS=2 docker compose -f
docker-compose.delay-demo.yml up --build`。

### 方式二：Docker Compose（补位/跳过演示，延播关闭）

```bash
docker compose -f docker-compose.demo.yml up --build
```

三个容器：`source` 发两路有损流量 → `relay` 补位/跳过 → `sink` 核对。
演示场景：A 路丢 10/20/30，B 路丢 15/30，B 路的 10/40 延迟 40ms，
seq 50 后双路静默 300ms。预期仅 **30** 进入跳过列表（两路同丢），其余全部补回。

### 方式三：Docker Compose（仅服务，对接真实播出机）

```bash
FORWARD_HOST=10.0.0.10 FORWARD_PORT=9200 docker compose up --build
# 需要延播时：
DELAY_ENABLED=true DELAY_SECONDS=8 FORWARD_HOST=10.0.0.10 \
  docker compose up --build
```

### 方式四：本地 Python（无 Docker）

```bash
pip install -r requirements.txt
python -m app.main                      # 启动中继（默认不延播）
DELAY_ENABLED=true python -m app.main   # 启动中继（8 秒延播）
./demo/run_local_demo.sh                # 另开终端，补位/跳过演示
./demo/run_local_delay_demo.sh          # 另开终端，延播撤播演示
```

端口冲突时可用环境变量覆盖：`API_PORT`、`UDP_PORT_A`、`UDP_PORT_B`、
`FORWARD_PORT`。本地延播演示也支持 `DELAY_SECONDS=1 ./demo/run_local_delay_demo.sh`
快速跑通。

## API

| 路径 | 说明 |
| --- | --- |
| `GET /health` | 存活检查 |
| `GET /skipped` | `{"skipped": [..]}` 最近 64 个**上游缺包**跳过序号，新者在前；撤播不在这里 |
| `GET /status` | 待发序号、缓冲长度、等待状态与截止剩余、全部计数、跳过列表、`delay` 延播快照与 `pending` 待播列表 |
| `GET /pending` | 延播队列中仍待播的包，按待播编号升序：`queue_id`、`rtp_seq`、已等待/剩余毫秒；未开启延播时返回空列表 |
| `POST /retract` |  body `{"start": int, "end": int}`，闭区间撤播；成功返回撤播数量与对应 RTP 序号；整段校验失败返回 409 与原因（`already_sent` / `already_retracted` / `not_enqueued`）；未开启延播返回 400；参数非法返回 400/422 |

`/status` 中 `stats` 关键字段：

- `forwarded`：已实际交给 UDP `sendto` 的包数（延播模式下只在真正发出后增长）；
- `retracted`：值班员成功撤下、从未发出的包数；
- `retraction_rejections`：被整段拒绝的撤播请求次数；
- `skipped` / `recent_skipped`：仅上游缺包超时，撤播不计入。

## 配置（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `UDP_PORT_A` / `UDP_PORT_B` | 9100 / 9101 | 两路 ingress UDP 端口 |
| `FORWARD_HOST` / `FORWARD_PORT` | 127.0.0.1 / 9200 | 播出机地址 |
| `API_HOST` / `API_PORT` | 0.0.0.0 / 8000 | FastAPI 监听地址 |
| `RELAY_WINDOW` | 64 | 接纳窗口（位置数） |
| `GAP_TIMEOUT_MS` | 80 | 缺包等待毫秒数 |
| `DELAY_ENABLED` | `false` | 是否在启动时开启延播模式（接受 `true/false/1/0/yes/no/on/off`） |
| `DELAY_SECONDS` | `8` | 延播队列保持时长（秒），合法范围 (0, 3600]，仅在开启延播时生效 |

延播开关只在启动时读取，运行中不可切换；需要切换请带新环境变量重启进程。

## 自测

```bash
python -m unittest discover -s tests -v
```

- 原有单元测试（21 个）：首包锚定、乱序重排、跨路去重首份优先、64 窗口边界、
  回绕距离、80ms 超时跳过、后续到包不延长等待、静默到期、临界补回、
  跳过列表 64 上限与新者在前、65535 回绕，以及 4 个真实 UDP/HTTP 端到端测试。
- 延播单元测试（`tests/test_delay_mode.py`，20 个）：待播编号从 1 单调分配、
  RTP 回绕不复用编号、到点前不发送、各自按入队时刻满延时发送、停流定时器排空、
  撤播成功后永不发送且不影响其余排期、三种整段拒绝（未入队/已发送/已撤下）、
  历史修剪后拒绝原因仍精确、非法区间、跨 65535 回绕撤播、拒绝计数、
  延播关闭时立即转发路径不变。
- 延播端到端测试（`tests/test_delay_end_to_end.py`，6 个）：真实 UDP socket +
  真实 uvicorn，覆盖 `/pending` 编号映射、撤播后播出机缺口与计数核对、
  409 原子拒绝与原因、已撤编号二次拒绝、停流后纯定时器排空、400/422 参数校验。

## 已知边界 / 未完成项

- 本沙箱环境未安装 Docker，容器化文件已编写并通过 YAML 静态校验，但**未在
  本机实际构建运行**；已用原生 Python 完成等价的端到端验证（含
  `demo/run_local_delay_demo.sh` 全流程与真实 UDP/HTTP 自动化测试）。
- 延播队列与合流缓冲均为进程内结构，无持久化：进程重启会丢失待播包与待播
  编号上下文，重启后以首包重新锚定、待播编号从 1 重新开始。
- 已发送/已撤播的条目只保留有界历史（4096 条）用于给出精确拒绝原因；更久
  以前的编号被撤播时会回退为 `not_enqueued` 拒绝，安全性不受影响。
- 未实现 SSRC 区分与 RTP 扩展头处理：默认两路为同一节目同一 SSRC；若两路
  SSRC 不同但序号同步，当前仍按序号合并（不校验 SSRC）。
- 未做 RTCP、拥塞控制与转发目的地址动态切换；`FORWARD_HOST` 为静态单播。
- 高码率下缓冲为进程内字典，无内存上限以外的持久化。
- 单进程单事件循环，未做多核/多实例水平扩展；撤播 API 无鉴权，默认只暴露
  在受控管理网络中，尚未接入认证/审计。

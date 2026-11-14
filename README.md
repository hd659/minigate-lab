# minigate-lab

手写的最小 LLM 网关实验场。纯 Python asyncio，零第三方运行依赖，不连任何真实上游。

用来回答几个在线上网关里只能靠翻日志猜的问题：

- 自用 key 和客户一起走全局 FIFO，与走 HMAC 签名的优先级通道相比，排队 p50/p99 差多少？
- 号池用"整池冷却"和"单号熔断"时，一个号出问题会不会连累整个池？
- 每一次失败到底是上游拒的，还是网关自己拒的？如果是网关拒的，起因又是什么？

## 快速开始

```bash
python3 -m minigate_lab explain                      # 4 个构造请求，逐条显示是谁拒的、为什么
python3 -m minigate_lab bench --scenario mixed       # 同一份到达序列跑 2x2 策略矩阵（约 16 秒）
python3 -m minigate_lab bench --scenario overload --strategies fifo+account,lanes+account
python3 -m minigate_lab bench --scenario dead-account --json
python3 -m minigate_lab token --secret 0123456789abcdef --key me --lane priority --scope me
python3 -m pytest -q
```

## 结构

| 文件 | 职责 |
| --- | --- |
| `minigate_lab/base.py` | 通道常量、错误码表（每个码固定归到 upstream 或 local）、`Result`、HTTP 状态码到错误码的映射 |
| `minigate_lab/admission.py` | `Signer`：`mg1.<key>.<lane>.<exp>.<HMAC-SHA256>` 令牌；`KeyLimiter`：每个 key 一个令牌桶 |
| `minigate_lab/pool.py` | 号池：`scope="pool"`（整池冷却）或 `scope="account"`（单号熔断 closed/open/half_open），号永久递增编号 |
| `minigate_lab/engine.py` | `Scheduler`（fifo 或加权双通道 + 保留槽）和 `Gateway`（鉴权、限流、排队、选号、失败转移、归因） |
| `minigate_lab/upstream.py` | 假上游：每个号单独配延迟、慢尾、随机 429/529/5xx、并发配额、定时触发"要求验证" |
| `minigate_lab/sim.py` | 带种子的开环泊松到达，4 种策略重放同一序列，统计 p50/p99 和冷却命中率 |
| `minigate_lab/cli.py` | `bench` / `explain` / `token` 三个子命令 |

## 设计要点

**签名优先级通道。** lane 字段在 HMAC 覆盖范围里，把 `standard` 改成 `priority` 签名就会失效，返回 `local_auth`。签发和校验都会检查 `priority_scope`：key 被移出作用域后，已经发出去的 priority 令牌会被降级到 standard（`downgraded=True`），不会直接锁死。全局限制没有放宽，只是给有作用域的身份单开了一条队。

**加权调度，不饿死客户。** 两个队都有人等的时候，每放行 `weight` 个优先请求就放行 1 个标准请求。另外有 `reserved` 个槽只给优先通道用，所以客户把槽占满时自用请求也不用排队。

**两种熔断范围。** `pool` 模式模拟常见的简单做法：任意一个号回 429/529，整池冻结 retry_after 秒。`account` 模式下 429 只让那一个号休息 retry_after 秒；5xx 和超时连续累计到阈值才熔断，之后指数退避，half_open 状态同一时间只放一个探测请求；上游要求验证时只停用那一个号。两种模式都会按已知配额给每个号设并发上限（`account_cap`），所以对比的只是熔断范围这一个变量。

**归因。** 每个失败的 `Result` 都带 `error`、`blame` 和 `root_cause`。网关自己拒绝、但起因在上游的情况（比如 429 引发的整池冷却），`blame` 记为 `local`，`root_cause` 记为 `upstream_429@acct#3`。这样"谁拒的"和"什么触发的"是两个独立字段：

```
forged priority claim            -> local refused (local_auth): token malformed, tampered, or signed with another secret
first call, upstream 429s        -> upstream refused (upstream_429): upstream account rate limited (429)
second call, pool frozen         -> local refused (local_pool_cooldown): ...; triggered by upstream_429@acct#1
third call, key over its bucket  -> local refused (local_rate_limit): this key exceeded its own token bucket
```

**编号永久递增。** 删掉 #2 之后再加号，新号是 #4，#3 还是 #3。

## 实测结果（seed=7，4 秒，12 个槽，5 个号）

`mixed` 场景：#3 有 25% 概率 429，#5 在 t=1s 被要求验证，另有一个客户狂刷。

| 策略 | 自用成功率 | 自用排队 p99 | 自用冷却命中 | 客户成功率 | 客户排队 p99 |
| --- | --- | --- | --- | --- | --- |
| fifo + 整池冷却 | 16.0% | 24ms | 84.0% | 14.8% | 20ms |
| lanes + 整池冷却 | 16.0% | 0ms | 84.0% | 10.6% | 1ms |
| fifo + 单号熔断 | 78.0% | 348ms | 0% | 69.0% | 362ms |
| lanes + 单号熔断 | 92.0% | 10ms | 0% | 64.8% | 493ms |

`overload` 场景（客户需求约为容量的 1.5 倍）：fifo + 单号熔断时自用成功 78.0%、排队 p99 628ms；lanes + 单号熔断时自用成功 100%、排队 p99 6ms，客户成功率从 67.3% 降到 60.6%。

`dead-account` 场景（#2 永远回 500）：整池冷却下成功率约 64%，单号熔断下 96% 到 98%。

换 seed=1 和 seed=42 重跑，结论方向一致。

读法：

1. 只要还是整池冷却，加优先通道基本没用。排队时间降到 0，但请求一进来就撞上冷却，失败原因几乎全是 `local_pool_cooldown`，根因是 `upstream_429`。所以先改熔断范围，再改调度。
2. 改成单号熔断后，成功率的提升来自隔离坏号；排队 p99 从几百毫秒降到个位数，来自优先通道。两者解决的是两个不同的问题。
3. 优先通道的代价由客户承担（overload 下客户成功率少了约 7 个百分点，排队 p99 变长）。`--weight` 和 `--reserved` 就是调这个取舍的旋钮。
4. 单号熔断下剩下的失败是 `local_no_account`，根因多半是 `upstream_429`：所有号同时都在休息或已满配额，这时候真正缺的是号的容量。

## 注意事项

- bench 跑在真实时钟上（asyncio.sleep），机器负载不同，数字会有几毫秒到几十毫秒的抖动。故障骰子是带种子的，但调用的先后顺序会因调度不同而变化。
- `lat p99` 只统计成功的请求。
- 假上游直接抛分好类的错误；接真实上游时，用 `error_from_status(status, retry_after, body)` 把 HTTP 状态码映射到同一套错误码。
- 演示用的 secret 硬编码在 `sim.py` 里，只能本地用。

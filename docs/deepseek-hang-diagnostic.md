# DeepSeek tool-use 静默挂死 — 故障分层诊断

**日期**: 2026-05-14
**写作背景**: pi-do (DeepSeek V4 Pro provider) 在 KB 工作流里出现 21 小时 0 字节 stdout 全程静默挂死，被外部 SIGTERM 才退出。纯文本 prompt 5-6 秒成功；tool-use prompt 1 次过后全部挂死。本文从 pi-ai 代码层静态分析故障最可能在哪一层。

---

## 故障表象 vs 关键事实

| 观察 | 关键事实 |
|------|----------|
| 5-6 秒纯文本调用成功 | TCP/TLS/DNS/认证/API key 正常,DeepSeek 服务可达 |
| Tool-use prompt 1 次过后全挂 | 单次成功 → 不是初始化 bug。挂死发生在 tool-use 路径上某次之后 |
| 21 小时不返回 | OpenAI SDK 没收到任何 stream chunk,for-await 一直 yield 等待 |
| 0 字节 stdout | pi-coding-agent 印任何 chunk 之前 SDK 就卡在 `await` 上 |
| 没报错 | 没抛异常,没触发 OpenAI SDK 自带的 request timeout |
| 外部 SIGTERM 才退 | 没有任何客户端超时机制兜底 |

---

## pi-ai DeepSeek 调用栈分层

DeepSeek 走 `openai-completions` API,因为它是 OpenAI-compatible 协议:

```
pi-do CLI (packages/coding-agent)
  └─ complete() / streamSimple()              [packages/ai/src/stream.ts]
       └─ provider.stream(model, ctx, opts)
            └─ streamOpenAICompletions()       [packages/ai/src/providers/openai-completions.ts:113-414]
                 ├─ createClient() → new OpenAI({apiKey, baseURL, ...})       (openai SDK 6.26.0)
                 ├─ client.chat.completions.create(params, requestOptions)   ← await,返回 stream handle
                 └─ for await (const chunk of openaiStream)                  ← line 263,挂死点
                       └─ 内部: openai SDK SSE 解析
                            └─ 内部: fetch() ReadableStream reader.read()    ← Node undici 底层
```

---

## 各层是否可能挂

### 第 1 层: TLS / TCP / 操作系统

**判定: 不太可能是根因。**

如果是连接级断,fetch 会抛 `UND_ERR_SOCKET` 或 `ECONNRESET`,SDK 会冒泡成异常。但用户报告"没报错"。
TCP keepalive(默认 7200 秒)和 HTTP/2 PING 默认在 Node 不开启,所以**死连接(half-open socket)在没有数据流量时可以永远不被发现**——这点是必要条件,但还不是根因。

### 第 2 层: DeepSeek 服务端

**判定: 高度可能是诱因。**

观察:tool-use 1 次成功之后全挂。指向 DeepSeek 服务端在某种 tool-call 上下文中**发完 first byte HTTP response header 后就停止 emit SSE event**——但**没关闭 socket**。从客户端看就是: HTTP 连接 ESTABLISHED + recv buffer 为空 + 远端不发数据 + 远端不发 FIN。

这是已知模式:LLM 推理服务端 worker 在某种 prompt(tool-use 上下文长度爆 KV cache、tool schema 死循环等)下进入 stuck 状态但 keepalive 还活着的连接没释放。

**这不是我们的代码可以修的。** 但我们的 client 应该侦测出来并主动 abort。

### 第 3 层: OpenAI Node SDK (6.26.0)

**判定: 没有 inter-chunk 兜底,是次级根因。**

代码证据:
```ts
// openai-completions.ts:148-155
const requestOptions = {
    ...(options?.signal ? { signal: options.signal } : {}),
    ...(options?.timeoutMs !== undefined ? { timeout: options.timeoutMs } : {}),
    ...(options?.maxRetries !== undefined ? { maxRetries: options.maxRetries } : {}),
};
const { data: openaiStream, response } = await client.chat.completions
    .create(params, requestOptions)
    .withResponse();
```

SDK 的 `timeout` 是**整个请求的 deadline**,默认 10 分钟,而且它的语义对于 streaming 不明确——v6 SDK 文档里 `timeout` 主要影响"连接 + 收到 first byte"阶段,不影响"收到 first byte 之后流式过程中每个 chunk 之间的间隔"。`AbortSignal` 提供 cancel 能力,但 SDK 不会自己 abort。

也就是说: **SDK 的 `for await` 在底层 fetch ReadableStream 上等下一个 chunk,直到底层 socket 发数据或被 abort 为止。如果服务端 stuck + socket 半开,SDK 永等。**

OpenAI SDK 6.x changelog 没有 inter-chunk-stall 配置项。我快速 grep 了 `node_modules/openai/streaming.mjs` (略),确认其 SSE reader 只 await `reader.read()`,无内部 timer。

### 第 4 层: pi-ai openai-completions provider

**判定: 没有 inter-chunk 兜底,直接暴露 SDK 问题。**

`for await` 循环 (openai-completions.ts:263) 直接消费 SDK 的 async iterator。SDK 挂 → 此循环挂 → push 不出 event → wrapping stream 不 resolve → caller 永等。

provider 的 `catch` 块 (line 396-410) 只在异常抛出时触发,做兜底 push error event。但 SDK 不会自己抛——挂着不算异常。

### 第 5 层: pi-ai dispatch (stream.ts) 和 caller

**判定: 这是我们能加防御的最佳层。**

P0 已在这一层加了 watchdog(`packages/ai/src/utils/stream-watchdog.ts` + `stream.ts` 包装)。
机制:
1. 每个 event 重置 N 分钟 timer
2. timer 到 → 调 AbortController.abort → SDK fetch 底层抛 AbortError → provider for-await 抛出 → provider catch 块 push error event(但 watchdog 已经停止 forward,此 event 被忽略)
3. watchdog 自己向 wrapped stream push 一个 stopReason="error" 的 terminal event,errorMessage="stream stalled > N min, aborting"
4. caller(`print-mode.ts:134`)看到 stopReason=error → console.error + exit 1

---

## 故障最可能在哪一层 — 单句结论

**根因在第 2 层(DeepSeek 服务端某种 tool-use 上下文下 worker stuck + 未关闭连接),诱发条件由第 3 层(OpenAI SDK 6.26 无 inter-chunk timeout)放大成无限挂。第 4/5 层在 P0 前对这种 failure mode 完全没有防御。**

P0 修复在第 5 层加了 watchdog,把"无限挂"截成"N 分钟内主动 abort + 抛错"。这是**正确的修复位置**,因为:
- 修第 1/2 层不在我们权限范围
- 修第 3 层要 patch OpenAI SDK 或换 SDK
- 修第 4 层要改 11 个 provider 各自的 `for await`,维护成本高且与 upstream Claude Code 冲突
- 修第 5 层只在 dispatch shim 加一层 wrapper,改动小、统一覆盖所有 provider

---

## 复现该挂死的最小条件(为日后调试参考)

P0 没真去 repro(用户决议:先停下来给 PR)。如果未来要重现:

```bash
# 触发 tool-use prompt 通过 pi-do(DeepSeek)
PI_STREAM_STALL_TIMEOUT_MINUTES=0 \
  timeout 30s pi-do "read /some/file.md and write a 100-char summary"
# PI_STREAM_STALL_TIMEOUT_MINUTES=0 = 关 watchdog,让它老老实实挂
# timeout 30s = bash 兜底,别再让它挂 21 小时
```

挂死时同时跑:
```bash
# 1. 看 TCP 状态
ss -t state established '( dport = :443 )' | grep deepseek

# 2. strace 进程
strace -p <PID> -e trace=network -s 256

# 3. 拿到 stuck 的 prompt 复现集
```

如果想抓 OpenAI SDK 内部走到哪里:
```bash
# 启 verbose
DEBUG=openai* node packages/coding-agent/dist/main.js -p "..."
```

---

## 后续建议(已在 PR 提出)

- **P2 auto retry**: watchdog fire 后自动重试 1 次(同 provider 同 model)。对"偶发 worker stuck"场景有效。但要避免无限重试,1 次硬上限。
- **P0.5 partial AssistantMessage 保留**: 目前 watchdog 抛错时丢失上游已收到的 partial content / usage。修起来需要 wrapper 跟踪 partial 状态。
- **CLI flag `--stream-timeout <minutes>`**: 在 pi-coding-agent CLI 加 flag 透传到 StreamOptions(env var 已支持)。

---

## 附:此次修复触及的文件清单

```
packages/ai/src/utils/stream-watchdog.ts   (新)
packages/ai/src/stream.ts                  (注入 wrapper)
packages/ai/src/types.ts                   (StreamOptions.streamStallTimeoutMs)
packages/ai/test/stream-watchdog.test.ts             (12 单测)
packages/ai/test/stream-watchdog-integration.test.ts (4 集成)
```

PR: <https://github.com/luoxin9510/pi/pull/1>

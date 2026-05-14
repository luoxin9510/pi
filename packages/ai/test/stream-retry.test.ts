import { afterEach, describe, expect, it } from "vitest";
import { registerApiProvider, unregisterApiProviders } from "../src/api-registry.js";
import { complete } from "../src/stream.js";
import type { AssistantMessage, AssistantMessageEvent, Context, Model } from "../src/types.js";
import { createAssistantMessageEventStream } from "../src/utils/event-stream.js";
import { resolveStreamStallRetries, wrapStreamWithRetry } from "../src/utils/stream-retry.js";
import { STREAM_STALL_ERROR_PREFIX } from "../src/utils/stream-watchdog.js";

const RETRY_API = "test-retry";
const RETRY_SOURCE = "stream-retry-test";

function model(): Model<typeof RETRY_API> {
	return {
		id: "retry-1",
		name: "Retry Model",
		api: RETRY_API,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 1000,
		maxTokens: 100,
	};
}

function ctx(): Context {
	return { messages: [{ role: "user", content: "hi", timestamp: 0 }] };
}

function emptyMsg(stopReason: AssistantMessage["stopReason"], errorMessage?: string): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: RETRY_API,
		provider: "test",
		model: "retry-1",
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason,
		errorMessage,
		timestamp: 0,
	};
}

describe("resolveStreamStallRetries", () => {
	const ENV = "PI_STREAM_STALL_RETRIES";
	it("explicit option takes precedence", () => {
		expect(resolveStreamStallRetries(3)).toBe(3);
	});
	it("explicit 0 disables", () => {
		expect(resolveStreamStallRetries(0)).toBe(0);
	});
	it("negative clamps to 0", () => {
		expect(resolveStreamStallRetries(-2)).toBe(0);
	});
	it("falls through to env", () => {
		const prev = process.env[ENV];
		try {
			process.env[ENV] = "4";
			expect(resolveStreamStallRetries(undefined)).toBe(4);
		} finally {
			if (prev === undefined) delete process.env[ENV];
			else process.env[ENV] = prev;
		}
	});
	it("default is 1", () => {
		const prev = process.env[ENV];
		try {
			delete process.env[ENV];
			expect(resolveStreamStallRetries(undefined)).toBe(1);
		} finally {
			if (prev !== undefined) process.env[ENV] = prev;
		}
	});
});

describe("wrapStreamWithRetry — direct", () => {
	it("forwards a successful stream untouched", async () => {
		let calls = 0;
		const runOnce = () => {
			calls++;
			const s = createAssistantMessageEventStream();
			queueMicrotask(() => {
				s.push({ type: "start", partial: emptyMsg("stop") });
				s.push({ type: "text_delta", contentIndex: 0, delta: "hi", partial: emptyMsg("stop") });
				s.push({ type: "done", reason: "stop", message: emptyMsg("stop") });
				s.end();
			});
			return s;
		};

		const wrapped = wrapStreamWithRetry(runOnce, 2);
		const result = await wrapped.result();
		expect(calls).toBe(1);
		expect(result.stopReason).toBe("stop");
	});

	it("retries once on a stall with no content emitted", async () => {
		let calls = 0;
		const runOnce = () => {
			calls++;
			const s = createAssistantMessageEventStream();
			queueMicrotask(() => {
				s.push({ type: "start", partial: emptyMsg("error") });
				if (calls === 1) {
					// fake stall error from watchdog
					s.push({
						type: "error",
						reason: "error",
						error: emptyMsg("error", `${STREAM_STALL_ERROR_PREFIX} > 0.05 min, aborting`),
					});
				} else {
					s.push({ type: "text_delta", contentIndex: 0, delta: "ok", partial: emptyMsg("stop") });
					s.push({ type: "done", reason: "stop", message: emptyMsg("stop") });
				}
				s.end();
			});
			return s;
		};

		const wrapped = wrapStreamWithRetry(runOnce, 1);
		const result = await wrapped.result();
		expect(calls).toBe(2);
		expect(result.stopReason).toBe("stop");
	});

	it("does NOT retry when stall happens after content emitted (mid-stream)", async () => {
		let calls = 0;
		const runOnce = () => {
			calls++;
			const s = createAssistantMessageEventStream();
			queueMicrotask(() => {
				s.push({ type: "start", partial: emptyMsg("error") });
				// emit content first
				s.push({ type: "text_delta", contentIndex: 0, delta: "partial", partial: emptyMsg("error") });
				// then stall
				s.push({
					type: "error",
					reason: "error",
					error: emptyMsg("error", `${STREAM_STALL_ERROR_PREFIX} > 0.05 min, aborting`),
				});
				s.end();
			});
			return s;
		};

		const wrapped = wrapStreamWithRetry(runOnce, 5);
		const result = await wrapped.result();
		expect(calls).toBe(1);
		expect(result.stopReason).toBe("error");
		expect(result.errorMessage).toContain(STREAM_STALL_ERROR_PREFIX);
	});

	it("does NOT retry on non-stall errors", async () => {
		let calls = 0;
		const runOnce = () => {
			calls++;
			const s = createAssistantMessageEventStream();
			queueMicrotask(() => {
				s.push({
					type: "error",
					reason: "error",
					error: emptyMsg("error", "rate limited"),
				});
				s.end();
			});
			return s;
		};

		const wrapped = wrapStreamWithRetry(runOnce, 3);
		const result = await wrapped.result();
		expect(calls).toBe(1);
		expect(result.errorMessage).toBe("rate limited");
	});

	it("gives up after maxRetries consecutive stalls", async () => {
		let calls = 0;
		const runOnce = () => {
			calls++;
			const s = createAssistantMessageEventStream();
			queueMicrotask(() => {
				s.push({
					type: "error",
					reason: "error",
					error: emptyMsg("error", `${STREAM_STALL_ERROR_PREFIX} > 0.05 min, aborting`),
				});
				s.end();
			});
			return s;
		};

		const wrapped = wrapStreamWithRetry(runOnce, 2);
		const result = await wrapped.result();
		expect(calls).toBe(3); // initial + 2 retries
		expect(result.errorMessage).toContain(STREAM_STALL_ERROR_PREFIX);
	});

	it("only buffers one start event (constant memory)", async () => {
		// Indirect check: ensure that when content arrives, no events are dropped
		// AND no synthetic duplicates appear.
		const events: AssistantMessageEvent[] = [];
		const runOnce = () => {
			const s = createAssistantMessageEventStream();
			queueMicrotask(() => {
				s.push({ type: "start", partial: emptyMsg("stop") });
				for (let i = 0; i < 5; i++) {
					s.push({ type: "text_delta", contentIndex: 0, delta: `${i}`, partial: emptyMsg("stop") });
				}
				s.push({ type: "done", reason: "stop", message: emptyMsg("stop") });
				s.end();
			});
			return s;
		};

		const wrapped = wrapStreamWithRetry(runOnce, 2);
		for await (const e of wrapped) events.push(e);

		const types = events.map((e) => e.type);
		expect(types).toEqual(["start", "text_delta", "text_delta", "text_delta", "text_delta", "text_delta", "done"]);
	});
});

describe("stream() integration — retry on stall", () => {
	afterEach(() => unregisterApiProviders(RETRY_SOURCE));

	it("end-to-end: initial-hang stall recovered by retry", async () => {
		let attempt = 0;
		registerApiProvider(
			{
				api: RETRY_API,
				stream: (m, _c, options) => {
					attempt++;
					const s = createAssistantMessageEventStream();
					if (attempt === 1) {
						// hang forever; signal handler will eventually fire
						options?.signal?.addEventListener("abort", () => s.end());
					} else {
						// success on retry
						queueMicrotask(() => {
							s.push({ type: "start", partial: emptyMsg("stop") });
							s.push({ type: "text_delta", contentIndex: 0, delta: "got it", partial: emptyMsg("stop") });
							s.push({
								type: "done",
								reason: "stop",
								message: { ...emptyMsg("stop"), api: m.api, provider: m.provider, model: m.id },
							});
							s.end();
						});
					}
					return s;
				},
				streamSimple: () => createAssistantMessageEventStream(),
			},
			RETRY_SOURCE,
		);

		const result = await complete(model(), ctx(), { streamStallTimeoutMs: 50, streamStallRetries: 1 });
		expect(attempt).toBe(2);
		expect(result.stopReason).toBe("stop");
	});

	it("user-supplied AbortSignal aborts during retry attempt 2", async () => {
		let attempt = 0;
		const attemptControllers: AbortSignal[] = [];
		registerApiProvider(
			{
				api: RETRY_API,
				stream: (m, _c, options) => {
					attempt++;
					const s = createAssistantMessageEventStream();
					if (options?.signal) attemptControllers.push(options.signal);
					if (attempt === 1) {
						// hang -> watchdog fires -> retry
						options?.signal?.addEventListener("abort", () => s.end());
					} else {
						// On retry attempt 2, listen for the user abort signal and emit
						// an "aborted" terminal event when fired.
						options?.signal?.addEventListener("abort", () => {
							s.push({
								type: "error",
								reason: "aborted",
								error: { ...emptyMsg("aborted", "user cancel"), api: m.api, provider: m.provider, model: m.id },
							});
							s.end();
						});
					}
					return s;
				},
				streamSimple: () => createAssistantMessageEventStream(),
			},
			RETRY_SOURCE,
		);

		const userCtl = new AbortController();
		// Watchdog window of 80ms per attempt: attempt 1 stalls at ~80ms, retry
		// fires, attempt 2 starts. We abort at ~120ms (40ms into attempt 2),
		// well before attempt 2's own watchdog window expires.
		const promise = complete(model(), ctx(), {
			streamStallTimeoutMs: 80,
			streamStallRetries: 1,
			signal: userCtl.signal,
		});
		await new Promise((r) => setTimeout(r, 120));
		userCtl.abort(new Error("user cancel"));
		const result = await promise;
		expect(attempt).toBe(2);
		expect(result.stopReason).toBe("aborted");
		expect(result.errorMessage).toBe("user cancel");
		// The second attempt's signal should reflect the user abort (separate
		// AbortController per attempt, but both linked to the user signal).
		expect(attemptControllers[1].aborted).toBe(true);
	});

	it("end-to-end: streamStallRetries=0 disables retry, surfaces stall", async () => {
		let attempt = 0;
		registerApiProvider(
			{
				api: RETRY_API,
				stream: (_m, _c, options) => {
					attempt++;
					const s = createAssistantMessageEventStream();
					options?.signal?.addEventListener("abort", () => s.end());
					return s;
				},
				streamSimple: () => createAssistantMessageEventStream(),
			},
			RETRY_SOURCE,
		);
		const result = await complete(model(), ctx(), { streamStallTimeoutMs: 50, streamStallRetries: 0 });
		expect(attempt).toBe(1);
		expect(result.stopReason).toBe("error");
		expect(result.errorMessage).toContain(STREAM_STALL_ERROR_PREFIX);
	});
});

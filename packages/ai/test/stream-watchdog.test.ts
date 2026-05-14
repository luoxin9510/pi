import { describe, expect, it } from "vitest";
import type { AssistantMessage, AssistantMessageEvent } from "../src/types.js";
import { AssistantMessageEventStream } from "../src/utils/event-stream.js";
import { resolveStreamStallTimeoutMs, wrapStreamWithWatchdog } from "../src/utils/stream-watchdog.js";

function emptyAssistant(): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: "openai-completions",
		provider: "deepseek",
		model: "deepseek-test",
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: 0,
	};
}

function startEvent(): AssistantMessageEvent {
	return { type: "start", partial: emptyAssistant() };
}

function doneEvent(): AssistantMessageEvent {
	return { type: "done", reason: "stop", message: { ...emptyAssistant(), stopReason: "stop" } };
}

describe("resolveStreamStallTimeoutMs", () => {
	const ENV = "PI_STREAM_STALL_TIMEOUT_MINUTES";
	it("uses explicit option when provided", () => {
		expect(resolveStreamStallTimeoutMs(1234)).toBe(1234);
	});

	it("returns 0 for explicit 0 (disabled)", () => {
		expect(resolveStreamStallTimeoutMs(0)).toBe(0);
	});

	it("clamps negative to 0", () => {
		expect(resolveStreamStallTimeoutMs(-5)).toBe(0);
	});

	it("falls back to env var (minutes)", () => {
		const prev = process.env[ENV];
		try {
			process.env[ENV] = "2";
			expect(resolveStreamStallTimeoutMs(undefined)).toBe(2 * 60_000);
		} finally {
			if (prev === undefined) delete process.env[ENV];
			else process.env[ENV] = prev;
		}
	});

	it("env var '0' disables the watchdog", () => {
		const prev = process.env[ENV];
		try {
			process.env[ENV] = "0";
			expect(resolveStreamStallTimeoutMs(undefined)).toBe(0);
		} finally {
			if (prev === undefined) delete process.env[ENV];
			else process.env[ENV] = prev;
		}
	});

	it("defaults to 5 minutes when nothing set", () => {
		const prev = process.env[ENV];
		try {
			delete process.env[ENV];
			expect(resolveStreamStallTimeoutMs(undefined)).toBe(5 * 60_000);
		} finally {
			if (prev !== undefined) process.env[ENV] = prev;
		}
	});

	it("ignores non-numeric env values and falls back to default", () => {
		const prev = process.env[ENV];
		try {
			process.env[ENV] = "not-a-number";
			expect(resolveStreamStallTimeoutMs(undefined)).toBe(5 * 60_000);
		} finally {
			if (prev === undefined) delete process.env[ENV];
			else process.env[ENV] = prev;
		}
	});
});

describe("wrapStreamWithWatchdog", () => {
	const baseCtx = { provider: "deepseek", api: "openai-completions", model: "deepseek-test" } as const;

	it("returns upstream unchanged when timeoutMs <= 0", () => {
		const upstream = new AssistantMessageEventStream();
		const wrapped = wrapStreamWithWatchdog(upstream, { ...baseCtx, timeoutMs: 0, abort: () => {} });
		expect(wrapped).toBe(upstream);
	});

	it("forwards events from upstream and resolves with the final message", async () => {
		const upstream = new AssistantMessageEventStream();
		const wrapped = wrapStreamWithWatchdog(upstream, { ...baseCtx, timeoutMs: 500, abort: () => {} });
		upstream.push(startEvent());
		upstream.push(doneEvent());
		upstream.end();

		const result = await wrapped.result();
		expect(result.stopReason).toBe("stop");
	});

	it("fires the watchdog when upstream stalls", async () => {
		const upstream = new AssistantMessageEventStream();
		let abortCalled = false;
		const wrapped = wrapStreamWithWatchdog(upstream, {
			...baseCtx,
			timeoutMs: 60,
			abort: () => {
				abortCalled = true;
			},
		});
		upstream.push(startEvent());
		// then go silent — never emit done/error, never call .end()
		const result = await wrapped.result();
		expect(abortCalled).toBe(true);
		expect(result.stopReason).toBe("error");
		expect(result.errorMessage).toContain("stream stalled");
		expect(result.errorMessage).toContain("aborting");
	});

	it("resets the watchdog timer on each event", async () => {
		const upstream = new AssistantMessageEventStream();
		let abortCalled = false;
		const wrapped = wrapStreamWithWatchdog(upstream, {
			...baseCtx,
			timeoutMs: 80,
			abort: () => {
				abortCalled = true;
			},
		});

		// Emit one event every 40ms for 4 ticks (total 160ms > timeoutMs);
		// because we reset on each chunk, the watchdog must NOT fire.
		upstream.push(startEvent());
		for (let i = 0; i < 3; i++) {
			await new Promise((r) => setTimeout(r, 40));
			upstream.push({ type: "text_delta", contentIndex: 0, delta: ".", partial: emptyAssistant() });
		}
		upstream.push(doneEvent());
		upstream.end();

		const result = await wrapped.result();
		expect(abortCalled).toBe(false);
		expect(result.stopReason).toBe("stop");
	});

	it("preserves partial content and usage from last seen partial when stalling", async () => {
		const upstream = new AssistantMessageEventStream();
		const wrapped = wrapStreamWithWatchdog(upstream, { ...baseCtx, timeoutMs: 60, abort: () => {} });

		const richPartial: AssistantMessage = {
			...emptyAssistant(),
			content: [
				{ type: "text", text: "hello so far" },
				{ type: "toolCall", id: "call-1", name: "calc", arguments: { a: 1 } },
			],
			usage: {
				input: 42,
				output: 7,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 49,
				cost: { input: 0.001, output: 0.0007, cacheRead: 0, cacheWrite: 0, total: 0.0017 },
			},
			responseId: "resp-abc",
		};
		upstream.push({ type: "start", partial: richPartial });
		upstream.push({ type: "text_delta", contentIndex: 0, delta: "hello so far", partial: richPartial });
		// then go silent

		const result = await wrapped.result();
		expect(result.stopReason).toBe("error");
		expect(result.errorMessage).toContain("stream stalled");
		// partial content + usage + responseId carried over
		expect(result.content.length).toBe(2);
		expect((result.content[0] as { type: string; text: string }).text).toBe("hello so far");
		expect(result.usage.input).toBe(42);
		expect(result.usage.output).toBe(7);
		expect(result.responseId).toBe("resp-abc");
		// scratch fields stripped on tool-call blocks (defensive)
		const toolBlock = result.content[1] as unknown as Record<string, unknown>;
		expect(toolBlock.partialArgs).toBeUndefined();
		expect(toolBlock.streamIndex).toBeUndefined();
		expect(toolBlock.index).toBeUndefined();
	});

	it("snapshot is a clone — mutating the result does not affect upstream partial", async () => {
		const upstream = new AssistantMessageEventStream();
		const wrapped = wrapStreamWithWatchdog(upstream, { ...baseCtx, timeoutMs: 50, abort: () => {} });
		const partial: AssistantMessage = {
			...emptyAssistant(),
			content: [{ type: "text", text: "abc" }],
		};
		upstream.push({ type: "start", partial });

		const result = await wrapped.result();
		(result.content[0] as { type: "text"; text: string }).text = "MUTATED";
		expect((partial.content[0] as { type: "text"; text: string }).text).toBe("abc");
	});

	it("propagates upstream error events without firing watchdog", async () => {
		const upstream = new AssistantMessageEventStream();
		let abortCalled = false;
		const wrapped = wrapStreamWithWatchdog(upstream, {
			...baseCtx,
			timeoutMs: 500,
			abort: () => {
				abortCalled = true;
			},
		});
		const errMsg: AssistantMessage = { ...emptyAssistant(), stopReason: "error", errorMessage: "upstream boom" };
		upstream.push(startEvent());
		upstream.push({ type: "error", reason: "error", error: errMsg });
		upstream.end();

		const result = await wrapped.result();
		expect(abortCalled).toBe(false);
		expect(result.stopReason).toBe("error");
		expect(result.errorMessage).toBe("upstream boom");
	});
});

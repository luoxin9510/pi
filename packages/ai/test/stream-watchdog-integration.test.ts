import { afterEach, describe, expect, it } from "vitest";
import { registerApiProvider, unregisterApiProviders } from "../src/api-registry.js";
import { complete, stream } from "../src/stream.js";
import type { Context, Model } from "../src/types.js";
import { createAssistantMessageEventStream } from "../src/utils/event-stream.js";

const HANG_API = "test-hang";
const HANG_SOURCE = "stream-watchdog-integration-test";

function hangModel(): Model<typeof HANG_API> {
	return {
		id: "hang-1",
		name: "Hang Model",
		api: HANG_API,
		provider: "test",
		baseUrl: "http://localhost:0",
		reasoning: false,
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
		contextWindow: 1000,
		maxTokens: 100,
	};
}

function emptyContext(): Context {
	return { messages: [{ role: "user", content: "hi", timestamp: 0 }] };
}

describe("stream() integration with watchdog", () => {
	afterEach(() => {
		unregisterApiProviders(HANG_SOURCE);
	});

	it("aborts hung provider via streamStallTimeoutMs option and yields error result", async () => {
		let abortSeen = false;
		registerApiProvider(
			{
				api: HANG_API,
				stream: (model, _context, options) => {
					const s = createAssistantMessageEventStream();
					s.push({
						type: "start",
						partial: {
							role: "assistant",
							content: [],
							api: model.api,
							provider: model.provider,
							model: model.id,
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
						},
					});
					options?.signal?.addEventListener("abort", () => {
						abortSeen = true;
						// Real providers terminate the stream on abort. Simulate that.
						s.end();
					});
					return s;
				},
				streamSimple: () => createAssistantMessageEventStream(),
			},
			HANG_SOURCE,
		);

		const result = await complete(hangModel(), emptyContext(), { streamStallTimeoutMs: 60 });

		expect(abortSeen).toBe(true);
		expect(result.stopReason).toBe("error");
		expect(result.errorMessage).toContain("stream stalled");
		expect(result.errorMessage).toContain("aborting");
	});

	it("respects PI_STREAM_STALL_TIMEOUT_MINUTES env var", async () => {
		const ENV = "PI_STREAM_STALL_TIMEOUT_MINUTES";
		const prev = process.env[ENV];
		try {
			// 0.001 min = 60ms
			process.env[ENV] = "0.001";
			registerApiProvider(
				{
					api: HANG_API,
					stream: (_model, _context, options) => {
						const s = createAssistantMessageEventStream();
						options?.signal?.addEventListener("abort", () => s.end());
						// never push anything — the start event isn't even emitted
						return s;
					},
					streamSimple: () => createAssistantMessageEventStream(),
				},
				HANG_SOURCE,
			);

			const result = await complete(hangModel(), emptyContext());
			expect(result.stopReason).toBe("error");
			expect(result.errorMessage).toContain("stream stalled");
		} finally {
			if (prev === undefined) delete process.env[ENV];
			else process.env[ENV] = prev;
		}
	});

	it("watchdog disabled (streamStallTimeoutMs=0) does not interfere", async () => {
		registerApiProvider(
			{
				api: HANG_API,
				stream: (model, _context, _options) => {
					const s = createAssistantMessageEventStream();
					queueMicrotask(() => {
						s.push({
							type: "done",
							reason: "stop",
							message: {
								role: "assistant",
								content: [],
								api: model.api,
								provider: model.provider,
								model: model.id,
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
							},
						});
						s.end();
					});
					return s;
				},
				streamSimple: () => createAssistantMessageEventStream(),
			},
			HANG_SOURCE,
		);

		const result = await complete(hangModel(), emptyContext(), { streamStallTimeoutMs: 0 });
		expect(result.stopReason).toBe("stop");
	});

	it("user-supplied AbortSignal still aborts when watchdog active", async () => {
		let providerSawAbort = false;
		registerApiProvider(
			{
				api: HANG_API,
				stream: (model, _context, options) => {
					const s = createAssistantMessageEventStream();
					options?.signal?.addEventListener("abort", () => {
						providerSawAbort = true;
						// Real providers must emit a terminal event before ending (StreamFunction contract).
						s.push({
							type: "error",
							reason: "aborted",
							error: {
								role: "assistant",
								content: [],
								api: model.api,
								provider: model.provider,
								model: model.id,
								usage: {
									input: 0,
									output: 0,
									cacheRead: 0,
									cacheWrite: 0,
									totalTokens: 0,
									cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
								},
								stopReason: "aborted",
								errorMessage: "user cancel",
								timestamp: 0,
							},
						});
						s.end();
					});
					return s;
				},
				streamSimple: () => createAssistantMessageEventStream(),
			},
			HANG_SOURCE,
		);

		const controller = new AbortController();
		const s = stream(hangModel(), emptyContext(), { signal: controller.signal, streamStallTimeoutMs: 60_000 });
		controller.abort(new Error("user cancel"));
		const result = await s.result();
		expect(providerSawAbort).toBe(true);
		expect(result.stopReason).toBe("aborted");
	});
});

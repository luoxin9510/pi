import type { Api, AssistantMessage, Provider } from "../types.js";
import { AssistantMessageEventStream } from "./event-stream.js";

const DEFAULT_STREAM_STALL_TIMEOUT_MINUTES = 5;
const ENV_STREAM_STALL_TIMEOUT_MINUTES = "PI_STREAM_STALL_TIMEOUT_MINUTES";

/**
 * Prefix used in the `errorMessage` of the synthetic AssistantMessage emitted
 * by the watchdog. Downstream retry/diagnostic code can match on this prefix
 * to detect that an error came from the stall watchdog (not from the provider).
 */
export const STREAM_STALL_ERROR_PREFIX = "stream stalled";

export function isStreamStallError(message: { errorMessage?: string }): boolean {
	return typeof message.errorMessage === "string" && message.errorMessage.startsWith(STREAM_STALL_ERROR_PREFIX);
}

/**
 * Resolve the effective stream-stall watchdog timeout in milliseconds.
 *
 * Precedence: explicit option > env var (`PI_STREAM_STALL_TIMEOUT_MINUTES`) > 5-minute default.
 * A value of 0 (or negative) disables the watchdog.
 */
export function resolveStreamStallTimeoutMs(explicitMs: number | undefined): number {
	if (typeof explicitMs === "number" && Number.isFinite(explicitMs)) {
		return Math.max(0, explicitMs);
	}
	const raw = typeof process !== "undefined" ? process.env?.[ENV_STREAM_STALL_TIMEOUT_MINUTES] : undefined;
	if (raw !== undefined && raw !== "") {
		const minutes = Number(raw);
		if (Number.isFinite(minutes)) {
			return Math.max(0, minutes * 60_000);
		}
	}
	return DEFAULT_STREAM_STALL_TIMEOUT_MINUTES * 60_000;
}

export interface WatchdogContext {
	timeoutMs: number;
	abort: (reason: unknown) => void;
	provider: Provider;
	api: Api;
	model: string;
}

/**
 * Clone an in-progress AssistantMessage so it can be used as a final synthetic
 * error payload without mutating shared state held by the provider, and strip
 * scratch fields (`partialArgs`, `streamIndex`, `index`) that some providers
 * attach to tool-call blocks during streaming.
 */
function snapshotPartial(partial: AssistantMessage): AssistantMessage {
	const clone = JSON.parse(JSON.stringify(partial)) as AssistantMessage;
	for (const block of clone.content) {
		delete (block as { index?: number }).index;
		delete (block as { partialArgs?: string }).partialArgs;
		delete (block as { streamIndex?: number }).streamIndex;
	}
	return clone;
}

/**
 * Wrap an upstream AssistantMessageEventStream with a stall watchdog.
 *
 * Each event resets a timer. If no event arrives within `timeoutMs`, the watchdog:
 *   1. Calls `abort()` so the upstream provider can tear down its connection.
 *   2. Emits a terminal `error` event with stopReason "error" and a descriptive errorMessage.
 *   3. Stops forwarding any further upstream events.
 *
 * If `timeoutMs <= 0`, the upstream stream is returned unchanged.
 */
export function wrapStreamWithWatchdog(
	upstream: AssistantMessageEventStream,
	ctx: WatchdogContext,
): AssistantMessageEventStream {
	if (ctx.timeoutMs <= 0) return upstream;

	const wrapped = new AssistantMessageEventStream();
	let timer: ReturnType<typeof setTimeout> | undefined;
	let stalled = false;
	let terminated = false;
	// Most recent partial AssistantMessage observed on a non-terminal event.
	// On stall we use this as the base so already-streamed content and usage
	// are preserved instead of dropped.
	let lastPartial: AssistantMessage | undefined;

	const buildErrorMessage = (errorMessage: string): AssistantMessage => {
		if (lastPartial) {
			const snap = snapshotPartial(lastPartial);
			snap.stopReason = "error";
			snap.errorMessage = errorMessage;
			snap.timestamp = Date.now();
			return snap;
		}
		return {
			role: "assistant",
			content: [],
			api: ctx.api,
			provider: ctx.provider,
			model: ctx.model,
			usage: {
				input: 0,
				output: 0,
				cacheRead: 0,
				cacheWrite: 0,
				totalTokens: 0,
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
			},
			stopReason: "error",
			errorMessage,
			timestamp: Date.now(),
		};
	};

	const fireStall = () => {
		if (terminated) return;
		stalled = true;
		terminated = true;
		if (timer) {
			clearTimeout(timer);
			timer = undefined;
		}
		try {
			ctx.abort(new Error(`pi-ai stream-stall watchdog: no chunks for ${ctx.timeoutMs}ms`));
		} catch {
			// swallow — provider may already be torn down
		}
		const errorMessage = `${STREAM_STALL_ERROR_PREFIX} > ${(ctx.timeoutMs / 60_000).toFixed(2)} min, aborting`;
		wrapped.push({ type: "error", reason: "error", error: buildErrorMessage(errorMessage) });
		// Force the upstream iterator to terminate so our forwarding coroutine
		// can exit even if the provider ignores the abort signal. EventStream.end()
		// is idempotent and subsequent push()es become no-ops, so this is safe
		// even when the provider does eventually emit something.
		try {
			upstream.end();
		} catch {
			// swallow
		}
	};

	const armTimer = () => {
		if (timer) clearTimeout(timer);
		timer = setTimeout(fireStall, ctx.timeoutMs);
	};

	armTimer();

	(async () => {
		try {
			for await (const event of upstream) {
				if (stalled) break;
				armTimer();
				// Track latest in-progress AssistantMessage. All event types except
				// `done`/`error` carry a `partial` field per the protocol.
				if ("partial" in event && event.partial) {
					lastPartial = event.partial;
				}
				wrapped.push(event);
				if (event.type === "done" || event.type === "error") {
					terminated = true;
					break;
				}
			}
		} catch (err) {
			if (!terminated) {
				terminated = true;
				const errorMessage = err instanceof Error ? err.message : String(err);
				wrapped.push({ type: "error", reason: "error", error: buildErrorMessage(errorMessage) });
			}
		} finally {
			if (timer) {
				clearTimeout(timer);
				timer = undefined;
			}
			wrapped.end();
		}
	})();

	return wrapped;
}

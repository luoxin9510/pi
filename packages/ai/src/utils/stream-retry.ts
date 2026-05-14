import type { AssistantMessageEvent } from "../types.js";
import { AssistantMessageEventStream } from "./event-stream.js";
import { isStreamStallError } from "./stream-watchdog.js";

const DEFAULT_STREAM_STALL_RETRIES = 1;
const ENV_STREAM_STALL_RETRIES = "PI_STREAM_STALL_RETRIES";

/**
 * Resolve the effective stream-stall retry count.
 *
 * Precedence: explicit option > env var (`PI_STREAM_STALL_RETRIES`) > 1 (default).
 * 0 disables retry. Negative values are clamped to 0.
 */
export function resolveStreamStallRetries(explicit: number | undefined): number {
	if (typeof explicit === "number" && Number.isFinite(explicit)) {
		return Math.max(0, Math.floor(explicit));
	}
	const raw = typeof process !== "undefined" ? process.env?.[ENV_STREAM_STALL_RETRIES] : undefined;
	if (raw !== undefined && raw !== "") {
		const parsed = Number(raw);
		if (Number.isFinite(parsed)) {
			return Math.max(0, Math.floor(parsed));
		}
	}
	return DEFAULT_STREAM_STALL_RETRIES;
}

/**
 * Wrap a stream-producing function with auto-retry on watchdog stalls.
 *
 * Safety contract: we only retry when the stall happened *before any content
 * event was emitted* (true initial hang — the LLM produced nothing). This
 * matches the observed DeepSeek failure mode (21h, 0 bytes stdout). If any
 * content event (text/thinking/tool-call delta or end) has already been
 * forwarded to the caller, we propagate the stall error instead of retrying,
 * because LLM calls are not idempotent and replaying mid-generation would
 * either duplicate or corrupt output.
 *
 * Implementation: we buffer the leading `start` event (if any) and flush it
 * the moment a content event arrives, at which point the retry door closes
 * for this attempt. Memory cost: at most one event buffered.
 */
export function wrapStreamWithRetry(
	runOnce: () => AssistantMessageEventStream,
	maxRetries: number,
): AssistantMessageEventStream {
	if (maxRetries <= 0) return runOnce();

	const outer = new AssistantMessageEventStream();

	(async () => {
		for (let attempt = 0; attempt <= maxRetries; attempt++) {
			const inner = runOnce();
			let flushed = false;
			const buffered: AssistantMessageEvent[] = [];
			let terminal: AssistantMessageEvent | undefined;

			for await (const event of inner) {
				if (event.type === "done" || event.type === "error") {
					terminal = event;
					break;
				}
				if (flushed) {
					outer.push(event);
				} else if (event.type === "start") {
					buffered.push(event);
				} else {
					// First non-`start` event — flush leading start (if any) and lock
					// out retry. Note: this is intentionally conservative — even
					// zero-content marker events (`text_start`, `thinking_start`,
					// `toolcall_start`) close the retry door, because any signal that
					// the LLM has begun producing output means a replay could
					// duplicate or interleave content. Trading one possible retry
					// opportunity for stronger non-duplication guarantees.
					for (const e of buffered) outer.push(e);
					buffered.length = 0;
					outer.push(event);
					flushed = true;
				}
			}

			const stallEligibleForRetry =
				!flushed &&
				terminal?.type === "error" &&
				terminal.reason === "error" &&
				isStreamStallError(terminal.error) &&
				attempt < maxRetries;

			if (stallEligibleForRetry) {
				// Discard buffered `start` and try again with a fresh stream.
				continue;
			}

			// Flush anything still buffered (e.g., start event when stream ended
			// with no content), then the terminal event.
			for (const e of buffered) outer.push(e);
			if (terminal) outer.push(terminal);
			break;
		}
		outer.end();
	})();

	return outer;
}

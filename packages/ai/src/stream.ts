import "./providers/register-builtins.js";

import { getApiProvider } from "./api-registry.js";
import type {
	Api,
	AssistantMessage,
	AssistantMessageEventStream,
	Context,
	Model,
	ProviderStreamOptions,
	SimpleStreamOptions,
	StreamOptions,
} from "./types.js";
import { resolveStreamStallRetries, wrapStreamWithRetry } from "./utils/stream-retry.js";
import { resolveStreamStallTimeoutMs, wrapStreamWithWatchdog } from "./utils/stream-watchdog.js";

export { getEnvApiKey } from "./env-api-keys.js";

function resolveApiProvider(api: Api) {
	const provider = getApiProvider(api);
	if (!provider) {
		throw new Error(`No API provider registered for api: ${api}`);
	}
	return provider;
}

function applyResilience<TApi extends Api>(
	model: Model<TApi>,
	options: StreamOptions | undefined,
	run: (opts: StreamOptions | undefined) => AssistantMessageEventStream,
): AssistantMessageEventStream {
	const timeoutMs = resolveStreamStallTimeoutMs(options?.streamStallTimeoutMs);
	if (timeoutMs <= 0) {
		// Watchdog disabled → no retry (we have no signal to retry on)
		return run(options);
	}

	const userSignal = options?.signal;
	const runWithWatchdog = (): AssistantMessageEventStream => {
		// Each attempt gets its own AbortController so the watchdog of attempt N
		// doesn't cancel the request of attempt N+1.
		const attemptController = new AbortController();
		let userAbortListener: (() => void) | undefined;
		const detach = () => {
			if (userSignal && userAbortListener) {
				userSignal.removeEventListener("abort", userAbortListener);
				userAbortListener = undefined;
			}
		};
		if (userSignal) {
			if (userSignal.aborted) {
				attemptController.abort(userSignal.reason);
			} else {
				userAbortListener = () => attemptController.abort(userSignal.reason);
				userSignal.addEventListener("abort", userAbortListener, { once: true });
			}
		}

		const upstream = run({ ...options, signal: attemptController.signal } as StreamOptions);
		const wrapped = wrapStreamWithWatchdog(upstream, {
			timeoutMs,
			abort: (reason) => {
				if (!attemptController.signal.aborted) attemptController.abort(reason);
			},
			provider: model.provider,
			api: model.api,
			model: model.id,
		});
		// Detach the abort listener on any terminal outcome (success, error,
		// stall, user-abort) so long-lived user signals don't accumulate
		// listeners across many calls.
		wrapped.result().finally(detach);
		return wrapped;
	};

	const maxRetries = resolveStreamStallRetries(options?.streamStallRetries);
	if (maxRetries <= 0) {
		return runWithWatchdog();
	}
	return wrapStreamWithRetry(runWithWatchdog, maxRetries);
}

export function stream<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: ProviderStreamOptions,
): AssistantMessageEventStream {
	const provider = resolveApiProvider(model.api);
	return applyResilience(model, options as StreamOptions | undefined, (opts) =>
		provider.stream(model, context, opts as StreamOptions),
	);
}

export async function complete<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: ProviderStreamOptions,
): Promise<AssistantMessage> {
	const s = stream(model, context, options);
	return s.result();
}

export function streamSimple<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: SimpleStreamOptions,
): AssistantMessageEventStream {
	const provider = resolveApiProvider(model.api);
	return applyResilience(model, options as StreamOptions | undefined, (opts) =>
		provider.streamSimple(model, context, opts as SimpleStreamOptions),
	);
}

export async function completeSimple<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: SimpleStreamOptions,
): Promise<AssistantMessage> {
	const s = streamSimple(model, context, options);
	return s.result();
}

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
import { resolveStreamStallTimeoutMs, wrapStreamWithWatchdog } from "./utils/stream-watchdog.js";

export { getEnvApiKey } from "./env-api-keys.js";

function resolveApiProvider(api: Api) {
	const provider = getApiProvider(api);
	if (!provider) {
		throw new Error(`No API provider registered for api: ${api}`);
	}
	return provider;
}

function applyWatchdog<TApi extends Api>(
	model: Model<TApi>,
	options: StreamOptions | undefined,
	run: (opts: StreamOptions | undefined) => AssistantMessageEventStream,
): AssistantMessageEventStream {
	const timeoutMs = resolveStreamStallTimeoutMs(options?.streamStallTimeoutMs);
	if (timeoutMs <= 0) {
		return run(options);
	}

	const ourController = new AbortController();
	const userSignal = options?.signal;
	if (userSignal) {
		if (userSignal.aborted) {
			ourController.abort(userSignal.reason);
		} else {
			const forward = () => ourController.abort(userSignal.reason);
			userSignal.addEventListener("abort", forward, { once: true });
		}
	}

	const upstream = run({ ...options, signal: ourController.signal } as StreamOptions);
	return wrapStreamWithWatchdog(upstream, {
		timeoutMs,
		abort: (reason) => {
			if (!ourController.signal.aborted) ourController.abort(reason);
		},
		provider: model.provider,
		api: model.api,
		model: model.id,
	});
}

export function stream<TApi extends Api>(
	model: Model<TApi>,
	context: Context,
	options?: ProviderStreamOptions,
): AssistantMessageEventStream {
	const provider = resolveApiProvider(model.api);
	return applyWatchdog(model, options as StreamOptions | undefined, (opts) =>
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
	return applyWatchdog(model, options as StreamOptions | undefined, (opts) =>
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

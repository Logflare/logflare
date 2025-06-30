import { instrument, ResolveConfigFn } from '@microlabs/otel-cf-workers';

let batch: { event_message: string }[] = [];
const BATCH_INTERVAL_MS = 1000;
const MAX_BATCH_SIZE = 100;
let batchTimeoutReached = false;
const config: ResolveConfigFn = (env: Env) => {
	return {
		exporter: {
			url: 'http://localhost:4000/v1/traces',
			headers: { 'x-source': env.SOURCE, 'x-api-key': env.API_KEY },
		},
		service: { name: 'greetings' },
	};
};

const handler = {
	async fetch(request, env: Env, ctx): Promise<Response> {
		await fetch(`http://localhost:4000/api/events?source=${env.SOURCE}`, {
			method: 'POST',
			headers: {
				'x-api-key': env.API_KEY,
				'content-type': 'application/json',
			},
			body: JSON.stringify({ event_message: 'Sync POST with Hello World!' }),
		});
		await maybeQueueBatch(env, ctx);
		ctx.waitUntil(scheduleBatch(env, ctx));
		return new Response('Hello World!');
	},
};

const maybeQueueBatch = async (env: Env, ctx: ExecutionContext) => {
	const randomIterations = Math.floor(Math.random() * 100) + 1;
	for (let i = 0; i < randomIterations; i++) {
		batch.push({ event_message: `Random iteration ${i + 1} with Hello World!` });
	}

	await sleep(200);
	ctx.waitUntil(maybeQueueBatch(env, ctx));
};

export async function scheduleBatch(env: Env, event: ExecutionContext): Promise<void> {
	maybeQueueBatch(env, event);
	if (batchTimeoutReached) {
		batchTimeoutReached = false;
		if (batch.length > MAX_BATCH_SIZE) {
			event.waitUntil(maybeSendBatch(env, event));
		}
		batchTimeoutReached = true;
	}
	await sleep(BATCH_INTERVAL_MS);
	event.waitUntil(scheduleBatch(env, event));
}

const maybeSendBatch = async (env: Env, ctx: ExecutionContext) => {
	if (batch.length > 0) {
		console.log('sending async batch');
		await fetch(`http://localhost:4000/api/events?source=${env.SOURCE}`, {
			method: 'POST',
			headers: {
				'x-api-key': env.API_KEY,
				'content-type': 'application/json',
			},
			body: JSON.stringify(batch),
		});
		batch = [];
	}
};

const sleep = (ms: number) => {
	return new Promise((resolve) => {
		setTimeout(resolve, ms, {});
	});
};

export default instrument(handler, config) satisfies ExportedHandler<Env>;

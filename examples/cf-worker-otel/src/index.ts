/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.json`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */
import { trace } from '@opentelemetry/api'
import { instrument, ResolveConfigFn } from '@microlabs/otel-cf-workers'

const config: ResolveConfigFn = (env: Env) => {
	return {
		exporter: {
			url: 'https://otel.logflare.app',
			headers: { 'x-source': env.SOURCE, 'x-api-key': env.API_KEY },
			
		},
		service: { name: 'greetings' },
	}
}

const handler = {
	async fetch(request, env: Env, ctx): Promise<Response> {

		await fetch(`https://api.logflare.app/api/events?source=${env.SOURCE}`, {
			method: "POST",
			headers: {
				'x-api-key': env.API_KEY,
				'content-type': "application/json"
			},
			body: JSON.stringify({'event_message': "Sync POST with Hello World!"})
		})


		return new Response('Hello World!');
	},
}  

export default instrument(handler, config) satisfies ExportedHandler<Env>;


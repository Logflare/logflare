---
title: Cloudflare Workers
---

Use the Cloudflare Worker example in the `examples/cf-worker-otel` directory to stream data into Logflare. The worker demonstrates sending OpenTelemetry traces alongside synchronous and batched HTTP events so you can validate both ingestion paths before deploying to production.

## Prerequisites

- A Logflare source UUID and API key for authentication
- [Wrangler](https://developers.cloudflare.com/workers/wrangler/install-and-update/) with Node.js/npm installed locally

## Configure the example Worker

1. In the repository root, navigate to `examples/cf-worker-otel`. The example includes scripts for installing dependencies and running the worker locally with Wrangler.
2. Create a `.dev.vars` file in that directory with the required environment variables:

   ```bash
   API_KEY=your_logflare_api_key
   SOURCE=your_source_uuid
   ```

   Wrangler automatically loads these values for `wrangler dev`, matching the `Env` interface used by the worker code.
3. Install dependencies and start the development server:

   ```bash
   npm install
   npm start
   ```

   The `start` script runs `wrangler dev`, which boots the worker locally.

## How the worker sends data

- Traces are exported via OpenTelemetry using the URL and headers configured in `src/index.ts`. Update the exporter `url` to point at your Logflare OTLP endpoint (for example, `https://otel.logflare.app:443`) when targeting hosted Logflare.
- Synchronous events are posted to `/api/events` with your source and API key headers, and an asynchronous batch sender keeps queueing events while the worker runs.

You can inspect and adjust the payloads in `src/index.ts` if you want to change what the worker emits. When ready to deploy, run `npm run deploy` to publish the worker with your Cloudflare account.

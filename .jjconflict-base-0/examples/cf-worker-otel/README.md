# Example - Cloudflare Worker

This cloudflare worker does the following:

1. sends OpenTelemetry traces
2. sends a syncronous event via HTTP fetch
3. sends batched events via HTTP fetch (TODO)

To run the app:

1. Add appropriate values to `.dev.vars`
2. `npm i`
3. `npm start`

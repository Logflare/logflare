# Vector gRPC ingestion (local dev)

This example wires a [Vector](https://vector.dev) container into the Logflare
docker-compose stack so the native `vector` gRPC sink can be exercised
end-to-end against a local Logflare instance.

Vector emits Apache-format demo logs and internal metrics, transforms them,
and forwards them via the `vector` sink to Logflare's gRPC server on port
`50051` using the `vector.Vector/PushEvents` RPC.

## Usage

1. Start Logflare (`logflare`, `db`, etc.) via `docker compose up logflare db`.
2. In Logflare's web UI, create a source and an access token with the
   `ingest` scope.
3. Export the credentials and start the vector service:

   ```bash
   export LOGFLARE_SOURCE_TOKEN=<source uuid>
   export LOGFLARE_API_KEY=<access token>
   docker compose up vector
   ```

4. Tail the source in Logflare to verify ingested log and metric events.

To point Vector at a Logflare running outside docker compose, override
`LOGFLARE_GRPC_ENDPOINT` (e.g. `http://host.docker.internal:50051`).

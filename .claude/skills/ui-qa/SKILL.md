---
name: ui-qa
description: Screenshot-verify the Logflare UI with `mix qa.ui`, which drives Chromium through PlaywrightEx and saves each capture with plain-English expectations that you check against the PNG. Use on /ui-qa, to check a release Docker image after a change to the Dockerfile, the release, or asset bundling, and to check a UI change on the dev server before you report it done.
---

# UI QA

Run the specs against a release image (Procedure A, for changes to `Dockerfile*`, releases,
`assets/build.mjs` or `mix phx.digest`) or the dev server (Procedure B, for templates,
LiveViews, CSS or JS), then verify every capture (Procedure C).

The harness is Elixir in `test/support/qa/`, shared with the `ingest-qa` skill. Specs live in
`test/support/qa/ui/` and use `Logflare.QA.Browser`. A spec asserts the DOM state with
`Browser.wait_for/2`, then captures a PNG plus a JSON file of one to three claims about the
picture. The claims are not executed: you confirm or refute each one by looking at the PNG,
which catches a page that passes its DOM checks but still looks broken.

## Before you start

```sh
mix deps.get
npm --prefix assets ci   # assets/node_modules holds the Playwright driver
```

If Chromium is missing, run `npx --prefix assets playwright install chromium`, or set
`PLAYWRIGHT_CHROMIUM_PATH`. In a cloud session it is `/opt/pw-browsers/chromium`.

## Procedure A: run a release image

1. Build the commit to test: `docker build -t logflare:qa .`. If the PR has a merge conflict,
   merge the base locally first, since the merged tree is what ships. Behind a TLS-re-signing
   proxy, see [Builds behind a TLS proxy](#builds-behind-a-tls-proxy).
2. Inspect the image:

   ```sh
   docker run --rm --entrypoint sh logflare:qa -c '
     cd /opt/app/rel/logflare && S=$(ls -d lib/logflare-*)/priv/static
     ls -d lib/logflare-* "$S/cache_manifest.json" "$S/js" bin/priv 2>&1; ls "$S/js"'
   ```

   Expect exactly one `lib/logflare-<VERSION>`, a `cache_manifest.json`, `app.css` and `app.js`
   with digested and `.gz` copies, and no `bin/priv`. To compare with the base branch, build it
   as `logflare:base` and diff `find . -type f | sort` run in both images' `priv/static`.
3. Start it in single-tenant Postgres mode, which needs no cloud credentials:

   ```sh
   docker network create lfqa
   docker run -d --name lfqa-db --network lfqa \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=logflare \
     -v "$PWD/priv/setup.sql:/docker-entrypoint-initdb.d/setup.sql:ro" postgres:15
   docker restart lfqa-db   # applies wal_level = logical from setup.sql
   docker run -d --name lfqa-app --network lfqa -p 4000:4000 \
     -e DB_HOSTNAME=lfqa-db -e DB_PORT=5432 -e DB_DATABASE=logflare \
     -e DB_USERNAME=postgres -e DB_PASSWORD=postgres -e LOGFLARE_SINGLE_TENANT=true \
     -e POSTGRES_BACKEND_URL=postgresql://postgres:postgres@lfqa-db:5432/logflare \
     -e POSTGRES_BACKEND_SCHEMA=analytics -e LOGFLARE_PUBLIC_ACCESS_TOKEN=qa-public \
     -e LOGFLARE_NODE_HOST=127.0.0.1 -e GOOGLE_PROJECT_ID=logflare-qa logflare:qa
   until curl -fs -o /dev/null localhost:4000/health; do sleep 2; done
   ```

   Keep `GOOGLE_PROJECT_ID`: without it every source page returns 500. Do not set `DB_SCHEMA`
   unless that schema exists. If the app does not start, read `docker logs lfqa-app`.
4. Check digests and cache headers, which the specs do not cover:

   ```sh
   for p in /js/app.css /js/app.js /images/favicon.ico "/js/app.js?vsn=d"; do
     curl -s -o /dev/null -w "%{http_code} %{content_type} cache=%header{cache-control} $p\n" "localhost:4000$p"
   done
   curl -s -o /dev/null -H 'Accept-Encoding: gzip' -w '%{http_code} enc=%header{content-encoding}\n' localhost:4000/js/app.js
   ```

   Expect 200 with the right content type, `public, max-age=31536000, immutable` for `?vsn=d`,
   and `enc=gzip`.
5. Do Procedure C, then clean up: `docker rm -f lfqa-app lfqa-db && docker network rm lfqa`.

## Procedure B: run the dev server

Start Postgres (`docker compose up -d db`), then run `mix qa.server > /tmp/logflare-qa-server.log 2>&1 &`
and wait for `curl -fs localhost:4000/health`. It runs `mix phx.server` in single-tenant Postgres
mode and creates and migrates the databases. The first start compiles the Rust NIFs, which can
take over ten minutes. The asset watcher rebuilds `assets/` changes, so rerun Procedure C after
each one. Stop the server with `kill` on its PID.

## Procedure C: run and verify the specs

1. Run `mix qa.ui`, or `mix qa.ui dashboard` for one spec. Set `LOGFLARE_URL` to target another
   server. Captures go to `tmp/qa/`.
2. Verify every capture yourself; do not delegate this. Read each `tmp/qa/<name>.json` and its
   PNG, and mark each expectation confirmed or refuted. A refuted expectation is a bug: fix it or
   report it, never edit the expectation to fit a broken picture.
3. To cover a new flow, extend the closest spec in `test/support/qa/ui/` or add one there and to
   `@specs` in `test/support/qa/mix/qa.ui.ex`. Drive the page as a user does, wait for the DOM
   state before each capture, end with `Browser.assert_no_problems!/1`, and write each
   expectation as something visible: a color, a position, an icon or some text.

Report the commit tested, image sizes and the asset location for Procedure A, the spec results,
each expectation with its verdict, the screenshots, and each failure with the command that
shows it.

## Builds behind a TLS proxy

When a proxy re-signs HTTPS, as in a Claude Code cloud session, `curl`, `git`, `hex`, `npm` and
`cargo` fail inside the build. Without committing the change, copy the `Dockerfile` to
`../Dockerfile.qa` and add these lines after each `FROM`:

```dockerfile
COPY --from=ccr ca-bundle.crt /tmp/ccr-ca.crt
ARG SSL_CERT_FILE=/tmp/ccr-ca.crt CURL_CA_BUNDLE=/tmp/ccr-ca.crt NODE_EXTRA_CA_CERTS=/tmp/ccr-ca.crt CARGO_HTTP_CAINFO=/tmp/ccr-ca.crt HEX_CACERTS_PATH=/tmp/ccr-ca.crt GIT_SSL_CAINFO=/tmp/ccr-ca.crt RUSTUP_USE_CURL=1
```

Then build with `docker build --network host -f ../Dockerfile.qa --build-context ccr=/root/.ccr
--build-arg HTTPS_PROXY="$HTTPS_PROXY" --build-arg NO_PROXY="$NO_PROXY" -t logflare:qa .`. If
Docker Hub returns 429, also pass `BUILDER_IMAGE` and `RUNNER_IMAGE` through `mirror.gcr.io`,
with the tags from the `Dockerfile`. Start the daemon with `dockerd > /tmp/dockerd.log 2>&1 &`
if it is not running.

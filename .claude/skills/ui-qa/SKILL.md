---
name: ui-qa
description: Screenshot-verify the Logflare UI with the Playwright harness in scripts/screenshot/. Each capture carries plain-English expectations that you check against the PNG. Use on /ui-qa, to check a release Docker image after a change to the Dockerfile, the release, or asset bundling, and to check a UI change on the dev server before you report it done.
---

# UI QA

This skill tells you how to verify the Logflare UI with screenshots. It has three parts:

- **Procedure A** builds and runs a release Docker image. Use it when a change touches `Dockerfile*`, `mix.exs` releases, `assets/build.mjs`, or `mix phx.digest`.
- **Procedure B** runs the dev server. Use it when a change touches templates, LiveViews, CSS, or JS.
- **Procedure C** runs the screenshot specs against either server and verifies each screenshot.

## The screenshot harness

The harness is in `scripts/screenshot/`. It is a small npm package with `@playwright/test`.

| File                  | Purpose                                                                       |
| --------------------- | ----------------------------------------------------------------------------- |
| `specs/*.spec.ts`     | One spec for each flow. A spec drives the real app in Chromium.               |
| `capture.ts`          | `capture(page, { name, expectations })` writes a PNG and a JSON manifest.     |
| `fixtures.ts`         | Fails a spec on a static-asset error, a failed request, or a console error.  |
| `playwright.config.ts`| Reads the server URL from `LOGFLARE_URL`. The default is `localhost:4000`.    |

A spec has two kinds of checks:

- **DOM assertions** use Playwright `expect`. They prove that the page reached a state before the capture.
- **Expectations** are one to three plain-English claims about the picture: colors, layout, icons, copy. The harness does not run them. It writes them to `.generated/<name>.json` next to `.generated/<name>.png`. You read the PNG and confirm or refute each claim.

A spec can pass all its DOM assertions and still show a broken page. The expectations catch that.

## Before you start

1. Make sure that Docker runs: `docker info`.
2. Install the harness: `npm --prefix scripts/screenshot ci`.
3. If Chromium is not installed, install it: `npx --prefix scripts/screenshot playwright install chromium`.

> **Note: cloud sessions.** Chromium is in `/opt/pw-browsers`. Do not install it again.
> Docker Hub can return `429 Too Many Requests`. If it does, pull through `mirror.gcr.io`.
> An HTTPS proxy can also re-sign TLS traffic. Then `curl`, `git`, `hex`, `npm`, and `cargo` fail inside the build.
> To fix this, see [Builds behind a TLS proxy](#builds-behind-a-tls-proxy).

## Procedure A: run a release image

### A1. Build the image

1. Check out the commit that you want to test.
2. If the PR has a merge conflict, merge the base branch locally. Test the merged tree, because that tree ships.
3. Build the image: `docker build -t logflare:qa .`
4. Make sure that the build exits with code `0`.

To compare with the base branch, build it as `logflare:base` in a second worktree.
The builder layers stay in the cache, so the second build is fast.

### A2. Inspect the image contents

Run this command:

```sh
docker run --rm --entrypoint sh logflare:qa -c '
  cd /opt/app/rel/logflare
  ls -d lib/logflare-*
  S=$(ls -d lib/logflare-*)/priv/static
  ls "$S" "$S/js"
  ls -la "$S/cache_manifest.json"
  ls -d bin/priv 2>&1
  find / -path "*priv/static" -type d 2>/dev/null'
```

Make sure that:

- There is exactly one `lib/logflare-<VERSION>` directory. `<VERSION>` is the contents of `VERSION`.
- `priv/static` contains `cache_manifest.json`.
- `priv/static/js` contains `app.css`, `app.js`, and digested copies such as `app-<hash>.css`.
- Each asset has a `.gz` copy.
- `priv/static/images` contains the favicons.
- `bin/priv` does not exist. A second copy of `priv/static` wastes space.
- Only one `priv/static` belongs to the `logflare` app. Dependency apps, such as `phoenix`, have their own.

To compare the asset list with the base image, run this for both images and `diff` the output:

```sh
docker run --rm --entrypoint sh logflare:qa -c \
  'cd /opt/app/rel/logflare/lib/logflare-*/priv/static && find . -type f | sort'
```

The file names must be the same. The digest hashes can change if the source changed.

To compare image sizes, run `docker image ls logflare`.

### A3. Start the image

Use single-tenant mode with the Postgres backend. This mode needs no cloud credentials.

1. Create a network:

   ```sh
   docker network create lfqa
   ```

2. Start Postgres. The `setup.sql` file creates the `analytics` schema and turns on logical replication.

   ```sh
   docker run -d --name lfqa-db --network lfqa \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=logflare \
     -v "$PWD/priv/setup.sql:/docker-entrypoint-initdb.d/setup.sql:ro" \
     postgres:15
   ```

3. Restart Postgres once, so that `wal_level = logical` applies: `docker restart lfqa-db`.
4. Start the app:

   ```sh
   docker run -d --name lfqa-app --network lfqa -p 4000:4000 \
     -e DB_HOSTNAME=lfqa-db -e DB_PORT=5432 -e DB_DATABASE=logflare \
     -e DB_USERNAME=postgres -e DB_PASSWORD=postgres \
     -e LOGFLARE_SINGLE_TENANT=true \
     -e POSTGRES_BACKEND_URL=postgresql://postgres:postgres@lfqa-db:5432/logflare \
     -e POSTGRES_BACKEND_SCHEMA=analytics \
     -e LOGFLARE_PUBLIC_ACCESS_TOKEN=qa-public -e LOGFLARE_PRIVATE_ACCESS_TOKEN=qa-private \
     -e LOGFLARE_NODE_HOST=127.0.0.1 \
     -e GOOGLE_PROJECT_ID=logflare-qa \
     logflare:qa
   ```

5. Wait for the health check to return `200`:

   ```sh
   until curl -fs -o /dev/null localhost:4000/health; do sleep 2; done
   ```

6. If the app does not start, read the log: `docker logs lfqa-app`.

> **Caution:** Do not set `DB_SCHEMA` unless the schema exists. If it does not exist, the migrations fail with `no schema has been selected to create in`.

> **Caution:** Keep `GOOGLE_PROJECT_ID`. Without it, each source page returns `500` in single-tenant Postgres mode.
> The cause is `Source.generate_bq_table_id/1`, which needs a project ID even when BigQuery is not in use.

### A4. Check the static files with curl

The screenshot specs do not check digests or cache headers. Check them with this command:

```sh
for p in /js/app.css /js/app.js /images/favicon.ico /robots.txt /manifest.json /worker.js "/js/app.js?vsn=d"; do
  curl -s -o /dev/null -w "%{http_code} %{content_type} cache=%header{cache-control} $p\n" "localhost:4000$p"
done
curl -s -o /dev/null -H 'Accept-Encoding: gzip' -w '%{http_code} enc=%header{content-encoding}\n' localhost:4000/js/app.js
```

Make sure that:

- Each path returns `200` with the correct content type.
- A `?vsn=d` request returns `cache-control: public, max-age=31536000, immutable`. This header shows that the cache manifest loaded.
- The gzip request returns `enc=gzip`.

Then do [Procedure C](#procedure-c-run-and-verify-the-screenshot-specs).

### A5. Clean up

```sh
docker rm -f lfqa-app lfqa-db && docker network rm lfqa
```

## Procedure B: run the dev server

The dev server builds assets with the `npm run watch` watcher in `config/dev.exs`.
The watcher writes the output to `priv/static`. The dev server does not use digests.

### B1. Start the dev server

1. Start Postgres: `docker compose up -d db`.
2. Install the dependencies:

   ```sh
   npm --prefix assets ci
   mix deps.get
   ```

3. Create and migrate the database: `mix ecto.setup`.
4. Start the server in single-tenant Postgres mode:

   ```sh
   LOGFLARE_SINGLE_TENANT=true \
   POSTGRES_BACKEND_URL=postgresql://postgres:postgres@localhost:5432/logflare_dev \
   GOOGLE_PROJECT_ID=logflare-qa \
   mix phx.server
   ```

5. Wait for the health check to return `200`:

   ```sh
   until curl -fs -o /dev/null localhost:4000/health; do sleep 2; done
   ```

6. Do [Procedure C](#procedure-c-run-and-verify-the-screenshot-specs).
7. To test a change to CSS or JS, edit the file under `assets/`. The watcher rebuilds it. Then run Procedure C again.
8. When you finish, press `Ctrl+C` two times in the server terminal. Then stop Postgres: `docker compose stop db`.

> **Note:** `make start` and `make start.st.pg` read `.dev.env`. That file holds team secrets and is not in Git.
> If you have the file, you can use `make start.st.pg`. If not, use the `mix phx.server` command above.

> **Note:** The first start compiles the Rust NIFs. This can take more than 10 minutes.

> **Note:** On a new dev database, the log can show `No Free Plan created yet in database` from `Users.CacheWarmer`.
> This error does not stop the server and does not affect the UI.

## Procedure C: run and verify the screenshot specs

### C1. Write or extend a spec

Do this step only when the change has a flow that no spec covers.

1. Find the closest spec in `scripts/screenshot/specs/`. If a spec covers the same flow, add captures to it. Do not copy its setup into a new file.
2. Otherwise, create `scripts/screenshot/specs/<slug>.spec.ts` with this shape:

   ```ts
   import { capture } from "../capture";
   import { expect, test } from "../fixtures";

   test("<what the user does>", async ({ page }) => {
     await page.goto("/dashboard");
     await expect(page.getByText("New source")).toBeVisible();

     await capture(page, {
       name: "<slug>-01-before",
       expectations: ["A claim about what the picture shows."],
     });

     await page.getByText("New source").click();
     await expect(page).toHaveURL(/\/sources\/new/);

     await capture(page, {
       name: "<slug>-02-after",
       expectations: ["A claim about what changed in the picture."],
     });
   });
   ```

3. Import `test` and `expect` from `../fixtures`, not from `@playwright/test`. The fixture adds the asset and console checks.
4. Drive the page as a user does: `click`, `fill`, `press`, `hover`. Do not call app internals.
5. Before each capture, assert the DOM state with `expect`. A capture of a page that has not loaded proves nothing.
6. Give each capture a name in the form `<slug>-<NN>-<what-it-shows>`. The name becomes the PNG and JSON file names.
7. Write one to three expectations for each capture. `capture` rejects zero or more than three.
   If you need more claims, the screenshot shows more than one thing. Take a second capture.
8. Write each expectation as a claim that you can see: a color, a position, an icon, some text.
   Do not repeat a DOM assertion as an expectation.
9. To capture one element, set `clipSelector`. To show `:hover` styles, set `hoverSelector`.

`specs/dashboard.spec.ts` shows a page check and a clipped capture. `specs/new-source.spec.ts` shows a form flow.

### C2. Run the specs

1. Run all specs:

   ```sh
   npm --prefix scripts/screenshot run screenshot
   ```

2. To run one spec, add its path: `npm --prefix scripts/screenshot run screenshot -- specs/<slug>.spec.ts`.
3. To use a different server, set `LOGFLARE_URL`, for example `LOGFLARE_URL=http://localhost:4001`.
4. If Playwright cannot find Chromium, set `PLAYWRIGHT_CHROMIUM_PATH` to the Chromium executable.
5. Make sure that each spec passes. A fixture failure lists each asset error or console error.

The output goes to `scripts/screenshot/.generated/`. Git ignores this directory.

### C3. Verify each screenshot

Do this step yourself. Do not give it to a subagent. The purpose is that the agent who ships the change looks at the pixels.

For each capture:

1. Read `scripts/screenshot/.generated/<name>.json`.
2. Read `scripts/screenshot/.generated/<name>.png`.
3. For each expectation, decide if the image confirms it or refutes it.
4. Write down each refuted expectation and what the image shows instead.

To list all expectations at once, run:

```sh
for f in scripts/screenshot/.generated/*.json; do
  jq -r '"\(.name) (\(.url))", (.expectations[] | "  - " + .)' "$f"
done
```

A refuted expectation is a bug. Fix it, or report it. Do not change the expectation to match a broken picture.

## Report the result

Give the reviewer:

- The commit that you tested, and the base you merged, if any.
- For an image: the image size, the base image size, the `priv/static` location, and the file count.
- The spec results.
- Each expectation, marked confirmed or refuted.
- The screenshots. Send them with `SendUserFile` when you can.
- Each failure, with the command that shows it.

Keep the specs. `scripts/screenshot/specs/` is a regression library. When a later change touches the same flow, extend its spec.

## Builds behind a TLS proxy

Use this section only when a proxy re-signs HTTPS traffic, for example in a Claude Code cloud session.
Do not commit these changes.

1. Copy the `Dockerfile` to a scratch path, for example `../Dockerfile.qa`.
2. After each `FROM` line, add these lines. `ARG` values apply to `RUN` steps only, so they do not stay in the final image.

   ```dockerfile
   COPY --from=ccr ca-bundle.crt /tmp/ccr-ca.crt
   ARG SSL_CERT_FILE=/tmp/ccr-ca.crt CURL_CA_BUNDLE=/tmp/ccr-ca.crt NODE_EXTRA_CA_CERTS=/tmp/ccr-ca.crt CARGO_HTTP_CAINFO=/tmp/ccr-ca.crt HEX_CACERTS_PATH=/tmp/ccr-ca.crt GIT_SSL_CAINFO=/tmp/ccr-ca.crt RUSTUP_USE_CURL=1
   ```

3. Build with the host network, the proxy, and the Docker Hub mirror:

   ```sh
   docker build --network host -f ../Dockerfile.qa \
     --build-context ccr=/root/.ccr \
     --build-arg HTTPS_PROXY="$HTTPS_PROXY" --build-arg NO_PROXY="$NO_PROXY" \
     --build-arg BUILDER_IMAGE=mirror.gcr.io/hexpm/elixir:<tag> \
     --build-arg RUNNER_IMAGE=mirror.gcr.io/library/debian:<tag> \
     -t logflare:qa .
   ```

   Copy each `<tag>` from the `ARG` lines at the top of the `Dockerfile`.

4. If the Docker daemon does not run, start it: `dockerd > /tmp/dockerd.log 2>&1 &`.

The CA file is copied into `/tmp` of the image. It does not change how the app serves assets.

For Procedure B in a container, pass the same CA variables with `-e`.
`mix deps.get` fetches some dependencies from GitHub with `git`, so `GIT_SSL_CAINFO` is necessary.

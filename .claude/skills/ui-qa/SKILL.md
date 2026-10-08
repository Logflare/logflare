---
name: ui-qa
description: Verify that the Logflare UI and its static assets (CSS, JS, images) load correctly. Use this skill to check a release Docker image after a change to the Dockerfile, the release, or asset bundling. Also use it to check a UI change on the dev server. The check builds and runs the app, inspects the files, and loads pages in Playwright.
---

# UI QA

This skill tells you how to verify the Logflare UI. It has two procedures:

- **Procedure A** checks a release Docker image. Use it when a change touches `Dockerfile*`, `mix.exs` releases, `assets/build.mjs`, or `mix phx.digest`.
- **Procedure B** checks the dev server. Use it when a change touches templates, LiveViews, CSS, or JS.

Both procedures use `check-assets.cjs` in this folder. The script opens `/` in Chromium.
It records each static asset that the page loads or refers to.
It fails if an asset returns an error, if no same-origin stylesheet has rules, or if an image is broken.
It also saves a screenshot.

Do not stop at a passing script. Look at the screenshot. A page without styles is a failure.

## Before you start

1. Make sure that Docker runs: `docker info`.
2. Make sure that Playwright is available: `NODE_PATH="$(npm root -g)" node -e 'require("playwright")'`.
3. If Playwright is not available, install it: `npm install -g playwright && npx playwright install chromium`.

> **Note: cloud sessions.** Docker Hub can return `429 Too Many Requests`. If it does, pull through `mirror.gcr.io`.
> An HTTPS proxy can also re-sign TLS traffic. Then `curl`, `git`, `hex`, `npm`, and `cargo` fail inside the build.
> To fix this, see [Builds behind a TLS proxy](#builds-behind-a-tls-proxy).

## Procedure A: verify a release image

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

### A3. Run the image

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
     logflare:qa
   ```

5. Wait for the health check to return `200`:

   ```sh
   until curl -fs -o /dev/null localhost:4000/health; do sleep 2; done
   ```

6. If the app does not start, read the log: `docker logs lfqa-app`.

> **Caution:** Do not set `DB_SCHEMA` unless the schema exists. If it does not exist, the migrations fail with `no schema has been selected to create in`.

### A4. Check the static files with curl

Run this command:

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

### A5. Check the UI with Playwright

1. Run the script:

   ```sh
   NODE_PATH="$(npm root -g)" node .claude/skills/ui-qa/check-assets.cjs http://localhost:4000 ui-qa-image.png
   ```

2. Make sure that the script prints `PASS`.
3. Make sure that the CSS and JS URLs contain a digest hash and `?vsn=d`. A plain `/js/app.css` means that the app did not find the cache manifest.
4. Open the screenshot. Make sure that the page has styles, icons, and a layout.

In single-tenant mode, `/` goes to `/dashboard`, so you do not need to log in.

### A6. Clean up

```sh
docker rm -f lfqa-app lfqa-db && docker network rm lfqa
```

## Procedure B: verify the dev server

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
   mix phx.server
   ```

5. Wait for the health check to return `200`:

   ```sh
   until curl -fs -o /dev/null localhost:4000/health; do sleep 2; done
   ```

> **Note:** `make start` and `make start.st.pg` read `.dev.env`. That file holds team secrets and is not in Git.
> If you have the file, you can use `make start.st.pg`. If not, use the `mix phx.server` command above.

> **Note:** The first start compiles the Rust NIFs. This can take more than 10 minutes.

> **Note:** On a new dev database, the log can show `No Free Plan created yet in database` from `Users.CacheWarmer`.
> This error does not stop the server and does not affect assets.

### B2. Check the UI with Playwright

1. Run the script:

   ```sh
   NODE_PATH="$(npm root -g)" node .claude/skills/ui-qa/check-assets.cjs http://localhost:4000 ui-qa-dev.png
   ```

2. Make sure that the script prints `PASS`.
3. Open the screenshot. Make sure that the page has styles, icons, and a layout.
4. To test a change to CSS or JS, edit the file under `assets/`.
5. Wait for the watcher to rebuild. Live reload refreshes the page.
6. Run the script again.

To check a page other than `/`, copy the script and change the `page.goto` path.

### B3. Stop the dev server

1. Press `Ctrl+C` two times in the server terminal.
2. Stop Postgres: `docker compose stop db`.

## Report the result

Give the reviewer:

- The commit that you tested, and the base you merged, if any.
- The image size, and the base image size, if you compared them.
- The `priv/static` location and the file count.
- The `check-assets.cjs` output.
- The screenshot.
- Each failure, with the command that shows it.

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

For Procedure B in a container, pass the same CA variables with `-e`, and add `GIT_SSL_CAINFO`.
`mix deps.get` fetches some dependencies from GitHub with `git`.

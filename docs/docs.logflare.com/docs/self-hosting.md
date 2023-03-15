# Self Hosting

Logflare can be self-hosted. As of now, only a single machine setup is supported.

## Pre-requisites

To ensure that you have sufficient permissions to insert into your Google Cloud BigQuery, ensure that the following service accounts have been set up:

- Project Service ACcount

You will also need to have a project created and the project ID and number on hand.

## `docker-compose`

Using docker compose is the **recommended method** for self-hosting.

1. Create the `docker-compose.yml`

```yaml
services:
  db:
    image: postgres:13.4-alpine
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_USER: postgres
    ports:
      - "5432:5432"
    volumes:
      - ./priv/wal.sql:/docker-entrypoint-initdb.d/wal.sql
      - pg-data:/var/lib/postgresql/data
  logflare:
    build:
      dockerfile: ./Dockerfile
    ports:
      - "4000:4000"
    hostname: 127.0.0.1
    volumes:
      - type: bind
        source: ${PWD}/.docker.env
        target: /tmp/.secrets.env
        read_only: true
      - type: bind
        source: ${PWD}/gcloud.json
        target: /opt/app/rel/logflare/bin/gcloud.json
        read_only: true
    depends_on:
      - db
```

2. Create a `.logflare.env` environment file. You can also base the file contents on this [reference file](https://github.com/Logflare/logflare/blob/master/.docker.env).

```text
DB_DATABASE=logflare_docker
DB_HOSTNAME=localhost
DB_PORT=5432
DB_PASSWORD=postgres
DB_USERNAME=postgres

LOGFLARE_SINGLE_TENANT=true
LOGFLARE_API_KEY=my-cool-api-key
GOOGLE_DATASET_ID_APPEND=_dev
GOOGLE_PROJECT_ID=logflare-docker-example
GOOGLE_PROJECT_NUMBER=123123123213
```

3. Download your Google Cloud API JWT and store it under `gcloud.json` in your working directory.

4. Run `docker-compose up -d` and visit http://localhost:4000

## Configuration

### `LOGFLARE_SINGLE_TENANT`

> Boolean, required, defaults to false

This is will seed a singular user into the database, and will disable browser authentication. All browser usage will default to this user. Inviting team users and other team-related functionality is currently not supported for self-hosted. Logflare self-hosted is currently intended for single-user experience only.

### `LOGFLARE_API_KEY`

> String, optional, defaults to `nil`

Allows you to pass in an API key that will used for authentication. This is intended for programmatic usage where an external program sets the API key. It is advised to use the UI to configure the access tokens instead. If this value is not provided, the default API key will be automatically generated.

### `LOGFLARE_SUPABASE_MODE`

> Boolean, defaults to false

This is a special mode for Logflare which will seed additional resources for usage with Supabase self-hosted.

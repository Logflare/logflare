# Self Hosting

Logflare can be self-hosted. As of now, only a single machine setup is supported.

## Pre-requisites

You will need a Google Cloud project **with billing enabled** in order to proceed.

The requirements for server startup are as follows after creating the project:

- Project ID
- Project number
- A service account key

### Setting up BigQuery Service Account

To ensure that you have sufficient permissions to insert into your Google Cloud BigQuery, ensure that you have created a service account with either:

- BigQuery Admin role; or
- The following permissions:
  - bigquery.datasets.create
  - bigquery.datasets.get
  - bigquery.datasets.getIamPolicy
  - bigquery.datasets.update
  - bigquery.jobs.create
  - bigquery.routines.create
  - bigquery.routines.update
  - bigquery.tables.create
  - bigquery.tables.delete
  - bigquery.tables.get
  - bigquery.tables.getData
  - bigquery.tables.update
  - bigquery.tables.updateData

We recommend setting the BigQuery Admin role, as it simplifies permissions setup.

### Obtaining the BigQuery Service Account Key

In order for Logflare to connect sources to their relevant BigQuery tables, we would need to have a service account key that can sign the JWTs needed to authenticate with the Google Cloud APIs.

To obtain the BigQuery service account key after creating it, navigate to IAM > Service Accounts in the web console and click on the "Manage Keys" action option.

![Service Account List Manage Keys Action](./service-account-mange-keys.png)

Thereafter, click on "Add Key" to create a new key. The key will be in a JSON format. Store this key securely on your host machine.

![Add Key Button](add-key.png)

You can also obtain the key via the `gcloud` cli by following the [official documentation](https://cloud.google.com/iam/docs/keys-create-delete).

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
      POSTGRES_DATABASE: logflare_docker
    ports:
      - "5432:5432"
    volumes:
      - ./priv/wal.sql:/docker-entrypoint-initdb.d/wal.sql
      - pg-data:/var/lib/postgresql/data
  logflare:
    image: supabase/logflare:1.0.1
    ports:
      - "4000:4000"
    hostname: 127.0.0.1
    environment:
      - DB_DATABASE=logflare_docker
      - DB_HOSTNAME=db
      - DB_PORT=5432
      - DB_PASSWORD=postgres
      - DB_USERNAME=postgres
      - LOGFLARE_SINGLE_TENANT=true
      - LOGFLARE_API_KEY=my-cool-api-key
      - GOOGLE_DATASET_ID_APPEND=_your_env
      - GOOGLE_PROJECT_ID=logflare-docker-example
      - GOOGLE_PROJECT_NUMBER=123123123213
    volumes:
      - type: bind
        source: ${PWD}/.env
        target: /tmp/.secrets.env
        read_only: true
      - type: bind
        source: ${PWD}/gcloud.json
        target: /opt/app/rel/logflare/bin/gcloud.json
        read_only: true
    depends_on:
      - db
```

2. Using the Service Account key that you had obtained under [the pre-requisites section](#pre-requisites), move and rename the JSON file to `gcloud.json` in your working directory.

The directory structure should be as follows:

```
\
|- gcloud.json
|- docker-compose.yml
```

4. Run `docker-compose up -d` and visit http://localhost:4000

### Using an `.env` file

You can optionally use a `.env` file to manage your environemnts. You can base the file contents on this [reference file](https://github.com/Logflare/logflare/blob/master/.docker.env)

:::note
You cannot have comments in the env file as we load it at startup via `xargs`.
:::

```yaml
# ... the rest is the same
volumes:
  # add in this bind bound. If you have a different name or location, update the source
  - type: bind
    source: ${PWD}/.env
    target: /tmp/.secrets.env
    read_only: true
```

The directory structure will now be as follows:

```
\
|- gcloud.json
|- .env
|- docker-compose.yml
```

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

### `PHX_HTTP_PORT`

> Defaults to 4000

Allows configuration of the HTTP port that Logflare will run on.

### `DB_SCHEMA`

> String, defaults to nil

This ENV variable sets the search path to a custom database schema. This allows you to customize the schema used on the database.

### `LOGFLARE_LOG_LEVEL`

> string, defualts to `info`. Options: `error|warn|info|debug`

Allows the setting of the log level at runtime. For production settings, we advise `warn`.

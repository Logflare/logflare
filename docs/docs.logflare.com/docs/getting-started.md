---
sidebar_position: 2
---

# Getting Started

## Step 1: Create an Account

Head over to the [Logflare site](https://logflare.app/) and [create an account](https://logflare.app/auth/login).

## Step 2: Create a Source

Once you're logged in, you can create a **New Source**.

Retrieve your source ID and API Key by clicking on the setup button.

## Step 3: Send a Log

Once your source is created , execute this cURL command to send a log event to Logflare.

Replace `YOUR-SOURCE-ID-HERE` and `YOUR-API-KEY-HERE` placeholders with the values from step 2.

```bash
curl -X "POST" "https://api.logflare.app/logs/json?source=YOUR-SOURCE-ID-HERE" \
        -H 'Content-Type: application/json; charset=utf-8' \
        -H 'X-API-KEY: YOUR-API-KEY-HERE' \
        -d $'[{
        "message": "This is the main event message",
        "metadata": {"some": "log event"}
    }]'
```

## Step 4: Check the Source

Congratulations, your first log event should be successfully POST-ed to Logflare! You can then search and filter the source for specific events using the [Logflare Query Language](./lql).

Of course, the JSON endpoint is not the only way to send log events to Logflare, check out our [integrations](./integrations) to the list of supported clients.

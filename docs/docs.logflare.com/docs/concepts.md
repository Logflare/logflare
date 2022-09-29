---
sidebar_position: 3
---

# Main Concepts

## Adaptive Schema

As your logging needs changes, Logflare is capable of detecting and adjusting the database schema accordingly. This allows you to focus on analyzing your logs instead of having to manage your logging pipeline manually.

Suppose your initial log events had the following shape:

```json
{
  "message": "This is my log event",
  "metadata": {
    "my": "first log"
  }
}
```

The generated schema would be the following:

```
message: string;
metadata: {
    my: string;
}
```

As your application requirements change, suppose you now need to add in new information to your log events.

```json
{
  "message": "This is my new log event",
  "metadata": {
    "my": "first log",
    "counter": 123
  }
}
```

Logflare will now detect the schema changes in the event, and add in the new column to the source's underlying table.

```ts
message: string;
metadata: {
    my: string;
    counter: number;
}
```

The schema changes is done automatically. If this is not the desired behaviour, you can disable this by locking the schema in the source's settings.

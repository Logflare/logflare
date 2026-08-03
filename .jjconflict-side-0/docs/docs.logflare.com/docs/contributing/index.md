---
sidebar_position: 11
---

# Contributing


## Backends
Logflare supports pluggable backends through adaptor modules. This guide helps you implement a new backend and submit it upstream.

All existing adaptors can be seen [here](https://github.com/Logflare/logflare/tree/main/lib/logflare/backends/adaptor) as examples. 

## Architecture overview

Backend adaptors live in [`lib/logflare/backends/adaptor`](https://github.com/Logflare/logflare/blob/main/lib/logflare/backends/adaptor.ex) and implement the `Logflare.Backends.Adaptor` behaviour. Required callbacks include `start_link/1`, `cast_config/1`, `validate_config/1`, and `execute_query/3`.

Each adaptor manages a dedicated Broadway pipeline for processing events. Pipelines may optionally scale dynamically using the `Logflare.DynamicPipeline` module.

## HTTP services with WebhookAdaptor

`Logflare.Backends.Adaptor.WebhookAdaptor` provides a generic pipeline for delivering events over HTTP. It supports automatic Finch pool selection, optional pool overrides, gzip, and dynamic `url_override` handling. Adaptors such as Datadog and Loki wrap this module for their HTTP integrations.

### Example adaptor

```elixir
defmodule Logflare.Backends.Adaptor.MyServiceAdaptor do
  @behaviour Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  def transform_config(%_{config: config}) do
    %{url: "https://api.example.com/logs", headers: %{"authorization" => "Bearer #{config.token}"}, http: "http2"}
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{token: :string}}
    |> Ecto.Changeset.cast(params, [:token])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    Ecto.Changeset.validate_required(changeset, [:token])
  end
end
```

defmodule Logflare.Backends.Adaptor.WebhookAdaptor.EgressMiddleware do
  @moduledoc false

  alias Logflare.Utils
  require OpenTelemetry.Tracer

  @behaviour Tesla.Middleware

  @impl true
  def call(env, next, options) do
    body_len =
      if is_binary(env.body) do
        IO.iodata_length(env.body)
      else
        0
      end

    headers_len =
      for {k, v} <- env.headers, reduce: 0 do
        acc when is_binary(k) and is_binary(v) ->
          acc + IO.iodata_length(k) + IO.iodata_length(v)

        acc ->
          acc
      end

    meta_kw =
      for {k, v} <- Keyword.get(options, :metadata) || %{},
          v != nil do
        {k, v}
      end

    attributes =
      [
        body_length: body_len,
        headers_length: headers_len,
        request_length: body_len + headers_len
      ] ++ meta_kw

    OpenTelemetry.Tracer.with_span :http_egress, %{attributes: attributes} do
    end

    str_meta =
      for {k, v} <- meta_kw, is_binary(k), into: %{} do
        {Utils.stringify(k), v}
      end

    :telemetry.execute(
      [:logflare, :backends, :ingest, :egress],
      %{request_bytes: body_len + headers_len},
      str_meta
    )

    with {:ok, env} <- Tesla.run(env, next) do
      {:ok, env}
    end
  end
end

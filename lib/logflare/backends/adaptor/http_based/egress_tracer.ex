defmodule Logflare.Backends.Adaptor.HttpBased.EgressTracer do
  @moduledoc """
  Tesla middleware tracing the egress.

  Should be used as last middleware called before the `Tesla.Adapter`
  """

  require OpenTelemetry.Tracer

  @behaviour Tesla.Middleware

  @impl true
  def call(env, next, _options) do
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
      for {k, v} <- Keyword.get(env.opts, :metadata) || %{},
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
      Tesla.run(env, next)
    end
  end
end

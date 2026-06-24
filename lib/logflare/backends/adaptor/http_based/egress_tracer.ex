defmodule Logflare.Backends.Adaptor.HttpBased.EgressTracer do
  @moduledoc """
  Tesla middleware tracing the egress.

  Should be used as last middleware called before the `Tesla.Adapter`
  """

  alias Logflare.Utils
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

    str_meta =
      for {k, v} <- meta_kw, is_binary(k), into: %{} do
        {Utils.stringify(k), v}
      end

    :telemetry.execute(
      [:logflare, :backends, :ingest, :egress],
      %{request_bytes: body_len + headers_len},
      str_meta
    )

    OpenTelemetry.Tracer.with_span :http_egress, %{attributes: attributes} do
      case Tesla.run(env, next) do
        {:ok, %Tesla.Env{status: status} = resp_env} = result ->
          OpenTelemetry.Tracer.set_attribute(:response_status_code, status)

          if response_length = response_length(resp_env) do
            OpenTelemetry.Tracer.set_attribute(:response_length, response_length)
          end

          if status >= 400 do
            OpenTelemetry.Tracer.set_status(:error, "HTTP #{status}")
          end

          result

        {:error, reason} = result ->
          OpenTelemetry.Tracer.set_attribute(:error, inspect(reason))
          OpenTelemetry.Tracer.set_status(:error, inspect(reason))
          result
      end
    end
  end

  defp response_length(%Tesla.Env{body: body}) when is_binary(body),
    do: IO.iodata_length(body)

  defp response_length(_env), do: nil
end

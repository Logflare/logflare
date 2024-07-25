defmodule Logflare.Backends.Adaptor.WebhookAdaptor.EgressMiddleware do
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

    :telemetry.execute(
      [:logflare, :backends, :egress],
      %{
        body_length: body_len,
        headers_length: headers_len,
        request_length: body_len + headers_len
      },
      options[:metadata]
    )

    with {:ok, env} <- Tesla.run(env, next) do
      {:ok, env}
    end
  end
end

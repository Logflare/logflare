defmodule Logflare.Backends.Adaptor.HttpBased.LogEventTransformer do
  @moduledoc """
  Middleware extracting body from `Logflare.LogEvent`s.

  Assumes that following middlewares (e.g. `Tesla.Middleware.JSON`) can handle conversion of maps to `t:iodata/0`.

  Accepts optional argument `:transform_fn` - a mapping function, transforming each LogEvent's body
  """
  alias Logflare.LogEvent
  @behaviour Tesla.Middleware

  @impl true
  def call(env, next, opts) do
    transform_fn = opts[:transform_fn]

    env
    |> Tesla.put_body(transform(env.body, transform_fn))
    |> Tesla.run(next)
  end

  defp transform([%LogEvent{} | _] = req_body, nil) do
    for %LogEvent{body: body} <- req_body, do: body
  end

  defp transform([%LogEvent{} | _] = req_body, transform_fn) when is_function(transform_fn) do
    for %LogEvent{body: body} <- req_body, do: transform_fn.(body)
  end

  defp transform(term, _transform_fn), do: term
end

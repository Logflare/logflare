defmodule Logflare.Backends.Adaptor.HttpBased.LogEventTransformer do
  @moduledoc """
  Middleware extracting body from `Logflare.LogEvent`s.

  Assumes that following middlewares (e.g. `Tesla.Middleware.JSON`) can handle conversion of maps to `t:iodata/0`.

  ## Options

  - `:map` - a mapping function, transforming the LogEvent's body
  """
  alias Logflare.LogEvent
  @behaviour Tesla.Middleware

  @impl true
  def call(env, next, opts) do
    mapper = opts[:map]

    env
    |> Tesla.put_body(transform(env.body, mapper))
    |> Tesla.run(next)
  end

  defp transform([%LogEvent{} | _] = req_body, nil) do
    for %LogEvent{body: body} <- req_body, do: body
  end

  defp transform([%LogEvent{} | _] = req_body, mapper) when is_function(mapper) do
    for %LogEvent{body: body} <- req_body, do: mapper.(body)
  end

  defp transform(term, _mapper), do: term
end

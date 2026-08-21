defmodule Logflare.Backends.Adaptor.SplunkAdaptor.HecFormatter do
  @moduledoc """
  A `Tesla.Middleware` encoding `Logflare.LogEvent`s as
  [Splunk HEC events](https://docs.splunk.com/Documentation/Splunk/latest/Data/FormateventsforHTTPEventCollector).

  Events are sent as a JSON array, which the HEC event endpoint accepts as a batch.
  Encoding is left to `Tesla.Middleware.JSON`.
  """

  alias Logflare.LogEvent

  @behaviour Tesla.Middleware

  @default_sourcetype "_json"

  @impl Tesla.Middleware
  def call(env, next, opts) do
    config = Keyword.fetch!(opts, :config)

    env
    |> Tesla.put_body(Enum.map(env.body, &build_event(&1, config)))
    |> Tesla.run(next)
  end

  @spec build_event(LogEvent.t(), map()) :: map()
  defp build_event(%LogEvent{body: body} = log_event, config) do
    %{
      "event" => body,
      "time" => body["timestamp"] / 1_000_000,
      "sourcetype" => Map.get(config, :sourcetype) || @default_sourcetype,
      "source" => Map.get(config, :source) || log_event.source_name,
      "index" => Map.get(config, :index),
      "host" => Map.get(config, :host)
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end
end

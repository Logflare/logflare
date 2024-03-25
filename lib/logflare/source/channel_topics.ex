defmodule Logflare.Source.ChannelTopics do
  @moduledoc """
  Broadcasts all source-related events to source-related topics
  """
  require Logger

  alias Logflare.LogEvent, as: LE
  alias Logflare.Source
  alias Number.Delimit

  use TypedStruct

  typedstruct do
    field :source_token, String.t(), enforce: true
    field :log_count, integer(), default: 0
    field :buffer, integer(), default: 0
    field :average_rate, integer(), default: 0
    field :rate, integer(), default: 0
    field :max_rate, integer(), default: 0
  end

  def subscribe_dashboard(source_token) do
    LogflareWeb.Endpoint.subscribe("dashboard:#{source_token}")
  end

  def subscribe_source(source_token) do
    LogflareWeb.Endpoint.subscribe("source:#{source_token}")
  end

  def broadcast_log_count(%{log_count: log_count, source_token: source_token} = payload) do
    payload = %{payload | log_count: Delimit.number_to_delimited(log_count)}
    topic = "dashboard:#{source_token}"
    event = "log_count"

    logflare_local_broadcast(topic, event, payload)
  end

  def broadcast_buffer(%{buffer: _buffer, source_token: source_token} = payload) do
    topic = "dashboard:#{source_token}"
    event = "buffer"

    logflare_local_broadcast(topic, event, payload)
  end

  def broadcast_rates(payload) do
    payload =
      payload
      |> Map.put(:rate, payload[:last_rate])

    topic = "dashboard:#{payload.source_token}"
    event = "rate"

    logflare_local_broadcast(topic, event, payload)
  end

  def broadcast_new(%LE{source: %Source{token: token}, body: body} = le) do
    maybe_broadcast("source:#{token}", "source:#{token}:new", %{
      body: body,
      via_rule: le.via_rule && Map.take(le.via_rule, [:regex]),
      origin_source_id: le.origin_source_id
    })
  end

  def maybe_broadcast(topic, event, payload) do
    case :ets.whereis(LogflareWeb.Endpoint) do
      :undefined ->
        Logger.error("Endpoint not up yet!")

      _ ->
        LogflareWeb.Endpoint.broadcast(
          topic,
          event,
          payload
        )
    end
  end

  def logflare_local_broadcast(topic, event, payload) do
    case :ets.whereis(LogflareWeb.Endpoint) do
      :undefined ->
        Logger.error("Endpoint not up yet!")

      _ ->
        LogflareWeb.Endpoint.local_broadcast(topic, event, payload)
    end
  end
end

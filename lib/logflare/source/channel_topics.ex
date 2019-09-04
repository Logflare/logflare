defmodule Logflare.Source.ChannelTopics do
  @moduledoc """
  Broadcasts all source-related events to source-related topics
  """
  require Logger

  alias __MODULE__, as: CT
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source
  alias Logflare.Source.Data
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

  def broadcast_log_count(payload) do
    payload = %{payload | log_count: Delimit.number_to_delimited(payload[:log_count])}

    topic = "dashboard:#{payload.source_token}"
    event = "log_count"
    payload = %Phoenix.Socket.Broadcast{event: event, payload: payload, topic: topic}

    Phoenix.PubSub.direct_broadcast(node(), Logflare.PubSub, topic, payload)
  end

  def broadcast_buffer(payload) do
    payload = %{payload | buffer: payload[:buffer]}

    maybe_broadcast(
      "dashboard:#{payload.source_token}",
      "buffer",
      payload
    )
  end

  def broadcast_rates(payload) do
    payload =
      %{
        payload
        | average_rate: payload[:average_rate],
          max_rate: payload[:max_rate]
      }
      |> Map.put(:rate, payload[:last_rate])

    LogflareWeb.Endpoint.broadcast(
      "dashboard:#{payload.source_token}",
      "rate",
      payload
    )
  end

  def broadcast_new(%LE{source: %Source{token: token}, body: body} = le) do
    maybe_broadcast("source:#{token}", "source:#{token}:new", %{
      body: body |> Map.from_struct(),
      via_rule: le.via_rule && Map.take(le.via_rule, [:regex]),
      origin_source_id: le.origin_source_id
    })
  end

  def maybe_broadcast(topic, event, payload) do
    case :ets.info(LogflareWeb.Endpoint) do
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
end

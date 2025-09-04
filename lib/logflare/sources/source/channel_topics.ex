defmodule Logflare.Sources.Source.ChannelTopics do
  @moduledoc """
  Broadcasts all source-related events to source-related topics
  """
  use TypedStruct

  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources.Source
  alias Logflare.Sources
  alias Number.Delimit

  require Logger

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

  def local_broadcast_log_count(%{log_count: log_count, source_token: source_token} = payload) do
    payload = %{payload | log_count: Delimit.number_to_delimited(log_count)}
    topic = "dashboard:#{source_token}"
    event = "log_count"

    maybe_local_broadcast(topic, event, payload)
  end

  @doc """
  Broadcasts the channel buffer locally
  """
  def local_broadcast_buffer(%{buffer: _buffer, source_id: source_id, backend_id: nil} = payload) do
    if source = Sources.Cache.get_by_id(source_id) do
      topic = "dashboard:#{source.token}"
      event = "buffer"

      maybe_local_broadcast(topic, event, payload)
    else
      :noop
    end
  end

  def local_broadcast_buffer(_), do: :noop

  def local_broadcast_rates(payload) do
    payload =
      payload
      |> Map.put(:rate, payload[:last_rate])

    topic = "dashboard:#{payload.source_token}"
    event = "rate"

    maybe_local_broadcast(topic, event, payload)
  end

  @doc """
  Broadcasts events to all nodes
  """
  def broadcast_new(events) when is_list(events), do: Enum.map(events, &broadcast_new/1)

  def broadcast_new(%LE{source: %Source{token: token}, body: body} = le) do
    maybe_broadcast("source:#{token}", "source:#{token}:new", %{
      body: body,
      via_rule: le.via_rule && Map.take(le.via_rule, [:regex]),
      origin_source_id: le.origin_source_id
    })
  end

  # performs a global broadcast
  @spec maybe_broadcast(String.t(), String.t(), map()) :: :ok | {:error, :endpoint_not_up}
  def maybe_broadcast(topic, event, payload) do
    case :ets.whereis(LogflareWeb.Endpoint) do
      :undefined ->
        {:error, :endpoint_not_up}

      _ ->
        LogflareWeb.Endpoint.broadcast(
          topic,
          event,
          payload
        )
    end
  end

  # performs a local broadcast
  @spec maybe_local_broadcast(String.t(), String.t(), map()) :: :ok | {:error, :endpoint_not_up}
  def maybe_local_broadcast(topic, event, payload) do
    case :ets.whereis(LogflareWeb.Endpoint) do
      :undefined ->
        {:error, :endpoint_not_up}

      _ ->
        LogflareWeb.Endpoint.local_broadcast(topic, event, payload)
    end
  end
end

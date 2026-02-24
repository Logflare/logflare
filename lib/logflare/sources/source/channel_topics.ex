defmodule Logflare.Sources.Source.ChannelTopics do
  @moduledoc """
  Broadcasts all source-related events to source-related topics
  """
  use TypedStruct

  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources

  require Logger

  typedstruct do
    field :source_token, String.t(), enforce: true
    field :log_count, integer(), default: 0
    field :buffer, integer(), default: 0
    field :average_rate, integer(), default: 0
    field :rate, integer(), default: 0
    field :max_rate, integer(), default: 0
  end

  @doc """
  Broadcasts events to all nodes
  """
  def broadcast_new(events) when is_list(events), do: Enum.map(events, &broadcast_new/1)

  def broadcast_new(%LE{source_id: source_id, body: body} = le) do
    source = Sources.Cache.get_by_id(source_id)

    maybe_broadcast("source:#{source.token}", "source:#{source.token}:new", %{
      body: body,
      via_rule_id: le.via_rule_id,
      source_uuid: le.source_uuid
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
end

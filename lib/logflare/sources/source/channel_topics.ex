defmodule Logflare.Sources.Source.ChannelTopics do
  @moduledoc """
  Broadcasts all source-related events to source-related topics
  """
  use TypedStruct

  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources.Source

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
end

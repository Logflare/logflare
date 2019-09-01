defmodule Logflare.Source.ChannelTopics do
  @moduledoc """
  Broadcasts all source-related events to source-related topics
  """
  require Logger
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source
  alias Logflare.Source.Data
  alias Number.Delimit

  @spec broadcast_log_count(Logflare.Source.t()) :: :ok | {:error, any}
  def broadcast_log_count(%Source{token: source_id}) do
    log_count = Data.get_total_inserts_cluster(source_id)

    LogflareWeb.Endpoint.broadcast(
      "dashboard:#{source_id}",
      "dashboard:#{source_id}:log_count",
      %{
        log_count: Delimit.number_to_delimited(log_count),
        source_token: "#{source_id}"
      }
    )
  end

  @spec broadcast_buffer(%{source_id: atom(), buffer: integer()}) :: :ok | {:error, any}
  def broadcast_buffer(%{source_id: source_id, buffer: count} = payload)
      when is_atom(source_id) do
    maybe_broadcast(
      "dashboard:#{source_id}",
      "dashboard:#{source_id}:buffer",
      %{
        source_token: "#{source_id}",
        buffer: count
      }
    )
  end

  @spec broadcast_rates(%{
          average_rate: number | Decimal.t(),
          last_rate: number | Decimal.t(),
          max_rate: number | Decimal.t(),
          source_id: atom()
        }) :: :ok | {:error, any}
  def broadcast_rates(%{source_id: source_id} = payload) do
    LogflareWeb.Endpoint.broadcast(
      "dashboard:#{source_id}",
      "dashboard:#{source_id}:rate",
      %{
        source_token: source_id,
        rate: payload.last_rate,
        average_rate: payload.average_rate,
        max_rate: payload.max_rate
      }
    )
  end

  @spec broadcast_new(Logflare.LogEvent.t()) :: :ok | {:error, any}
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

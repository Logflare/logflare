defmodule Logflare.Source.ChannelTopics do
  @moduledoc """
  Broadcasts all source-related events to source-related topics
  """
  require Logger
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source
  alias Number.Delimit
  alias Logflare.Sources.Counters

  def broadcast_log_count(%Source{token: source_id}) do
    {:ok, log_count} = Counters.get_total_inserts(source_id)
    source_table_string = Atom.to_string(source_id)

    payload = %{
      log_count: Delimit.number_to_delimited(log_count),
      source_token: source_table_string
    }

    LogflareWeb.Endpoint.broadcast(
      "dashboard:" <> source_table_string,
      "dashboard:#{source_table_string}:log_count",
      payload
    )
  end

  def broadcast_buffer(source_id, count) when is_atom(source_id) do
    source_id_string = Atom.to_string(source_id)

    maybe_broadcast(
      "dashboard:#{source_id_string}",
      "dashboard:#{source_id_string}:buffer",
      %{
        source_token: source_id_string,
        buffer: Delimit.number_to_delimited(count)
      }
    )
  end

  def broadcast_rates(payload) do
    payload = %{
      source_token: payload.source_token,
      rate: Delimit.number_to_delimited(payload.last_rate),
      average_rate: Delimit.number_to_delimited(payload.average_rate),
      max_rate: Delimit.number_to_delimited(payload.max_rate)
    }

    maybe_broadcast(
      "dashboard:#{payload.source_token}",
      "dashboard:#{payload.source_token}:rate",
      payload
    )
  end

  def broadcast_new(%LE{source: %Source{token: token}, body: body}) do
    maybe_broadcast("source:#{token}", "source:#{token}:new", %{
      log_message: body.message,
      timestamp: body.timestamp
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

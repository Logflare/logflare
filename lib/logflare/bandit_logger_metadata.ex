defmodule Logflare.BanditLoggerMetadata do
  @moduledoc """
  Correlates Bandit request and connection logs with a shared connection ID.
  """

  @events [
    [:thousand_island, :connection, :start],
    [:thousand_island, :connection, :stop],
    [:thousand_island, :connection, :exception],
    [:bandit, :request, :start]
  ]

  def attach do
    :telemetry.attach_many(__MODULE__, @events, &__MODULE__.handle_event/4, [])
  end

  def handle_event(event, _measurements, metadata, _config) do
    case {event, metadata} do
      {[:thousand_island, :connection, phase],
       %{handler: Bandit.DelegatingHandler, telemetry_span_context: ref}}
      when phase in [:start, :stop, :exception] and is_reference(ref) ->
        put_connection_id(ref)

      {[:bandit, :request, :start], %{connection_telemetry_span_context: ref}}
      when is_reference(ref) ->
        put_connection_id(ref)

      _event ->
        :ok
    end
  end

  defp put_connection_id(ref) do
    Logger.metadata(
      connection_id: ref |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)
    )
  end
end

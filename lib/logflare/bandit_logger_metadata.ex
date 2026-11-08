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

  def handle_event(
        [:thousand_island, :connection, event],
        _measurements,
        %{handler: Bandit.DelegatingHandler, telemetry_span_context: ref},
        _config
      )
      when event in [:start, :stop, :exception] and is_reference(ref) do
    put_connection_id(ref)
  end

  def handle_event(
        [:bandit, :request, :start],
        _measurements,
        %{connection_telemetry_span_context: ref},
        _config
      )
      when is_reference(ref) do
    put_connection_id(ref)
  end

  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  defp put_connection_id(ref) do
    Logger.metadata(
      connection_id: ref |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)
    )
  end
end

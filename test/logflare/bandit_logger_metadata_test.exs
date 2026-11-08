defmodule Logflare.BanditLoggerMetadataTest do
  use ExUnit.Case, async: true

  alias Logflare.BanditLoggerMetadata

  test "shares connection IDs across request and connection events and preserves request IDs" do
    ref = make_ref()
    Logger.metadata(request_id: "request-123")

    :ok =
      BanditLoggerMetadata.handle_event(
        [:bandit, :request, :start],
        %{},
        %{connection_telemetry_span_context: ref},
        []
      )

    connection_id = Logger.metadata()[:connection_id]
    assert is_binary(connection_id) and byte_size(connection_id) > 0

    for event <- [:start, :stop, :exception] do
      Logger.metadata(connection_id: nil)

      :ok =
        BanditLoggerMetadata.handle_event(
          [:thousand_island, :connection, event],
          %{},
          %{handler: Bandit.DelegatingHandler, telemetry_span_context: ref},
          []
        )

      assert Logger.metadata()[:connection_id] == connection_id
      assert Logger.metadata()[:request_id] == "request-123"
    end

    :ok =
      BanditLoggerMetadata.handle_event(
        [:thousand_island, :connection, :start],
        %{},
        %{handler: __MODULE__, telemetry_span_context: make_ref()},
        []
      )

    assert Logger.metadata()[:connection_id] == connection_id
  end
end

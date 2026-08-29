defmodule Logflare.BanditLoggerMetadataTest do
  use ExUnit.Case, async: true

  alias Logflare.BanditLoggerMetadata

  setup do
    Logger.reset_metadata(existing: "metadata")
    :ok = BanditLoggerMetadata.attach()
  end

  test "sets sanitized request metadata when a Bandit request starts" do
    conn =
      :post
      |> Plug.Test.conn(
        "https://api.logflare.app/api/endpoints/query/path-secret?token=query-secret",
        "body-secret"
      )

    :telemetry.execute([:bandit, :request, :start], %{}, %{conn: conn})

    metadata = Map.new(Logger.metadata())

    assert metadata.request == %{
             method: "POST",
             path: "/api/endpoints/query/:token_or_name"
           }

    assert metadata.existing == "metadata"

    sanitized = inspect(metadata)
    refute sanitized =~ "path-secret"
    refute sanitized =~ "query-secret"
    refute sanitized =~ "body-secret"
  end

  test "omits an unmatched raw path" do
    conn = Plug.Test.conn(:get, "/unmatched/path-secret?token=query-secret")

    :telemetry.execute([:bandit, :request, :start], %{}, %{conn: conn})

    assert Map.new(Logger.metadata()).request == %{method: "GET"}
  end

  test "sets connection metadata when a DelegatingHandler connection stops" do
    Logger.metadata(request: %{method: "GET", path: "/active"})

    :telemetry.execute(
      [:thousand_island, :connection, :stop],
      %{},
      %{
        handler: Bandit.DelegatingHandler,
        remote_address: {0x2001, 0xDB8, 0, 0, 0, 0, 0, 1},
        remote_port: 54_321,
        error: :econnreset
      }
    )

    metadata = Map.new(Logger.metadata())

    assert metadata.connection == %{
             error: "econnreset",
             remote_ip: "2001:db8::1",
             remote_port: 54_321
           }

    assert metadata.request == %{method: "GET", path: "/active"}
  end

  test "ignores telemetry events without usable HTTP metadata" do
    :telemetry.execute([:bandit, :request, :start], %{}, %{error: "bad headers"})

    :telemetry.execute(
      [:thousand_island, :connection, :stop],
      %{},
      %{
        handler: SomeOtherHandler,
        remote_address: {192, 0, 2, 1},
        remote_port: 1234
      }
    )

    assert Map.new(Logger.metadata()) == %{existing: "metadata"}
  end
end

defmodule LogflareWeb.ElixirLoggerControllerTest do
  @moduledoc false
  import Logflare.DummyFactory
  alias Logflare.{Repo, TableCounter, SystemCounter, TableBuffer, TableManager, SourceRateCounter}
  use LogflareWeb.ConnCase

  @moduletag integration: true
  @endpoint LogflareWeb.ElixirLoggerController

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
    s = insert(:source, token: Faker.UUID.v4(), rules: [])
    u = insert(:user, sources: [s])

    TableCounter.start_link()
    SystemCounter.start_link()
    TableManager.start_link([s.token])

    Process.sleep(3_000)

    {:ok, users: [u], sources: [s], conn: Phoenix.ConnTest.build_conn()}
  end

  describe "ElixirLoggerController" do
    test "create action", %{conn: conn, users: [u], sources: [s]} do
      conn =
        conn
        |> assign(:user, u)
        |> assign(:source, s)
        |> post(:create, %{
          "batch" => [
            %{
              "message" => "message",
              "metadata" => %{},
              "timestamp" => NaiveDateTime.to_iso8601(NaiveDateTime.utc_now()),
              "level" => "info"
            },
            %{
              "message" => "message",
              "metadata" => %{},
              "timestamp" => NaiveDateTime.to_iso8601(NaiveDateTime.utc_now()),
              "level" => "info"
            },
            %{
              "message" => "message",
              "metadata" => %{},
              "timestamp" => NaiveDateTime.to_iso8601(NaiveDateTime.utc_now()),
              "level" => "info"
            }
          ]
        })

      assert TableBuffer.get_count(s.token) == 3
      Process.sleep(1000)
      # sleep needed by SourceRateCounter to send itself
      # a message to get and calculate metrics
      %{average: avg, duration: duration, sum: sum} = SourceRateCounter.get_metrics(s.token)
      assert avg in [1, 2]
      assert duration == 60
      assert sum == 3
      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end
  end
end

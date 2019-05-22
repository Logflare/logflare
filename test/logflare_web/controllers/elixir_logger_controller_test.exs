defmodule LogflareWeb.ElixirLoggerControllerTest do
  @moduledoc false
  import Logflare.DummyFactory
  alias Logflare.{Repo, TableCounter, SystemCounter, TableBuffer, TableManager, SourceRateCounter}
  use LogflareWeb.ConnCase

  @endpoint LogflareWeb.ElixirLoggerController

  describe "ElixirLoggerController integration" do
    @describetag integration: true

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

    test "create action", %{conn: conn, users: [u], sources: [s]} do
      log_event = build_log_event()

      conn =
        conn
        |> assign(:user, u)
        |> assign(:source, s)
        |> post(:create, %{"batch" => [log_event, log_event, log_event]})

      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end
  end

  describe "ElixirLoggerController" do
    setup do
      :ok = Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
      s = insert(:source, token: Faker.UUID.v4(), rules: [])
      u = insert(:user, sources: [s])

      {:ok, users: [u], sources: [s], conn: Phoenix.ConnTest.build_conn()}
    end

    test "create action", %{conn: conn, users: [u], sources: [s]} do
      log_event = build_log_event()

      conn =
        conn
        |> assign(:user, u)
        |> assign(:source, s)
        |> post(:create, %{"batch" => [log_event, log_event, log_event]})

      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end
  end

  def build_log_event() do
      %{
        "message" => "log message",
        "metadata" => %{},
        "timestamp" => NaiveDateTime.to_iso8601(NaiveDateTime.utc_now()),
        "level" => "info"
      }
  end
end

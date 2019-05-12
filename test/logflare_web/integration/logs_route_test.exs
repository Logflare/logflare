defmodule LogflareWeb.LogsRouteTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.TableBuffer
  alias Logflare.TableManager
  alias Logflare.{TableBuffer, TableManager, SourceRateCounter}
  import Logflare.DummyFactory

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})
    s = insert(:source, token: Faker.UUID.v4())
    u = insert(:user, api_key: Faker.String.base64(), sources: [s])

    Logflare.TableCounter.start_link()
    Logflare.SystemCounter.start_link()
    TableManager.start_link([s.token])

    # Sleep to make sure that table manager has time to init all GenServers
    Process.sleep(3_000)
    {:ok, user: u, sources: [s]}
  end

  describe "POST /logs" do
    test "fails without api key", %{conn: conn, user: user} do
      conn = post(conn, "/logs", %{"log_entry" => %{}})
      assert json_response(conn, 403) == %{"message" => "Unknown x-api-key."}
    end

    test "fails without source or source_name", %{conn: conn, user: user} do
      conn =
        conn
        |> put_api_key_header(user.api_key)
        |> post("/logs", %{"log_entry" => %{}})

      assert json_response(conn, 403) == %{"message" => "Source or source_name needed."}
    end

    test "fails with an empty-ish log_entry", %{conn: conn, user: u, sources: [s]} do
      conn1 = post_logs(conn, u, s, %{})

      assert json_response(conn1, 403) == %{"message" => "Log entry needed."}

      conn2 = post_logs(conn, u, s, nil)

      assert json_response(conn2, 403) == %{"message" => "Log entry needed."}

      conn3 = post_logs(conn, u, s, [])

      assert json_response(conn3, 403) == %{"message" => "Log entry needed."}
    end

    test "succeeds with log entry and no metadata ", %{conn: conn, user: u, sources: [s]} do
      conn = post_logs(conn, u, s, "log binary message")

      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end

    test "succeeds with log entry and metadata ", %{conn: conn, user: u, sources: [s]} do
      conn = post_logs(conn, u, s, "log binary message", metadata)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
    end
    test "succeeds and puts 10 log entries in the TableBuffer", %{
      conn: conn,
      user: u,
      sources: [s]
    } do
      post_with_index = &post_logs(conn, u, s, "log binary message #{&1}")
      Enum.each(1..10, post_with_index)
      assert TableBuffer.get_count(s.token) == 10
      Process.sleep(1_000)
      # sleep needed by SourceRateCounter to send itself
      # a message to get and calculate metrics
      assert SourceRateCounter.get_metrics(s.token) == %{average: 5, duration: 60, sum: 10}
    end
    end
  end

  defp post_logs(conn, user, source, log_entry, metadata \\ nil) do
    conn
    |> put_api_key_header(user.api_key)
    |> post("/logs", %{
      "log_entry" => log_entry,
      "source" => Atom.to_string(source.token),
      "metadata" => metadata
    })
  end

  defp put_api_key_header(conn, api_key) do
    conn
    |> put_req_header("x-api-key", api_key)
  end

  defp metadata() do
    %{
      "datacenter" => "aws",
      "ip_address" => "100.100.100.100",
      "request_headers" => %{
        "connection" => "close",
        "servers" => %{
          "blah" => "water",
          "home" => "not home",
          "deep_nest" => [
            %{"more_deep_nest" => %{"a" => 1}},
            %{"more_deep_nest2" => %{"a" => 2}}
          ]
        },
        "user_agent" => "chrome"
      },
      "request_method" => "POST"
    }

  end
end

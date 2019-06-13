defmodule LogflareWeb.Plugs.VerifyApiRequestTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.LogEvent
  alias Logflare.{Source, User, Sources}
  alias Logflare.Sources.Counters
  alias Logflare.SystemCounter
  alias LogflareWeb.Plugs.VerifyApiRequest
  import Logflare.DummyFactory
  alias Logflare.Source.RateCounterServer
  use Placebo

  setup do
    allow(Source.Data.get_rate()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_latest_date()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_avg_rate()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_max_rate()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_buffer()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_total_inserts()) |> exec(fn _ -> 0 end)
    allow(Source.Data.get_log_count()) |> exec(fn _, _ -> 0 end)

    allow SystemCounter.log_count(any()), return: {:ok, 0}
    allow SystemCounter.incriment(any()), return: :ok

    Sources.Counters.start_link()

    u1 = insert(:user, api_key: "dummy_key", api_quota: 5)
    u2 = insert(:user, api_key: "other_dummy_key", api_quota: 0)
    s1 = insert(:source, user_id: u1.id)
    s2 = insert(:source, user_id: u2.id)
    s1 = Sources.get_by(id: s1.id)
    s2 = Sources.get_by(id: s2.id)
    {:ok, _} = RateCounterServer.start_link(s1.token)
    {:ok, _} = RateCounterServer.start_link(s2.token)
    {:ok, users: [u1, u2], sources: [s1, s2]}
  end

  describe "Plugs.VerifyApiRequest.validate_log_entries" do
    test "halts conn if invalid", %{users: [u | _], sources: [s | _]} do
      conn =
        build_conn(:post, "/logs")
        |> assign(:user, u)
        |> assign(:source, %{s | user: u})
        |> assign(:log_params_batch, [
          %{
            metadata: %{
              "users" => [
                %{
                  "id" => 1
                },
                %{
                  "id" => "2"
                }
              ]
            },
            message: true
          }
        ])
        |> VerifyApiRequest.validate_log_events()

      assert conn.halted == true
    end

    test "doesn't halt conn if valid", %{users: [u | _], sources: [s | _]} do
      conn =
        build_conn(:post, "/logs")
        |> assign(:user, u)
        |> assign(:source, s)
        |> assign(:log_params_batch, [
          %{
              "metadata" => %{
                "users" => [
                  %{
                    "id" => 1
                  },
                  %{
                    "id" => 2
                  }
                ]
              },
              "message" => "valid"
          }
        ])
        |> VerifyApiRequest.validate_log_events()

      assert conn.halted == false
      assert_called SystemCounter.incriment(any()), once()
      assert_called SystemCounter.log_count(any()), once()
    end
  end

  describe "Plugs.VerifyApiRequest.check_log_entry" do
    test "halts on nil or empty log_entry or batch", %{
      conn: conn,
      users: [u1, u2],
      sources: [s1 | _]
    } do
      conn1 =
        conn
        |> assign(:user, u1)
        |> assign(:source, s1)
        |> assign(:log_params_batch, [%{"metadata" => %{}, "key" => 0}])
        |> VerifyApiRequest.validate_log_events()

      IO.inspect(conn1)
      assert conn1.status === 406

      conn2 =
        conn
        |> assign(:user, u1)
        |> assign(:source, s1)
        |> assign(:log_params_batch, [%{"log_entry" => ""}])
        |> VerifyApiRequest.validate_log_events()

      assert conn2.status == 406

      conn3 =
        conn
        |> assign(:user, u1)
        |> assign(:source, s1)
        |> assign(:log_params_batch, [])
        |> VerifyApiRequest.validate_log_events()

      assert conn3.status == 406
      assert conn3.assigns.message === "Log entry needed."
    end

    test "doesn't halt with log_entry present ", %{conn: conn, users: [u1, u2]} do
      conn =
        conn
        |> assign(:params, %{"log_entry" => "string log entry"})

      refute conn.halted
    end

    test "doesn't halt with batch param present", %{conn: conn, users: [u1, u2]} do
      conn =
        conn
        |> assign(:params, %{"batch" => [%{"log_entry" => "string log entry"}]})

      refute conn.halted
    end
  end
end

defmodule LogflareWeb.Plugs.VerifyApiRequestTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.{User, Sources}
  alias LogflareWeb.Plugs.VerifyApiRequest
  import Logflare.DummyFactory
  alias Logflare.Source.RateCounterServer

  setup do
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
        |> assign(:log_events, [
          %{
            "metadata" => %{
              "users" => [
                %{
                  "id" => 1
                },
                %{
                  "id" => "2"
                }
              ]
            },
            "log_entry" => true
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
        |> assign(:log_events, [
          %{
            "users" => [
              %{
                "id" => 1
              },
              %{
                "id" => 2
              }
            ]
          }
        ])
        |> VerifyApiRequest.validate_log_events()

      assert conn.halted == false
    end
  end

  describe "Plugs.VerifyApiRequest.check_log_entry" do
    test "halts on nil or empty log_entry or batch", %{conn: conn, users: [u1, u2]} do
      conn1 =
        conn
        |> assign(:params, %{"metadata" => %{}, "key" => 0})
        |> fetch_query_params()
        |> VerifyApiRequest.check_log_entry()

      assert conn1.halted
      assert conn1.status == 406
      assert conn1.assigns.message === "Log entry needed."

      conn2 =
        conn
        |> assign(:params, %{"log_entry" => ""})
        |> fetch_query_params()
        |> VerifyApiRequest.check_log_entry()

      assert conn2.halted
      assert conn2.status == 406
      assert conn2.assigns.message === "Log entry needed."

      conn3 =
        conn
        |> assign(:params, %{"batch" => []})
        |> fetch_query_params()
        |> VerifyApiRequest.check_log_entry()

      assert conn3.halted
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

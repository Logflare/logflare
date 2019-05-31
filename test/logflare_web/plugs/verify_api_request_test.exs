defmodule LogflareWeb.Plugs.VerifyApiRequestTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.User
  alias LogflareWeb.Plugs.VerifyApiRequest
  import Logflare.DummyFactory

  setup do
    s1 = insert(:source)
    s2 = insert(:source)
    u1 = insert(:user, sources: [s1], api_quota: 5)
    u2 = insert(:user, sources: [s2], api_quota: 0)
    {:ok, _} = Logflare.SourceRateCounter.start_link(s1.token)
    {:ok, _} = Logflare.SourceRateCounter.start_link(s2.token)
    {:ok, users: [u1, u2], sources: [s1, s2]}
  end

  describe "check log entry" do
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

defmodule LogflareWeb.Plugs.VerifyApiRequestTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.User
  alias LogflareWeb.Plugs.VerifyApiRequest

  setup do
    s1 = insert(:source)
    s2 = insert(:source)
    u1 = insert(:user, sources: [s1], api_key: "dummy_key", api_quota: 5)
    u2 = insert(:user, sources: [s2], api_key: "other_dummy_key", api_quota: 0)
    {:ok, _} = Logflare.SourceRateCounter.start_link(s1.token)
    {:ok, _} = Logflare.SourceRateCounter.start_link(s2.token)
    {:ok, users: [u1, u2], sources: [s1, s2]}
  end

  describe "check user" do
    test "doesn't halt with assigned user", %{conn: conn, users: [u1, u2]} do
      conn = conn
        |> assign(conn, user: u1)
        |> VerifyApiRequest.check_user()

        refute conn.halted
    end

    test "halts with no assigned user", %{conn: conn, users: [u1, u2]} do
      conn = conn
        |> VerifyApiRequest.check_user()

      assert conn.halted
      assert conn.status == 401
      assert conn.assigns.message == "Error: please set API token"
    end
  end

end

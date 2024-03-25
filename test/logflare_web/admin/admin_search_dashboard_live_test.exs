defmodule LogflareWeb.AdminSearchDashboardLiveTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Users
  @endpoint LogflareWeb.Endpoint
  # alias Logflare.BigQuery.PredefinedTestUser
  alias Logflare.Source.RecentLogsServer, as: RLS
  @test_token :"2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"
  @moduletag :this

  setup_all do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Logflare.Repo)

    {:ok, _} =
      RLS.start_link(%RLS{source_id: @test_token, source: Sources.get_by(token: @test_token)})

    :ok
  end

  describe "mount" do
    setup [:assign_user_source]

    @tag :failing
    test "successfully for admin", %{conn: conn, user: [user | _], source: [_source | _]} do
      assert {:ok, _view, html} =
               conn
               |> assign(:user, %{user | admin: true})
               |> live("/admin/search")

      assert html =~ "Search Dashboard"
    end

    @tag :failing
    test "forbidden for non-admin", %{conn: conn, user: [_user | _], source: [_source | _]} do
      conn =
        conn
        |> get("/admin/search")

      assert html_response(conn, 403) =~ "403"
    end

    @tag :failing
    test "forbidden for anonynous user", %{conn: conn, user: [_user | _], source: [_source | _]} do
      conn =
        conn
        |> assign(:user, nil)
        |> get("/admin/search")

      assert html_response(conn, 403) =~ "403"
    end
  end

  defp assign_user_source(_context) do
    user = Users.get_by_and_preload(email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))

    # SourceSchemas.Cache.put_bq_schema(@test_token, PredefinedTestUser.table_schema())
    source = Sources.get_by(token: @test_token)

    conn =
      build_conn()
      |> assign(:user, user)

    %{source: [source], user: [user], conn: conn}
  end
end

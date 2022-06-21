defmodule LogflareWeb.EndpointControllerTest do
  @moduledoc false
  import Logflare.Factory
  use LogflareWeb.ConnCase

  @session Plug.Session.init(
             store: :cookie,
             key: "_app",
             encryption_salt: "yadayada",
             signing_salt: "yadayada"
           )

  describe "create" do
    @tag :failing
    test "Endpoint for User", %{conn: conn} do
      _plan = insert(:plan, name: "Free", type: "standard")
      user = insert(:user)
      _source = insert(:source, user: user)
      _billing_account = insert(:billing_account, user: user)
      user = user |> Logflare.Repo.preload(:billing_account)

      params = %{
        "name" => "current date",
        "query" => "select current_date() as date"
      }

      conn =
        conn |> assign(:user, user) |> post(Routes.endpoint_path(conn, :create), query: params)

      assert %{id: id} = redirected_params(conn)
      assert redirected_to(conn) == Routes.endpoint_path(conn, :show, id)

      conn = get(conn, Routes.endpoint_path(conn, :show, id))
      assert html_response(conn, 200) =~ "/endpoints/"
    end
  end

  describe "query" do
    @tag :failing
    test "Query Endpoint", %{conn: conn} do
      # This fails to start because :erlexec fails to start
      # Adding Application.ensure_all_started(:erlexec) to test_helper.exs and it stall there instead
      Logflare.SQL.start_link([])

      _plan = insert(:plan, name: "Free", type: "standard")
      user = insert(:user)
      _source = insert(:source, user: user)
      _billing_account = insert(:billing_account, user: user)
      _user = user |> Logflare.Repo.preload(:billing_account)
      endpoint = insert(:endpoint, name: "current date", query: "select current_date() as date")

      conn = get(conn, Routes.endpoint_path(conn, :query, endpoint.token))

      assert [%{"date" => "2022-06-21"}] = json_response(conn, 200)["result"]
    end
  end

  describe "index" do
    test "Endpoints index", %{conn: conn} do
      _plan = insert(:plan, name: "Free", type: "standard")
      user = insert(:user)
      _source = insert(:source, user: user)
      _billing_account = insert(:billing_account, user: user)
      user = user |> Logflare.Repo.preload(:billing_account)

      conn =
        conn
        |> Map.put(:secret_key_base, String.duplicate("abcdefgh", 8))
        |> Plug.Session.call(@session)
        |> fetch_session()

      conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)

      conn = get(conn, Routes.endpoint_path(conn, :index))

      assert html_response(conn, 200) =~ "/endpoints"
    end
  end
end

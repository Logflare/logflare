defmodule LogflareWeb.Source.RulesLqlTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  @endpoint LogflareWeb.Endpoint
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Users
  import Logflare.Factory
  alias Logflare.Billing
  alias Logflare.Billing.Plan
  use Mimic

  setup_all do
    start_supervised!(Sources.Counters)
    :ok
  end

  describe "Mount" do
    setup %{conn: conn} do
      user = insert(:user)
      source = insert(:source, user: user)
      plan = insert(:plan)
      conn = login_user(conn, user)

      {:ok, conn: conn, user: user, source: source, plan: plan}
    end

    test "subheader - lql docs", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/rules")
      html = render(view)
      assert html =~ "Rules"
      assert html =~ "Sink source"
      assert html =~ "No rules yet..."
    end
  end

  describe "LQL rules" do
    setup :set_mimic_global

    setup do
      stub(Billing, :get_plan_by_user, fn _ -> %Plan{limit_source_fields_limit: 500} end)
      user = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_2"))
      user = Users.get(user.id)

      source = params_for(:source)
      {:ok, source} = Sources.create_source(source, user)
      # SourceSchemas.Cache.put_bq_schema(source.token, SchemaBuilder.initial_table_schema())

      {:ok, sink} =
        :source
        |> params_for(name: "Sink Source 1")
        |> Sources.create_source(user)

      rls = %RLS{source_id: source.token, source: source}

      {:ok, _pid} = RLS.start_link(rls)

      Process.sleep(100)
      %{sources: [source, sink], user: [user]}
    end

    @tag :failing
    test "mount with source owner user", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u.id})
        |> assign(:user, u)
        |> get("/sources/#{s.id}/rules")

      assert {:ok, view, html} = live(conn)
      assert html =~ ~S|id="lql-rules-container"|

      assert render_submit(view, :fsubmit, %{
               "rule" => %{"lql_string" => "123errorstring", "sink" => sink.token}
             }) =~ "Sink Source 1"

      html =
        render_submit(view, :fsubmit, %{
          "rule" => %{"lql_string" => "123infostring", "sink" => sink.token}
        })

      assert html =~ "123infostring"
      assert html =~ "123errorstring"
    end

    @tag :failing
    test "add and delete rules", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
      {:ok, view, _html} =
        conn
        |> assign(:user, u)
        |> Plug.Test.init_test_session(%{user_id: u.id})
        |> get("/sources/#{s.id}/rules")
        |> live()

      assert render_submit(view, :fsubmit, %{
               "rule" => %{"lql_string" => "123errorstring", "sink" => sink.token}
             }) =~ "Sink Source 1"

      html =
        render_submit(view, :fsubmit, %{
          "rule" => %{"lql_string" => "123infostring", "sink" => sink.token}
        })

      assert html =~ "123infostring"
      assert html =~ "123errorstring"

      source = Sources.get_by_and_preload(token: s.token)
      assert length(source.rules) == 2

      rule_id =
        source.rules
        |> Enum.find(&(&1.lql_string == "123infostring"))
        |> Map.get(:id)
        |> to_string()

      html = render_click(view, "delete_rule", %{"rule_id" => rule_id})

      refute html =~ rule_id
      source = Sources.get_by_and_preload(token: s.token)
      assert length(source.rules) == 1
    end

    @tag :failing
    test "mount with non-existing source", %{conn: conn, sources: [s, _sink | _], user: [u | _]} do
      conn =
        conn
        |> Plug.Test.init_test_session(%{user_id: u.id})
        |> assign(:user, u)
        |> get("/sources/#{s.id + 1000}/rules")

      assert html_response(conn, 404) =~ "404"
    end
  end
end

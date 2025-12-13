defmodule LogflareWeb.Sources.RulesLiveTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Rules

  setup do
    insert(:plan)
    :ok
  end

  test "redirects unauthenticated users to login", %{conn: conn} do
    source = insert(:source, user: build(:user))

    conn
    |> visit(~p"/sources/#{source}/rules")
    |> assert_path(~p"/auth/login")
  end

  test "returns 403 for source that doesn't belong to user", %{conn: conn} do
    user = insert(:user)
    other_user = insert(:user)
    source = insert(:source, user: other_user)

    conn =
      conn
      |> login_user(user)
      |> get(~p"/sources/#{source}/rules")

    assert html_response(conn, 404) =~ "not found"
  end

  describe "Authenticated User" do
    setup %{conn: conn} do
      user = insert(:user)
      source = insert(:source, user: user)
      sink_source = insert(:source, user: user, name: "error-sink")

      insert(:source_schema,
        source: source,
        bigquery_schema:
          TestUtils.build_bq_schema(%{
            "level" => "string"
          })
      )

      %{
        conn: login_user(conn, user),
        user: user,
        source: source,
        sink_source: sink_source
      }
    end

    test "displays empty rules list initially", %{conn: conn, source: source} do
      conn
      |> visit(~p"/sources/#{source}/rules")
      |> assert_has(".subhead h5 a", text: source.name)
      |> assert_has("a[href='/sources/#{source.id}']", text: source.name)
      |> assert_has("h5", text: "Source Routing Rules")
      |> assert_has("li.list-group-item", text: "No rules yet...")
    end

    test "displays rules after creating them", %{
      conn: conn,
      source: source,
      sink_source: sink_source
    } do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source}/rules")

      assert view
             |> element("button[type=submit]", "Add Rule")
             |> has_element?()

      assert view
             |> form("form", %{
               "rule" => %{
                 "lql_string" => "level:error",
                 "sink" => sink_source.token
               }
             })
             |> render_submit()

      assert view |> element("div[role='alert']") |> render() =~
               "LQL source routing rule created successfully!"

      assert view
             |> element("li.list-group-item")
             |> render() =~ "Matching LQL <code>level:error</code>"
    end

    test "displays errors when creating rules with invalid data", %{
      conn: conn,
      source: source
    } do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source}/rules")

      assert view
             |> element("button[type=submit]", "Add Rule")
             |> has_element?()

      assert view
             |> form("form", %{
               "rule" => %{
                 "lql_string" => ""
               }
             })
             |> render_submit()

      assert view |> element("div[role='alert']") |> render() =~ "lql_string: can&#39;t be blank"

      assert view
             |> element("li.list-group-item")
             |> render() =~ "<li class=\"list-group-item\"><div>No rules yet...</div></li>"
    end

    test "can delete multiple rules", %{conn: conn, source: source, sink_source: sink_source} do
      rule1 = insert(:rule, lql_string: "testing", sink: sink_source.token, source_id: source.id)
      rule2 = insert(:rule, lql_string: "error", sink: sink_source.token, source_id: source.id)

      {:ok, view, _html} = live(conn, ~p"/sources/#{source}/rules")

      assert render(view) =~ "Matching LQL <code>testing</code>"
      assert render(view) =~ "Matching LQL <code>error</code>"

      assert view
             |> element("li.list-group-item a[phx-value-rule_id='#{rule1.id}']")
             |> render_click()

      refute render(view) =~ "Matching LQL <code>testing</code>"

      assert view
             |> element("li.list-group-item a[phx-value-rule_id='#{rule2.id}']")
             |> render_click()

      refute render(view) =~ "Matching LQL <code>error</code>"

      refute Rules.get_rule(rule1.id)
      refute Rules.get_rule(rule2.id)
    end
  end
end

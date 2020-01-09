defmodule LogflareWeb.Source.RulesLqlTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import Phoenix.LiveViewTest
  alias Logflare.Sources
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Users
  @endpoint LogflareWeb.Endpoint
  import Logflare.Factory
  use Placebo

  setup do
    user = insert(:user)
    source = insert(:source, user: user)
    source = Sources.get(source.id)
    sink = insert(:source, user: user, name: "Sink Source 1")
    sink = Sources.get(sink.id)
    user = Users.get(user.id)
    schema = SchemaBuilder.initial_table_schema()
    {:ok, true} = Sources.Cache.put_bq_schema(source.token, schema)
    %{sources: [source, sink], user: [user]}
  end

  test "mount", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
    conn =
      conn
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

  test "add and delete rules", %{conn: conn, sources: [s, sink | _], user: [u | _]} do
    {:ok, view, html} =
      conn
      |> assign(:user, u)
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
    html = render_click(view, "delete_rule", %{"rule_id" => to_string(hd(source.rules).id)})

    refute html =~ "123infostring"
    source = Sources.get_by_and_preload(token: s.token)
    assert length(source.rules) == 1
  end
end

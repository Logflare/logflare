defmodule LogflareWeb.SourceEnrichmentRulesLiveTest do
  use LogflareWeb.ConnCase

  import Phoenix.LiveViewTest

  alias Logflare.EnrichmentRules
  alias LogflareWeb.SourceEnrichmentRulesLive

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user_id: user.id)

    [source: source, user: user]
  end

  defp render_view(conn, source) do
    {:ok, view, html} =
      live_isolated(conn, SourceEnrichmentRulesLive, session: %{"source_id" => source.id})

    {view, html}
  end

  test "creates a rule with typed field values", %{conn: conn, source: source} do
    {view, html} = render_view(conn, source)
    assert html =~ "No enrichment rules yet."

    html =
      view
      |> element("#enrichment-rule-form")
      |> render_submit(%{
        rule: %{
          match: "any",
          conditions: "m.level in error, fatal\nm.status greater_than 499\nm.user_id exists",
          set_fields: "alert=true\nm.retries=3\nteam=core"
        }
      })

    assert html =~ "Enrichment rule created."
    assert html =~ "m.level in [&quot;error&quot;,&quot;fatal&quot;] OR m.status greater_than 499"

    assert [rule] = EnrichmentRules.list_by_source_id(source.id)
    assert rule.match == :any

    assert rule.conditions == [
             %{"path" => "m.level", "predicate" => "in", "value" => ["error", "fatal"]},
             %{"path" => "m.status", "predicate" => "greater_than", "value" => 499},
             %{"path" => "m.user_id", "predicate" => "exists"}
           ]

    assert rule.set_fields == %{"alert" => true, "m.retries" => 3, "team" => "core"}
  end

  test "parses not_in, case-insensitive predicates and derived fields", %{
    conn: conn,
    source: source
  } do
    {view, _html} = render_view(conn, source)

    html =
      view
      |> element("#enrichment-rule-form")
      |> render_submit(%{
        rule: %{
          conditions: "m.level not_in/i error, fatal\n@severity_text not_in ERROR, FATAL",
          set_fields: "excluded=true"
        }
      })

    assert html =~ "Enrichment rule created."
    assert html =~ "m.level not_in/i"

    assert [rule] = EnrichmentRules.list_by_source_id(source.id)

    assert rule.conditions == [
             %{
               "path" => "m.level",
               "predicate" => "not_in",
               "value" => ["error", "fatal"],
               "case_insensitive" => true
             },
             %{"path" => "@severity_text", "predicate" => "not_in", "value" => ["ERROR", "FATAL"]}
           ]
  end

  test "shows errors for malformed set fields and invalid rules", %{conn: conn, source: source} do
    {view, _html} = render_view(conn, source)
    valid = "m.level equals error"

    assert view
           |> element("#enrichment-rule-form")
           |> render_submit(%{rule: %{conditions: "m.level", set_fields: "a=1"}}) =~
             "one field predicate value per line"

    assert view
           |> element("#enrichment-rule-form")
           |> render_submit(%{rule: %{conditions: "m.level matches x", set_fields: "a=1"}}) =~
             "unknown predicate: matches"

    assert view
           |> element("#enrichment-rule-form")
           |> render_submit(%{rule: %{conditions: valid, set_fields: "no-equals-sign"}}) =~
             "one field=value per line"

    assert view
           |> element("#enrichment-rule-form")
           |> render_submit(%{rule: %{conditions: valid, set_fields: "timestamp=1"}}) =~
             "cannot set reserved field"

    assert EnrichmentRules.list_by_source_id(source.id) == []
  end

  test "deletes a rule of its own source only", %{conn: conn, source: source, user: user} do
    rule = insert(:enrichment_rule, source: source)
    other_rule = insert(:enrichment_rule, source: insert(:source, user: user))
    {view, html} = render_view(conn, source)
    assert html =~ "enrichment-rule-#{rule.id}"

    assert render_click(view, "delete", %{"id" => to_string(other_rule.id)}) =~
             "Enrichment rule not found on this source."

    assert EnrichmentRules.get(other_rule.id)

    view
    |> element("#enrichment-rule-#{rule.id} button")
    |> render_click()

    assert EnrichmentRules.get(rule.id) == nil
    refute render(view) =~ "enrichment-rule-#{rule.id}"
  end
end

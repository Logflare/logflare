defmodule Logflare.AlertIssues.IssueTypesTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues.IssueTypes
  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)

    for {rule_key, name, managed} <- [
          {"supabase.advisors.security", "Security advisor", true},
          {"logflare.edge-fn-5xx", "Edge function errors", false}
        ] do
      {:ok, _} =
        Alerting.create_alert_query(user, %{
          name: name,
          cron: "*/5 * * * *",
          query: "select 1",
          language: :pg_sql,
          rule_key: rule_key,
          managed: managed,
          category: :security
        })
    end

    {:ok, _} =
      Alerting.create_alert_query(user, %{
        name: "Plain alert",
        cron: "*/5 * * * *",
        query: "select 1",
        language: :pg_sql
      })

    %{user: user}
  end

  test "lists rule-keyed alerts with their silence state for the identifiers", %{user: user} do
    {:ok, silence} =
      Silences.create(user, %{
        "rule_key" => "logflare.edge-fn-5xx",
        "matchers" => %{"project" => "p1"}
      })

    types = IssueTypes.list(user, %{"project" => "p1"})

    assert Enum.map(types, & &1.rule_key) == [
             "logflare.edge-fn-5xx",
             "supabase.advisors.security"
           ]

    by_key = Map.new(types, &{&1.rule_key, &1})
    assert by_key["logflare.edge-fn-5xx"].silenced
    assert by_key["logflare.edge-fn-5xx"].silence_id == silence.id
    refute by_key["supabase.advisors.security"].silenced
    assert by_key["supabase.advisors.security"].managed
    assert by_key["supabase.advisors.security"].lql == nil

    refute Enum.any?(IssueTypes.list(user, %{"project" => "p2"}), & &1.silenced)
  end
end

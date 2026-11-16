defmodule Logflare.AlertIssues.IssueTypesTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues.IssueTypes
  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends.Adaptor.PostgresAdaptor

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

  describe "custom LQL alert issue types" do
    setup %{user: user} do
      cfg = Application.get_env(:logflare, Logflare.Repo)
      url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"
      source = insert(:source, user: user, name: "function_edge_logs")

      backend =
        insert(:backend, type: :postgres, config: %{url: url}, sources: [source], user: user)

      PostgresAdaptor.create_repo(backend)
      PostgresAdaptor.create_events_table({source, backend})
      on_exit(fn -> PostgresAdaptor.destroy_instance({source, backend}) end)
      :ok
    end

    @custom %{
      "name" => "Stripe webhook timeouts",
      "lql" => ~s(f:function_edge_logs m.status:>=500 "timeout"),
      "severity" => "major",
      "category" => "performance",
      "group_by" => ["metadata.function_id"],
      "identifiers" => %{"project" => "p1"}
    }

    test "creates a scheduled LQL rule with a grouped preview", %{user: user} do
      assert {:ok, issue_type} = IssueTypes.create_custom(user, @custom)

      assert %{
               rule_key: "custom.stripe-webhook-timeouts",
               managed: false,
               language: :lql,
               severity: :major,
               silenced: false
             } = issue_type

      alert = Alerting.get_alert_query_by(user_id: user.id, rule_key: issue_type.rule_key)
      assert %AlertQuery{cron: "* * * * *", kind: :scheduled, scope: %{"project" => "p1"}} = alert

      assert alert.signal_mapping["preview"]["lql"] ==
               ~s(f:function_edge_logs m.status:>=500 "timeout" m.function_id:{{function_id}})

      assert [_ | _] = Alerting.list_future_jobs(alert.id)

      assert {:ok, %{rule_key: "custom.stripe-webhook-timeouts-2"}} =
               IssueTypes.create_custom(user, @custom)
    end

    test "rejects invalid LQL, unknown sources, bad group_by and missing identifiers", %{
      user: user
    } do
      for {params, field} <- [
            {%{"lql" => "m.status:500"}, :lql},
            {%{"lql" => "f:nope m.status:500"}, :lql},
            {%{"group_by" => ["metadata.bad path"]}, :group_by},
            {%{"identifiers" => %{}}, :identifiers},
            {%{"name" => nil}, :name}
          ] do
        assert {:error, changeset} = IssueTypes.create_custom(user, Map.merge(@custom, params))
        assert Map.has_key?(errors_on(changeset), field)
      end
    end

    test "stores notebook templates on custom rules", %{user: user} do
      notebook = [%{"title" => "Errors", "source" => "logs", "sql" => "select 1"}]

      {:ok, %{rule_key: rule_key}} =
        IssueTypes.create_custom(user, Map.put(@custom, "notebook", notebook))

      assert Alerting.get_alert_query_by(user_id: user.id, rule_key: rule_key).signal_mapping[
               "notebook"
             ] == notebook

      assert {:error, changeset} =
               IssueTypes.create_custom(user, Map.put(@custom, "notebook", [%{"title" => "x"}]))

      assert %{signal_mapping: [_]} = errors_on(changeset)
    end

    test "deletes custom rules but not managed ones", %{user: user} do
      {:ok, %{rule_key: rule_key}} = IssueTypes.create_custom(user, @custom)

      assert :ok = IssueTypes.delete_custom(user, rule_key)
      assert {:error, :not_found} = IssueTypes.delete_custom(user, rule_key)
      assert {:error, :managed} = IssueTypes.delete_custom(user, "supabase.advisors.security")
    end
  end
end

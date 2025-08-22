defmodule Logflare.RulesTest do
  use Logflare.DataCase

  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.Sources
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Logs.SourceRouting
  alias Logflare.SystemMetrics.AllLogsLogged
  alias GoogleApi.BigQuery.V2.Model.{TableSchema, TableFieldSchema}

  test "list_rules" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, sources: [source], user: user)
    assert [] = Rules.list_rules(source)
    assert [] = Rules.list_rules(backend)

    insert(:rule, source: source, backend: backend)
    assert [_] = Rules.list_rules(source)
    assert [_] = Rules.list_rules(backend)
  end

  test "create_rule/1 from attrs" do
    user = insert(:user)
    source = insert(:source, user: user)

    assert {:ok,
            %Rule{
              lql_string: "a:testing",
              lql_filters: [_]
            } = rule} =
             Rules.create_rule(%{
               source_id: source.id,
               lql_string: "a:testing"
             })

    assert {:ok, _rule} = Rules.delete_rule(rule)

    refute Rules.get_rule(rule.id)
  end

  test "create_rule/1 with backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, sources: [source], user: user)

    assert {:ok,
            %Rule{
              id: rule_id,
              lql_string: "a:testing",
              lql_filters: [_]
            } = rule} =
             Rules.create_rule(%{
               source_id: source.id,
               backend_id: backend.id,
               lql_string: "a:testing"
             })

    assert {:ok, %Rule{id: ^rule_id}} = Rules.fetch_rule_by(user_id: user.id)

    # delete the rule
    assert {:ok, _rule} = Rules.delete_rule(rule)

    refute Rules.get_rule(rule.id)

    assert {:error, :not_found} = Rules.fetch_rule_by(user_id: user.id)
  end

  describe "create_rule/2" do
    test "creates a rule with valid LQL string and source" do
      user = insert(:user)
      source = insert(:source, user: user)

      insert(:source_schema,
        source: source,
        bigquery_schema: %TableSchema{
          fields: [
            %TableFieldSchema{name: "message", type: "STRING"},
            %TableFieldSchema{name: "timestamp", type: "TIMESTAMP"},
            %TableFieldSchema{name: "level", type: "STRING"}
          ]
        }
      )

      params = %{
        "lql_string" => "level:error"
      }

      assert {:ok, %Rule{} = rule} = Rules.create_rule(params, source)
      assert rule.lql_string == "level:error"
      assert rule.source_id == source.id
      assert rule.token != nil
      assert is_list(rule.lql_filters)
      assert length(rule.lql_filters) >= 1
    end

    test "creates a rule with backend_id when provided" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)

      insert(:source_schema,
        source: source,
        bigquery_schema: %TableSchema{
          fields: [
            %TableFieldSchema{name: "message", type: "STRING"},
            %TableFieldSchema{name: "timestamp", type: "TIMESTAMP"},
            %TableFieldSchema{name: "level", type: "STRING"}
          ]
        }
      )

      params = %{
        "lql_string" => "level:info",
        "backend_id" => backend.id
      }

      assert {:ok, %Rule{} = rule} = Rules.create_rule(params, source)
      assert rule.lql_string == "level:info"
      assert rule.source_id == source.id
      assert rule.backend_id == backend.id
    end

    test "returns error when LQL string is invalid due to field not found" do
      user = insert(:user)
      source = insert(:source, user: user)

      insert(:source_schema,
        source: source,
        bigquery_schema: %TableSchema{
          fields: [
            %TableFieldSchema{name: "message", type: "STRING"},
            %TableFieldSchema{name: "timestamp", type: "TIMESTAMP"},
            %TableFieldSchema{name: "level", type: "STRING"}
          ]
        }
      )

      params = %{
        "lql_string" => "nonexistent_field:error"
      }

      assert {:error, :field_not_found, _suggested_query, _error_message} =
               Rules.create_rule(params, source)
    end

    test "returns error when lql_string is empty" do
      user = insert(:user)
      source = insert(:source, user: user)

      insert(:source_schema,
        source: source,
        bigquery_schema:
          TestUtils.build_bq_schema(%{
            "test" => %{"nested" => 123, "listical" => ["testing", "123"]}
          })
      )

      params = %{
        "lql_string" => ""
      }

      assert {:error, %Ecto.Changeset{} = changeset} = Rules.create_rule(params, source)
      assert changeset.valid? == false
      assert errors_on(changeset).lql_string == ["can't be blank"]
    end
  end

  test "update_rule" do
    user = insert(:user)
    source = insert(:source, user: user)
    rule = insert(:rule, source: source)

    assert {:ok, %Rule{lql_string: "a:1 b:2 c:3", lql_filters: [_, _, _]}} =
             Rules.update_rule(rule, %{lql_string: "a:1 b:2 c:3"})

    assert {:error, %Ecto.Changeset{} = changeset} =
             Rules.update_rule(rule, %{lql_string: nil})

    assert changeset.valid? == false
    assert errors_on(changeset).lql_string == ["can't be blank"]
  end

  describe "SourceSup management" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source, user: user}
    end

    test "on rule creation with a backend, update SourceSup of the related source if SourceSup is started",
         %{source: source, user: user} do
      backend = insert(:backend, user: user)
      start_supervised!({SourceSup, source})
      # create the rule
      via = Backends.via_source(source, SourceSup)
      prev_length = Supervisor.which_children(via) |> length()

      assert {:ok, _} =
               Rules.create_rule(%{
                 source_id: source.id,
                 backend_id: backend.id,
                 lql_string: "a:testing"
               })

      assert Supervisor.which_children(via) |> length() > prev_length
    end

    test "on rule deletion with a backend, update SourceSup of the related source", %{
      source: source,
      user: user
    } do
      backend = insert(:backend, user: user)
      [rule1, rule2] = insert_pair(:rule, backend: backend, source: source)
      start_supervised!({SourceSup, source})
      # create the rule
      via = Backends.via_source(source, SourceSup)
      prev_length = Supervisor.which_children(via) |> length()
      assert {:ok, _} = Rules.delete_rule(rule1)
      assert {:ok, _} = Rules.delete_rule(rule2)
      assert Supervisor.which_children(via) |> length() < prev_length
    end

    test "multiple rules on same source, same backend, does not crash SourceSup", %{
      source: source,
      user: user
    } do
      backend = insert(:backend, user: user)
      insert_pair(:rule, source: source, backend: backend)
      start_supervised!({SourceSup, source})
    end

    test "v2_pipeline=true when source is routed, should ensure that backend is started on the SourceSup",
         %{source: source, user: user} do
      start_supervised!(AllLogsLogged)

      backend = insert(:backend, user: user)
      start_supervised!({SourceSup, source})
      # create the rule
      via = Backends.via_source(source, SourceSup)
      prev_length = Supervisor.which_children(via) |> length()

      insert(:rule, source: source, backend: backend, lql_string: "testing")

      source = Sources.get_by_and_preload(id: source.id)
      # should not be started yet
      assert Supervisor.which_children(via) |> length() == prev_length

      # route the source
      le = build(:log_event, source: source, message: "testing123")
      SourceRouting.route_to_sinks_and_ingest(le)

      :timer.sleep(200)
      assert Supervisor.which_children(via) |> length() > prev_length
    end
  end
end

defmodule Logflare.RulesTest do
  use Logflare.DataCase
  alias Logflare.Rules
  alias Logflare.Rule
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup

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

  test "create_rule from attrs" do
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

    # delete the rule
    assert {:ok, _rule} = Rules.delete_rule(rule)
  end

  test "create_rule with backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, sources: [source], user: user)

    assert {:ok,
            %Rule{
              lql_string: "a:testing",
              lql_filters: [_]
            } = rule} =
             Rules.create_rule(%{
               source_id: source.id,
               backend_id: backend.id,
               lql_string: "a:testing"
             })

    # delete the rule
    assert {:ok, _rule} = Rules.delete_rule(rule)
  end

  test "update_rule" do
    user = insert(:user)
    source = insert(:source, user: user)
    rule = insert(:rule, source: source)

    assert {:ok, %Rule{lql_string: "a:1 b:2 c:3", lql_filters: [_, _, _]}} =
             Rules.update_rule(rule, %{lql_string: "a:1 b:2 c:3"})
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

    test "multuple rules on same source, same backend, does not crash SourceSup", %{
      source: source,
      user: user
    } do
      backend = insert(:backend, user: user)
      insert_pair(:rule, source: source, backend: backend)
      start_supervised!({SourceSup, source})
    end
  end
end

defmodule Logflare.RulesTest do
  use Logflare.DataCase
  alias Logflare.Rules
  alias Logflare.Rule

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
end

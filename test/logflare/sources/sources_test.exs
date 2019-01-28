defmodule Logflare.SourcesTest do
  use Logflare.DataCase

  alias Logflare.Sources

  describe "rules" do
    alias Logflare.Sources.Rule

    @valid_attrs %{regex: "some regex"}
    @update_attrs %{regex: "some updated regex"}
    @invalid_attrs %{regex: nil}

    def rule_fixture(attrs \\ %{}) do
      {:ok, rule} =
        attrs
        |> Enum.into(@valid_attrs)
        |> Sources.create_rule()

      rule
    end

    test "list_rules/0 returns all rules" do
      rule = rule_fixture()
      assert Sources.list_rules() == [rule]
    end

    test "get_rule!/1 returns the rule with given id" do
      rule = rule_fixture()
      assert Sources.get_rule!(rule.id) == rule
    end

    test "create_rule/1 with valid data creates a rule" do
      assert {:ok, %Rule{} = rule} = Sources.create_rule(@valid_attrs)
      assert rule.regex == "some regex"
    end

    test "create_rule/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Sources.create_rule(@invalid_attrs)
    end

    test "update_rule/2 with valid data updates the rule" do
      rule = rule_fixture()
      assert {:ok, %Rule{} = rule} = Sources.update_rule(rule, @update_attrs)
      assert rule.regex == "some updated regex"
    end

    test "update_rule/2 with invalid data returns error changeset" do
      rule = rule_fixture()
      assert {:error, %Ecto.Changeset{}} = Sources.update_rule(rule, @invalid_attrs)
      assert rule == Sources.get_rule!(rule.id)
    end

    test "delete_rule/1 deletes the rule" do
      rule = rule_fixture()
      assert {:ok, %Rule{}} = Sources.delete_rule(rule)
      assert_raise Ecto.NoResultsError, fn -> Sources.get_rule!(rule.id) end
    end

    test "change_rule/1 returns a rule changeset" do
      rule = rule_fixture()
      assert %Ecto.Changeset{} = Sources.change_rule(rule)
    end
  end
end

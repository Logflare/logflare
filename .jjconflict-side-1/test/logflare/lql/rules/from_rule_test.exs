defmodule Logflare.Lql.Rules.FromRuleTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.Rules.FromRule

  describe "__struct__" do
    test "creates struct with default values" do
      from_rule = %FromRule{}

      assert from_rule.table == nil
      assert from_rule.table_type == :unknown
    end

    test "creates struct with custom values" do
      from_rule = %FromRule{
        table: "my_table",
        table_type: :cte
      }

      assert from_rule.table == "my_table"
      assert from_rule.table_type == :cte
    end

    test "creates struct with source table type" do
      from_rule = %FromRule{
        table: "source_token",
        table_type: :source
      }

      assert from_rule.table == "source_token"
      assert from_rule.table_type == :source
    end
  end

  describe "new/2" do
    test "creates from rule with table name" do
      from_rule = FromRule.new("my_table")

      assert %FromRule{table: "my_table", table_type: :unknown} = from_rule
    end

    test "creates from rule with table type" do
      from_rule = FromRule.new("my_cte", :cte)

      assert %FromRule{table: "my_cte", table_type: :cte} = from_rule
    end

    test "creates from rule with source type" do
      from_rule = FromRule.new("my_source", :source)

      assert %FromRule{table: "my_source", table_type: :source} = from_rule
    end
  end

  describe "changeset/2" do
    test "creates changeset from existing FromRule struct" do
      from_rule = %FromRule{
        table: "my_table",
        table_type: :cte
      }

      changeset = FromRule.changeset(%FromRule{}, from_rule)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "creates changeset from params map" do
      params = %{
        table: "errors",
        table_type: :cte
      }

      changeset = FromRule.changeset(%FromRule{}, params)

      assert %Ecto.Changeset{} = changeset
      assert changeset.valid?
    end

    test "validates required table field" do
      changeset = FromRule.changeset(%FromRule{}, %{})

      refute changeset.valid?
      assert changeset.errors[:table]
    end

    test "validates table identifier format - rejects invalid characters" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "invalid-name"})

      refute changeset.valid?
      assert changeset.errors[:table]
    end

    test "validates table identifier format - rejects starting with number" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "123invalid"})

      refute changeset.valid?
      assert changeset.errors[:table]
    end

    test "validates table identifier format - rejects spaces" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "invalid name"})

      refute changeset.valid?
      assert changeset.errors[:table]
    end

    test "validates table identifier format - rejects special characters" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "invalid@table"})

      refute changeset.valid?
      assert changeset.errors[:table]
    end

    test "accepts valid identifier with letters only" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "validtable"})

      assert changeset.valid?
    end

    test "accepts valid identifier with underscore" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "valid_table_name"})

      assert changeset.valid?
    end

    test "accepts valid identifier with numbers" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "valid_table_123"})

      assert changeset.valid?
    end

    test "accepts valid identifier starting with underscore" do
      changeset = FromRule.changeset(%FromRule{}, %{table: "_valid_table"})

      assert changeset.valid?
    end

    test "validates table length - minimum 1 character" do
      changeset = FromRule.changeset(%FromRule{}, %{table: ""})

      refute changeset.valid?
      assert changeset.errors[:table]
    end

    test "validates table length - maximum 255 characters" do
      long_name = String.duplicate("a", 256)
      changeset = FromRule.changeset(%FromRule{}, %{table: long_name})

      refute changeset.valid?
      assert changeset.errors[:table]
    end

    test "accepts table with exactly 255 characters" do
      max_name = String.duplicate("a", 255)
      changeset = FromRule.changeset(%FromRule{}, %{table: max_name})

      assert changeset.valid?
    end
  end

  describe "build/1" do
    test "builds valid from rule from params list" do
      from_rule = FromRule.build(table: "my_table", table_type: :cte)

      assert %FromRule{table: "my_table", table_type: :cte} = from_rule
    end

    test "returns empty struct for invalid params" do
      from_rule = FromRule.build(table: "invalid-name")

      assert %FromRule{table: nil} = from_rule
    end

    test "builds with default table_type when not specified" do
      from_rule = FromRule.build(table: "my_table")

      assert %FromRule{table: "my_table", table_type: :unknown} = from_rule
    end
  end

  describe "get_table/1" do
    test "returns table name from from rule" do
      from_rule = %FromRule{table: "errors", table_type: :cte}

      assert FromRule.get_table(from_rule) == "errors"
    end
  end

  describe "get_table_type/1" do
    test "returns table type from from rule" do
      from_rule = %FromRule{table: "errors", table_type: :cte}

      assert FromRule.get_table_type(from_rule) == :cte
    end

    test "returns :unknown for default table type" do
      from_rule = %FromRule{table: "my_table"}

      assert FromRule.get_table_type(from_rule) == :unknown
    end
  end

  describe "virtual_fields/0" do
    test "returns list of virtual fields" do
      fields = FromRule.virtual_fields()

      assert :table in fields
      assert :table_type in fields
      assert length(fields) == 2
    end
  end
end

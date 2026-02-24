defmodule Logflare.BigQuery.SchemaTypesTest do
  use ExUnit.Case, async: true

  alias Logflare.BigQuery.SchemaTypes

  describe "bq_type_to_ex/1" do
    test "handles standard BigQuery types" do
      assert SchemaTypes.bq_type_to_ex("STRING") == :string
      assert SchemaTypes.bq_type_to_ex("INTEGER") == :integer
      assert SchemaTypes.bq_type_to_ex("FLOAT") == :float
      assert SchemaTypes.bq_type_to_ex("BOOL") == :boolean
      assert SchemaTypes.bq_type_to_ex("BOOLEAN") == :boolean
      assert SchemaTypes.bq_type_to_ex("TIMESTAMP") == :datetime
      assert SchemaTypes.bq_type_to_ex("RECORD") == :map
      assert SchemaTypes.bq_type_to_ex("ARRAY") == :list
    end

    test "handles ARRAY<TYPE> string format" do
      assert SchemaTypes.bq_type_to_ex("ARRAY<STRING>") == {:list, :string}
      assert SchemaTypes.bq_type_to_ex("ARRAY<INTEGER>") == {:list, :integer}
      assert SchemaTypes.bq_type_to_ex("ARRAY<FLOAT>") == {:list, :float}
    end

    test "handles legacy tuple format - single level" do
      assert SchemaTypes.bq_type_to_ex({"ARRAY", "STRING"}) == {:list, :string}
      assert SchemaTypes.bq_type_to_ex({"ARRAY", "INTEGER"}) == {:list, :integer}
      assert SchemaTypes.bq_type_to_ex({"ARRAY", "FLOAT"}) == {:list, :float}
      assert SchemaTypes.bq_type_to_ex({"ARRAY", "BOOL"}) == {:list, :boolean}
      assert SchemaTypes.bq_type_to_ex({"ARRAY", "RECORD"}) == {:list, :map}
      assert SchemaTypes.bq_type_to_ex({"ARRAY", "TIMESTAMP"}) == {:list, :datetime}
    end

    test "handles legacy tuple format - nested arrays" do
      assert SchemaTypes.bq_type_to_ex({"ARRAY", {"ARRAY", "STRING"}}) ==
               {:list, {:list, :string}}

      assert SchemaTypes.bq_type_to_ex({"ARRAY", {"ARRAY", "INTEGER"}}) ==
               {:list, {:list, :integer}}
    end

    test "handles legacy tuple format - deeply nested arrays" do
      assert SchemaTypes.bq_type_to_ex({"ARRAY", {"ARRAY", {"ARRAY", "STRING"}}}) ==
               {:list, {:list, {:list, :string}}}

      assert SchemaTypes.bq_type_to_ex({"ARRAY", {"ARRAY", {"ARRAY", {"ARRAY", "INTEGER"}}}}) ==
               {:list, {:list, {:list, {:list, :integer}}}}
    end

    test "handles legacy tuple format - nested arrays with records" do
      assert SchemaTypes.bq_type_to_ex({"ARRAY", {"ARRAY", "RECORD"}}) == {:list, {:list, :map}}
    end
  end

  describe "to_schema_type/1" do
    test "returns tuples for arrays" do
      assert SchemaTypes.to_schema_type(["foo"]) == {"ARRAY", "STRING"}
      assert SchemaTypes.to_schema_type([1]) == {"ARRAY", "INTEGER"}
      assert SchemaTypes.to_schema_type([1.0]) == {"ARRAY", "FLOAT"}
      assert SchemaTypes.to_schema_type([%{}]) == {"ARRAY", "RECORD"}
    end

    test "returns nested tuples for nested arrays" do
      assert SchemaTypes.to_schema_type([["foo"]]) == {"ARRAY", {"ARRAY", "STRING"}}
      assert SchemaTypes.to_schema_type([[[1]]]) == {"ARRAY", {"ARRAY", {"ARRAY", "INTEGER"}}}
    end
  end
end

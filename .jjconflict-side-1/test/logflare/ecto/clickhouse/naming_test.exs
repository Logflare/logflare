defmodule Logflare.Ecto.ClickHouse.NamingTest do
  use ExUnit.Case, async: true

  alias Ecto.SubQuery
  alias Logflare.Ecto.ClickHouse.Naming

  describe "create_names/2" do
    test "creates names for single source" do
      sources = {{"logs", nil, nil}}
      result = Naming.create_names(sources, [])

      assert tuple_size(result) == 1
      {table, name, schema} = elem(result, 0)
      assert IO.iodata_to_binary(table) == ~s("logs")
      assert IO.iodata_to_binary(name) == "l0"
      assert schema == nil
    end

    test "creates names for multiple sources" do
      sources = {{"logs", nil, nil}, {"users", nil, nil}}
      result = Naming.create_names(sources, [])

      assert tuple_size(result) == 2
      {table1, name1, _} = elem(result, 0)
      {table2, name2, _} = elem(result, 1)

      assert IO.iodata_to_binary(table1) == ~s("logs")
      assert IO.iodata_to_binary(name1) == "l0"
      assert IO.iodata_to_binary(table2) == ~s("users")
      assert IO.iodata_to_binary(name2) == "u1"
    end

    test "creates names with schema" do
      sources = {{"logs", SomeSchema, nil}}
      result = Naming.create_names(sources, [])

      {_table, _name, schema} = elem(result, 0)
      assert schema == SomeSchema
    end

    test "creates names with prefix" do
      sources = {{"logs", nil, "public"}}
      result = Naming.create_names(sources, [])

      {table, _name, _schema} = elem(result, 0)
      assert IO.iodata_to_binary(table) == ~s("public"."logs")
    end

    test "creates names with as_prefix" do
      sources = {{"logs", nil, nil}}
      result = Naming.create_names(sources, [?s])

      {_table, name, _schema} = elem(result, 0)
      assert IO.iodata_to_binary(name) == "sl0"
    end
  end

  describe "create_name/3" do
    test "creates name for fragment source" do
      sources = {{:fragment, [], ["SELECT * FROM logs"]}}
      {table, name, schema} = Naming.create_name(sources, 0, [])

      assert table == nil
      assert IO.iodata_to_binary(name) == "f0"
      assert schema == nil
    end

    test "creates name for values source" do
      sources = {{:values, [], []}}
      {table, name, schema} = Naming.create_name(sources, 0, [])

      assert table == nil
      assert IO.iodata_to_binary(name) == "v0"
      assert schema == nil
    end

    test "creates name for regular table source" do
      sources = {{"logs", nil, nil}}
      {table, name, schema} = Naming.create_name(sources, 0, [])

      assert IO.iodata_to_binary(table) == ~s("logs")
      assert IO.iodata_to_binary(name) == "l0"
      assert schema == nil
    end

    test "creates name for SubQuery source" do
      subquery = %SubQuery{query: %Ecto.Query{}}
      sources = {subquery}
      {table, name, schema} = Naming.create_name(sources, 0, [])

      assert table == nil
      assert IO.iodata_to_binary(name) == "s0"
      assert schema == nil
    end
  end

  describe "create_alias/1" do
    test "creates alias from lowercase first letter" do
      assert Naming.create_alias("logs") == "l"
    end

    test "creates alias from uppercase first letter" do
      assert Naming.create_alias("Users") == "U"
    end

    test "creates default alias for non-letter" do
      assert Naming.create_alias("123") == ?t
      assert Naming.create_alias("_table") == ?t
    end

    test "creates default alias for empty string" do
      assert Naming.create_alias("") == ?t
    end
  end

  describe "subquery_as_prefix/1" do
    test "creates prefix for new subquery" do
      sources = {{"logs", nil, nil}}
      result = Naming.subquery_as_prefix(sources)
      assert result == [?s]
    end

    test "appends to existing prefix" do
      sources = {{"logs", nil, nil}, [?s, ?x]}
      result = Naming.subquery_as_prefix(sources)
      assert result == [?s, ?s, ?x]
    end
  end

  describe "quote_name/2" do
    test "quotes simple name" do
      result = Naming.quote_name("field")
      assert IO.iodata_to_binary(result) == ~s("field")
    end

    test "quotes atom name" do
      result = Naming.quote_name(:timestamp)
      assert IO.iodata_to_binary(result) == ~s("timestamp")
    end

    test "returns empty for nil" do
      result = Naming.quote_name(nil)
      assert result == []
    end

    test "quotes list of names with dots" do
      result = Naming.quote_name(["schema", "table", "field"])
      assert IO.iodata_to_binary(result) == ~s("schema.table.field")
    end

    test "filters nil values from list" do
      result = Naming.quote_name(["table", nil, "field"])
      assert IO.iodata_to_binary(result) == ~s("table.field")
    end

    test "accepts custom quoter" do
      result = Naming.quote_name("field", ?`)
      assert IO.iodata_to_binary(result) == "`field`"
    end

    test "accepts nil quoter for no quotes" do
      result = Naming.quote_name("field", nil)
      assert result == "field"
    end
  end

  describe "quote_qualified_name/3" do
    test "quotes field with source prefix" do
      sources = {{nil, "t0", nil}}
      result = Naming.quote_qualified_name(:field, sources, 0)
      assert IO.iodata_to_binary(result) == ~s(t0."field")
    end

    test "quotes field without source prefix when nil" do
      sources = {{nil, nil, nil}}
      result = Naming.quote_qualified_name(:field, sources, 0)
      assert IO.iodata_to_binary(result) == ~s("field")
    end
  end

  describe "field_access/3" do
    test "generates field access for atom field" do
      sources = {{nil, "t0", nil}}
      result = Naming.field_access(:event_message, sources, 0)
      assert IO.iodata_to_binary(result) == ~s(t0."event_message")
    end

    test "generates field access for string field" do
      sources = {{nil, "t0", nil}}
      result = Naming.field_access("dynamic_field", sources, 0)
      assert IO.iodata_to_binary(result) == ~s(t0."dynamic_field")
    end

    test "handles field without source" do
      sources = {{nil, nil, nil}}
      result = Naming.field_access(:field, sources, 0)
      assert IO.iodata_to_binary(result) == ~s("field")
    end
  end

  describe "quote_table/2" do
    test "quotes table name without prefix" do
      result = Naming.quote_table(nil, "logs")
      assert IO.iodata_to_binary(result) == ~s("logs")
    end

    test "quotes table name with prefix" do
      result = Naming.quote_table("public", "logs")
      assert IO.iodata_to_binary(result) == ~s("public"."logs")
    end
  end

  describe "escape_string/1" do
    test "escapes single quotes" do
      result = Naming.escape_string("it's")
      assert result == "it''s"
    end

    test "escapes backslashes" do
      result = Naming.escape_string("path\\to\\file")
      assert result == "path\\\\to\\\\file"
    end

    test "escapes both quotes and backslashes" do
      result = Naming.escape_string("it's a \\path")
      assert result == "it''s a \\\\path"
    end

    test "returns unchanged string without special chars" do
      result = Naming.escape_string("hello world")
      assert result == "hello world"
    end
  end

  describe "escape_json_key/1" do
    test "escapes single quotes" do
      result = Naming.escape_json_key("it's")
      assert result == "it''s"
    end

    test "escapes backslashes" do
      result = Naming.escape_json_key("path\\key")
      assert result == "path\\\\key"
    end

    test "escapes double quotes" do
      result = Naming.escape_json_key("key\"name")

      # escapes both single quotes and backslashes first, then double quotes
      assert result == "key\\\"name"
    end

    test "escapes all special characters" do
      result = Naming.escape_json_key(~s(it's "key"\\path))

      # escapes in order - single quotes, backslashes, then double quotes
      assert result == ~s(it''s \\"key\\"\\\\path)
    end

    test "returns unchanged string without special chars" do
      result = Naming.escape_json_key("simple_key")
      assert result == "simple_key"
    end
  end
end

defmodule Logflare.Ecto.ClickHouse.ParamsTest do
  use ExUnit.Case, async: true

  alias Logflare.Ecto.ClickHouse.Params

  describe "build_param/2" do
    test "builds parameter placeholder for string" do
      result = Params.build_param(0, "test")
      assert IO.iodata_to_binary(result) == "{$0:String}"
    end

    test "builds parameter placeholder for integer" do
      result = Params.build_param(1, 42)
      assert IO.iodata_to_binary(result) == "{$1:Int64}"
    end

    test "builds parameter placeholder for boolean" do
      result = Params.build_param(2, true)
      assert IO.iodata_to_binary(result) == "{$2:Bool}"
    end

    test "builds parameter placeholder for DateTime" do
      dt = ~U[2024-01-01 12:00:00.123456Z]
      result = Params.build_param(3, dt)
      assert IO.iodata_to_binary(result) == "{$3:DateTime64(6)}"
    end

    test "builds parameter placeholder for Date" do
      date = ~D[2024-01-01]
      result = Params.build_param(4, date)
      assert IO.iodata_to_binary(result) == "{$4:Date}"
    end

    test "builds parameter placeholder for array" do
      result = Params.build_param(5, ["a", "b"])
      assert IO.iodata_to_binary(result) == "{$5:Array(String)}"
    end
  end

  describe "build_params/3" do
    test "builds multiple parameter placeholders" do
      params = ["test", 42, true]
      result = Params.build_params(0, 3, params)
      sql = IO.iodata_to_binary(result)

      assert sql == "{$0:String},{$1:Int64},{$2:Bool}"
    end

    test "builds single parameter" do
      params = [123]
      result = Params.build_params(0, 1, params)
      sql = IO.iodata_to_binary(result)

      assert sql == "{$0:Int64}"
    end

    test "returns empty list for zero length" do
      result = Params.build_params(0, 0, [])
      assert result == []
    end
  end

  describe "inline_param/1" do
    test "converts nil to NULL" do
      assert Params.inline_param(nil) == "NULL"
    end

    test "converts true to true" do
      assert Params.inline_param(true) == "true"
    end

    test "converts false to false" do
      assert Params.inline_param(false) == "false"
    end

    test "converts string with quotes" do
      result = Params.inline_param("hello")
      assert IO.iodata_to_binary(result) == "'hello'"
    end

    test "escapes single quotes in strings" do
      result = Params.inline_param("it's")
      assert IO.iodata_to_binary(result) == "'it''s'"
    end

    test "escapes backslashes in strings" do
      result = Params.inline_param("path\\to\\file")
      assert IO.iodata_to_binary(result) == "'path\\\\to\\\\file'"
    end

    test "converts regular integer" do
      assert Params.inline_param(42) == "42"
    end

    test "converts large integer with type annotation" do
      # Max UInt64 + 1
      large_int = 0xFFFFFFFFFFFFFFFF + 1
      result = Params.inline_param(large_int)
      assert String.contains?(result, "::UInt128")
    end

    test "converts small negative integer with type annotation" do
      # Min Int64 - 1
      small_int = -0x8000000000000001
      result = Params.inline_param(small_int)
      assert String.contains?(result, "::Int128")
    end

    test "converts float" do
      assert Params.inline_param(3.14) == "3.14"
    end

    test "converts NaiveDateTime without microseconds" do
      naive = ~N[2024-01-01 12:00:00]
      result = Params.inline_param(naive)
      assert IO.iodata_to_binary(result) == "'2024-01-01 12:00:00'::datetime"
    end

    test "converts NaiveDateTime with microseconds" do
      naive = ~N[2024-01-01 12:00:00.123456]
      result = Params.inline_param(naive)
      sql = IO.iodata_to_binary(result)
      assert String.starts_with?(sql, "'2024-01-01 12:00:00.123456'::DateTime64(6)")
    end

    test "converts DateTime without microseconds" do
      dt = ~U[2024-01-01 12:00:00Z]
      result = Params.inline_param(dt)
      sql = IO.iodata_to_binary(result)
      assert String.contains?(sql, "DateTime('Etc/UTC')")
    end

    test "converts DateTime with microseconds and timezone" do
      dt = ~U[2024-01-01 12:00:00.123456Z]
      result = Params.inline_param(dt)
      sql = IO.iodata_to_binary(result)
      assert String.contains?(sql, "DateTime64(6,'Etc/UTC')")
    end

    test "converts Date in normal range" do
      date = ~D[2024-01-01]
      result = Params.inline_param(date)
      assert IO.iodata_to_binary(result) == "'2024-01-01'::date"
    end

    test "converts Date before 1970 with date32" do
      date = ~D[1950-01-01]
      result = Params.inline_param(date)
      assert IO.iodata_to_binary(result) == "'1950-01-01'::date32"
    end

    test "converts Date after 2148 with date32" do
      date = ~D[2200-01-01]
      result = Params.inline_param(date)
      assert IO.iodata_to_binary(result) == "'2200-01-01'::date32"
    end

    test "converts Decimal" do
      decimal = Decimal.new("123.45")
      result = Params.inline_param(decimal)
      assert result == "123.45"
    end

    test "converts array" do
      result = Params.inline_param([1, 2, 3])
      assert IO.iodata_to_binary(result) == "[1,2,3]"
    end

    test "converts nested array" do
      result = Params.inline_param([[1, 2], [3, 4]])
      assert IO.iodata_to_binary(result) == "[[1,2],[3,4]]"
    end

    test "converts tuple" do
      result = Params.inline_param({1, "test", true})
      assert IO.iodata_to_binary(result) == "(1,'test',true)"
    end

    test "converts map" do
      result = Params.inline_param(%{"key1" => "value1", "key2" => 42})
      sql = IO.iodata_to_binary(result)
      # Map order may vary, so check both possibilities
      assert sql == "map('key1','value1','key2',42)" or
               sql == "map('key2',42,'key1','value1')"
    end

    test "raises for struct that is not supported" do
      assert_raise ArgumentError, ~r/struct URI is not supported in params/, fn ->
        Params.inline_param(%URI{})
      end
    end
  end

  describe "param_type/1" do
    test "returns String for binary" do
      assert Params.param_type("test") == "String"
    end

    test "returns Int64 for normal integers" do
      assert Params.param_type(42) == "Int64"
      assert Params.param_type(-100) == "Int64"
    end

    test "returns UInt64 for large positive integers" do
      large_int = 0x7FFFFFFFFFFFFFFF + 1
      assert Params.param_type(large_int) == "UInt64"
    end

    test "returns UInt128 for very large positive integers" do
      very_large_int = 0xFFFFFFFFFFFFFFFF + 1
      assert Params.param_type(very_large_int) == "UInt128"
    end

    test "returns Int128 for very small negative integers" do
      very_small_int = -0x8000000000000001
      assert Params.param_type(very_small_int) == "Int128"
    end

    test "returns Float64 for float" do
      assert Params.param_type(3.14) == "Float64"
    end

    test "returns Bool for boolean" do
      assert Params.param_type(true) == "Bool"
      assert Params.param_type(false) == "Bool"
    end

    test "returns Date for Date struct" do
      assert Params.param_type(~D[2024-01-01]) == "Date"
    end

    test "returns DateTime64(6) for DateTime without precision" do
      dt = ~U[2024-01-01 12:00:00Z]
      assert Params.param_type(dt) == "DateTime64(6)"
    end

    test "returns DateTime64 with precision for DateTime" do
      dt = ~U[2024-01-01 12:00:00.123Z]
      result = Params.param_type(dt)
      assert IO.iodata_to_binary(result) == "DateTime64(3)"
    end

    test "returns DateTime64(6) for NaiveDateTime without precision" do
      naive = ~N[2024-01-01 12:00:00]
      assert Params.param_type(naive) == "DateTime64(6)"
    end

    test "returns DateTime64 with precision for NaiveDateTime" do
      naive = ~N[2024-01-01 12:00:00.123456]
      result = Params.param_type(naive)
      assert IO.iodata_to_binary(result) == "DateTime64(6)"
    end

    test "returns Decimal64 with scale for Decimal" do
      decimal = Decimal.new("123.45")
      result = Params.param_type(decimal)
      assert IO.iodata_to_binary(result) == "Decimal64(2)"
    end

    test "returns Array(Nothing) for empty array" do
      assert Params.param_type([]) == "Array(Nothing)"
    end

    test "returns Array(String) for string array" do
      result = Params.param_type(["a", "b"])
      assert IO.iodata_to_binary(result) == "Array(String)"
    end

    test "returns Array(Int64) for integer array" do
      result = Params.param_type([1, 2, 3])
      assert IO.iodata_to_binary(result) == "Array(Int64)"
    end

    test "returns Map(Nothing,Nothing) for empty map" do
      assert Params.param_type(%{}) == "Map(Nothing,Nothing)"
    end

    test "returns Map with types for non-empty map" do
      result = Params.param_type(%{"key" => 42})
      assert IO.iodata_to_binary(result) == "Map(String,Int64)"
    end

    test "raises for nil parameter" do
      assert_raise ArgumentError, ~r/param at index is nil/, fn ->
        Params.param_type(nil)
      end
    end

    test "raises for unsupported struct" do
      assert_raise ArgumentError, ~r/struct URI is not supported in params/, fn ->
        Params.param_type(%URI{})
      end
    end
  end
end

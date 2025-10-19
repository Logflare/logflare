defmodule Logflare.Ecto.ClickHouse.HelpersTest do
  use ExUnit.Case, async: true

  alias Logflare.Ecto.ClickHouse.Helpers

  describe "intersperse_map/3" do
    test "maps and intersperses empty list" do
      result = Helpers.intersperse_map([], ",", &to_string/1)
      assert result == []
    end

    test "maps and intersperses single element" do
      result = Helpers.intersperse_map([1], ",", &to_string/1)
      assert result == ["1"]
    end

    test "maps and intersperses multiple elements" do
      result = Helpers.intersperse_map([1, 2, 3], ",", &to_string/1)
      assert result == ["1", ",", "2", ",", "3"]
    end

    test "uses custom mapper function" do
      result = Helpers.intersperse_map([1, 2, 3], " + ", &(&1 * 2))
      assert result == [2, " + ", 4, " + ", 6]
    end

    test "works with different separators" do
      result = Helpers.intersperse_map(["a", "b", "c"], " AND ", &String.upcase/1)
      assert result == ["A", " AND ", "B", " AND ", "C"]
    end
  end

  describe "intersperse_reduce/5" do
    test "reduces empty list" do
      reducer = fn elem, acc -> {to_string(elem), acc + 1} end
      {result, final_acc} = Helpers.intersperse_reduce([], ",", 0, reducer)

      assert result == []
      assert final_acc == 0
    end

    test "reduces single element" do
      reducer = fn elem, acc -> {to_string(elem), acc + elem} end
      {_result, final_acc} = Helpers.intersperse_reduce([5], ",", 0, reducer)

      # Result is nested structure: [[[] | "5"]]
      assert final_acc == 5
    end

    test "reduces multiple elements with accumulator" do
      reducer = fn elem, acc -> {to_string(elem), acc + elem} end
      {_result, final_acc} = Helpers.intersperse_reduce([1, 2, 3], ",", 0, reducer)

      # Result structure is nested, but we track accumulator
      assert final_acc == 6
    end

    test "maintains accumulator through reduction" do
      reducer = fn elem, acc -> {elem * acc, acc + 1} end
      {_result, final_acc} = Helpers.intersperse_reduce([5, 10, 15], " ", 1, reducer)

      assert final_acc == 4
    end

    test "uses custom separator" do
      reducer = fn elem, acc -> {to_string(elem), acc} end
      {_result, _} = Helpers.intersperse_reduce([1, 2, 3], " AND ", 0, reducer)

      # verify it doesn't error
      assert true
    end

    test "allows custom initial accumulator value" do
      reducer = fn elem, acc -> {elem, [elem | acc]} end
      {_result, final_acc} = Helpers.intersperse_reduce([1, 2, 3], ",", [], reducer, [])

      assert final_acc == [3, 2, 1]
    end
  end

  describe "interval/5" do
    test "generates interval for integer count" do
      result = Helpers.interval(5, :day, {}, [], %{})

      assert is_list(result)
      assert ["INTERVAL ", "5", 32, :day] = result
    end

    test "generates interval for float count" do
      result = Helpers.interval(2.5, :hour, {}, [], %{})

      assert is_list(result)
      [_, count_str, _, unit] = result
      assert String.contains?(count_str, "2.5")
      assert unit == :hour
    end

    test "generates interval for zero" do
      result = Helpers.interval(0, :second, {}, [], %{})
      assert ["INTERVAL ", "0", 32, :second] = result
    end

    test "generates interval for negative integer" do
      result = Helpers.interval(-3, :month, {}, [], %{})
      assert ["INTERVAL ", "-3", 32, :month] = result
    end

    test "handles different interval units" do
      units = [:second, :minute, :hour, :day, :week, :month, :year]

      for unit <- units do
        result = Helpers.interval(1, unit, {}, [], %{})
        assert is_list(result)
        [_, _, _, result_unit] = result
        assert result_unit == unit
      end
    end
  end

  describe "ecto_to_db/2" do
    test "converts :integer to Int64" do
      assert Helpers.ecto_to_db(:integer, %{}) == "Int64"
    end

    test "converts :binary to String" do
      assert Helpers.ecto_to_db(:binary, %{}) == "String"
    end

    test "converts :string to String" do
      assert Helpers.ecto_to_db(:string, %{}) == "String"
    end

    test "converts :uuid to UUID" do
      assert Helpers.ecto_to_db(:uuid, %{}) == "UUID"
    end

    test "converts :date to Date" do
      assert Helpers.ecto_to_db(:date, %{}) == "Date"
    end

    test "converts :boolean to Bool" do
      assert Helpers.ecto_to_db(:boolean, %{}) == "Bool"
    end

    test "converts array type recursively" do
      result = Helpers.ecto_to_db({:array, :integer}, %{})
      assert IO.iodata_to_binary(result) == "Array(Int64)"
    end

    test "converts nested array type" do
      result = Helpers.ecto_to_db({:array, {:array, :string}}, %{})
      assert IO.iodata_to_binary(result) == "Array(Array(String))"
    end

    test "converts parameterized Ch type" do
      result = Helpers.ecto_to_db({:parameterized, {Ch, :u8}}, %{})
      assert result == "UInt8"
    end

    test "converts tuple with index and field to String" do
      result = Helpers.ecto_to_db({0, :field_name}, %{})
      assert result == "String"
    end

    test "raises for unknown type" do
      query = %Ecto.Query{from: nil}

      assert_raise Ecto.QueryError, ~r/unknown or ambiguous/, fn ->
        Helpers.ecto_to_db(:unknown_type, query)
      end
    end
  end
end

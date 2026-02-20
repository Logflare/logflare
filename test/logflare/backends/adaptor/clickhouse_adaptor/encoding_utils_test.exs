defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodingUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodingUtils

  describe "sanitize_for_json/1" do
    test "passes through simple values unchanged" do
      assert EncodingUtils.sanitize_for_json("hello") == "hello"
      assert EncodingUtils.sanitize_for_json(42) == 42
      assert EncodingUtils.sanitize_for_json(3.14) == 3.14
      assert EncodingUtils.sanitize_for_json(true) == true
      assert EncodingUtils.sanitize_for_json(nil) == nil
      assert EncodingUtils.sanitize_for_json(:atom) == :atom
    end

    test "recursively sanitizes maps" do
      input = %{"key" => {1, 2}, "nested" => %{"inner" => self()}}
      result = EncodingUtils.sanitize_for_json(input)

      assert result["key"] == [1, 2]
      assert is_binary(result["nested"]["inner"])
    end

    test "recursively sanitizes lists" do
      input = [{1, 2}, self(), "normal"]
      result = EncodingUtils.sanitize_for_json(input)

      assert Enum.at(result, 0) == [1, 2]
      assert is_binary(Enum.at(result, 1))
      assert Enum.at(result, 2) == "normal"
    end

    test "converts tuples to lists" do
      assert EncodingUtils.sanitize_for_json({1, 2, 3}) == [1, 2, 3]
      assert EncodingUtils.sanitize_for_json({:ok, "value"}) == [:ok, "value"]
    end

    test "converts nested tuples to lists" do
      assert EncodingUtils.sanitize_for_json({1, {2, 3}}) == [1, [2, 3]]
    end

    test "converts pids to strings" do
      pid = self()
      result = EncodingUtils.sanitize_for_json(pid)

      assert is_binary(result)
      assert result == inspect(pid)
    end

    test "converts ports to strings" do
      port = Port.open({:spawn, "cat"}, [:binary])
      result = EncodingUtils.sanitize_for_json(port)

      assert is_binary(result)
      assert result == inspect(port)

      Port.close(port)
    end

    test "converts references to strings" do
      ref = make_ref()
      result = EncodingUtils.sanitize_for_json(ref)

      assert is_binary(result)
      assert result == inspect(ref)
    end

    test "converts functions to strings" do
      fun = fn -> :ok end
      result = EncodingUtils.sanitize_for_json(fun)

      assert is_binary(result)
      assert result == inspect(fun)
    end

    test "sanitized output is Jason-encodable" do
      input = %{
        "pid" => self(),
        "tuple" => {1, 2},
        "ref" => make_ref(),
        "list" => [self(), {3, 4}],
        "normal" => "hello"
      }

      result = EncodingUtils.sanitize_for_json(input)
      assert {:ok, _} = Jason.encode(result)
    end
  end
end

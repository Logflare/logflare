defmodule Ecto.TermTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ecto.Term

  describe "type/0" do
    test "returns :binary" do
      assert Term.type() == :binary
    end
  end

  describe "cast/1" do
    property "accepts any value" do
      check all value <- term() do
        assert {:ok, ^value} = Term.cast(value)
      end
    end
  end

  describe "dump/1" do
    property "converts various data types to binary except nil and empty string" do
      check all value <- term(), value != nil && value != "" do
        assert {:ok, binary} = Term.dump(value)
        assert is_binary(binary)
        assert {:ok, ^value} = Term.load(binary)
      end
    end

    test "handles empty string and nil input correctly" do
      assert {:ok, nil} = Term.dump(nil)
      assert {:ok, ""} = Term.dump("")
    end
  end

  describe "load/1" do
    test "handles empty string and nil input correctly" do
      assert {:ok, nil} = Term.load(nil)
      assert {:ok, ""} = Term.load("")
    end

    test "returns error for invalid binary or non-binary input" do
      assert {:error, "errors were found at the given arguments:" <> _rest} =
               Term.load("not a term binary")

      assert {:error, "errors were found at the given arguments:" <> _rest} = Term.load(123)
      assert {:error, "errors were found at the given arguments:" <> _rest} = Term.load([1, 2, 3])
    end
  end

  describe "embed_as/1" do
    property "returns :self for any input" do
      check all value <- term() do
        assert Term.embed_as(value) == :self
      end
    end
  end

  describe "equal?/2" do
    property "is reflective (any value equals itself)" do
      check all value <- term() do
        assert Term.equal?(value, value)
      end
    end

    property "is symmetric (order does not matter)" do
      check all a <- term(), b <- term() do
        assert Term.equal?(a, b) == Term.equal?(b, a)
      end
    end

    test "returns false for not strictly equal terms" do
      refute Term.equal?(3, 3.0)
      refute Term.equal?("hello", :hello)
      refute Term.equal?([1, 2, 3], {1, 2, 3})
      refute Term.equal?(~c"hello", "hello")
      refute Term.equal?(%{"key" => "value"}, %{key: "value"})
      refute Term.equal?("", nil)
    end
  end

  property "cast -> dump -> load preserves data" do
    check all value <- term() do
      assert {:ok, ^value} = Term.cast(value)
      assert {:ok, binary_or_nil} = Term.dump(value)
      assert {:ok, ^value} = Term.load(binary_or_nil)
    end
  end
end

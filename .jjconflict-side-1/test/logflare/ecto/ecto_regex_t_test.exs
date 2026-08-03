defmodule Ecto.RegexTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @valid_modifiers ["i", "m", "s", "x", "u"]

  @valid_regexes [
    "hello",
    "^[a-z]+$",
    "\\d+",
    "(?i)hello",
    "(?m)^test$",
    "(?s).*",
    ".*?",
    "\\d+\\.\\d+"
  ]

  describe "type/0" do
    test "returns :binary" do
      assert Ecto.Regex.type() == :binary
    end
  end

  describe "cast/1" do
    test "casts valid regex string successfully" do
      for string <- @valid_regexes do
        assert Ecto.Regex.cast(string) == {:ok, Regex.compile!(string)}
      end
    end

    property "casts valid regex struct with any combination of modifiers" do
      check all string <- member_of(@valid_regexes), modifiers <- regex_modifier() do
        regex = Regex.compile!(string, modifiers)
        assert Ecto.Regex.cast(regex) == {:ok, regex}
      end
    end

    test "returns error for invalid regex string" do
      assert Ecto.Regex.cast("[") == :error
      assert Ecto.Regex.cast("(") == :error
      assert Ecto.Regex.cast("*") == :error
      assert Ecto.Regex.cast("?") == :error
      assert Ecto.Regex.cast("+") == :error
    end

    test "returns error for non-string, non-regex values" do
      assert Ecto.Regex.cast(123) == :error
      assert Ecto.Regex.cast(nil) == :error
      assert Ecto.Regex.cast([]) == :error
      assert Ecto.Regex.cast(%{}) == :error
      assert Ecto.Regex.cast(:atom) == :error
    end

    property "casting valid regex strings always with at least 1 character returns ok tuple with Regex" do
      check all string <- string(:alphanumeric, min_length: 1) do
        assert {:ok, %Regex{}} = Ecto.Regex.cast(string)
      end
    end
  end

  describe "load/1" do
    property "load/dump roundtrip preserves regex" do
      check all string <- member_of(@valid_regexes), modifiers <- regex_modifier() do
        regex = Regex.compile!(string, modifiers)
        assert {:ok, binary} = Ecto.Regex.dump(regex)
        assert is_binary(binary)
        assert {:ok, ^regex} = Ecto.Regex.load(binary)
      end
    end

    test "returns error for invalid binary data" do
      assert {:error, %ArgumentError{}} = Ecto.Regex.load("invalid binary")
      assert {:error, %ArgumentError{}} = Ecto.Regex.load(<<1, 2, 3>>)
    end

    test "returns error for non-binary input" do
      assert {:error, %ArgumentError{}} = Ecto.Regex.load(123)
      assert {:error, %ArgumentError{}} = Ecto.Regex.load(nil)
      assert {:error, %ArgumentError{}} = Ecto.Regex.load([])
      assert {:error, %ArgumentError{}} = Ecto.Regex.load(%{})
    end
  end

  describe "dump/1" do
    property "converts regex patterns to binary" do
      check all string <- member_of(@valid_regexes), modifiers <- regex_modifier() do
        regex = Regex.compile!(string, modifiers)
        assert {:ok, binary} = Ecto.Regex.dump(regex)
        assert is_binary(binary)
      end
    end

    test "returns error for non-Regex values" do
      assert Ecto.Regex.dump("string") == :error
      assert Ecto.Regex.dump(123) == :error
      assert Ecto.Regex.dump(nil) == :error
      assert Ecto.Regex.dump([]) == :error
      assert Ecto.Regex.dump(%{}) == :error
    end
  end

  describe "embed_as/1" do
    property "returns :self for any input" do
      check all value <- term() do
        assert Ecto.Regex.embed_as(value) == :self
      end
    end
  end

  describe "equal?/2" do
    property "is reflective (any value equals itself)" do
      check all string <- member_of(@valid_regexes), modifiers <- regex_modifier() do
        assert Ecto.Regex.equal?(string, string) == true
        regex = Regex.compile!(string, modifiers)
        assert Ecto.Regex.equal?(regex, regex) == true
      end
    end

    property "is symmetric (order does not matter)" do
      check all a <- member_of(@valid_regexes), b <- member_of(@valid_regexes) do
        assert Ecto.Regex.equal?(a, b) == Ecto.Regex.equal?(b, a)
        a = Regex.compile!(a)
        b = Regex.compile!(b)
        assert Ecto.Regex.equal?(a, b) == Ecto.Regex.equal?(b, a)
      end
    end

    test "returns false for not strictly equal terms" do
      assert Ecto.Regex.equal?(~r/test/, ~r/different/) == false
      assert Ecto.Regex.equal?(~r/test/i, ~r/test/) == false
      assert Ecto.Regex.equal?("test", "tests") == false
      assert Ecto.Regex.equal?("test", ~r/test/) == false
    end
  end

  property "cast -> dump -> load preserves data when initial input is regex" do
    check all string <- member_of(@valid_regexes), modifiers <- regex_modifier() do
      regex = Regex.compile!(string, modifiers)
      assert {:ok, ^regex} = Ecto.Regex.cast(regex)
      assert {:ok, binary} = Ecto.Regex.dump(regex)
      assert {:ok, ^regex} = Ecto.Regex.load(binary)
    end
  end

  property "cast -> dump -> load preserves data when initial input is string" do
    check all string <- member_of(@valid_regexes) do
      regex = Regex.compile!(string)
      assert {:ok, ^regex} = Ecto.Regex.cast(string)
      assert {:ok, binary} = Ecto.Regex.dump(regex)
      assert {:ok, ^regex} = Ecto.Regex.load(binary)
    end
  end

  defp regex_modifier do
    @valid_modifiers
    |> member_of()
    |> list_of(min_length: 0, max_length: Enum.count(@valid_modifiers))
    |> bind(fn list ->
      list |> Enum.uniq() |> Enum.join("") |> constant()
    end)
  end
end

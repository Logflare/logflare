defmodule Logflare.KeyValues.KeyFilterTest do
  use ExUnit.Case, async: false

  alias Logflare.KeyValues.KeyFilter

  setup do
    KeyFilter.clear()
    on_exit(&KeyFilter.clear/0)
  end

  test "every key reads as absent before the first build" do
    assert KeyFilter.add(1, "a") == :ok
    refute KeyFilter.maybe_exists?(1, "a")
    assert KeyFilter.info() == nil
  end

  test "rebuild swaps in a filter of the added keys" do
    assert :built = KeyFilter.rebuild(2, &build(&1, [{1, "a"}, {2, "b"}]))

    assert KeyFilter.maybe_exists?(1, "a")
    assert KeyFilter.maybe_exists?(2, "b")
    refute KeyFilter.maybe_exists?(1, "b")
    assert %{size: 2, capacity: capacity} = KeyFilter.info()
    assert capacity >= 1_000_000
  end

  test "keys added while a rebuild runs land in both filters" do
    KeyFilter.rebuild(1, &build(&1, [{1, "old"}]))

    KeyFilter.rebuild(1, fn filter ->
      KeyFilter.add(1, "during")
      assert KeyFilter.maybe_exists?(1, "during")
      build(filter, [{1, "new"}])
    end)

    assert KeyFilter.maybe_exists?(1, "during")
    assert KeyFilter.maybe_exists?(1, "new")
    refute KeyFilter.maybe_exists?(1, "old")
  end

  test "adding a key twice stores it once" do
    KeyFilter.rebuild(0, &build(&1, []))

    KeyFilter.add(1, "a")
    KeyFilter.add(1, "a")

    assert %{size: 1} = KeyFilter.info()
  end

  test "a failed rebuild keeps the current filter" do
    KeyFilter.rebuild(1, &build(&1, [{1, "a"}]))

    assert_raise RuntimeError, fn -> KeyFilter.rebuild(1, fn _ -> raise "boom" end) end

    assert KeyFilter.maybe_exists?(1, "a")
    KeyFilter.add(1, "b")
    assert %{size: 2} = KeyFilter.info()
  end

  test "keys that were never added rarely read as present" do
    keys = for i <- 1..100_000, do: {i, "project_#{i}"}
    KeyFilter.rebuild(length(keys), &build(&1, keys))

    false_positives = Enum.count(1..100_000, &KeyFilter.maybe_exists?(&1, "other_#{&1}"))

    assert Enum.all?(keys, fn {user_id, key} -> KeyFilter.maybe_exists?(user_id, key) end)
    assert false_positives <= 10
  end

  defp build(filter, pairs) do
    KeyFilter.add_all(filter, pairs)
    :built
  end
end

defmodule Logflare.KeyValues.KeyFilter do
  @moduledoc """
  A cuckoo filter of every `{user_id, key}` with a key value, so lookups for keys that do not exist
  can skip the database.

  It may report a key that does not exist (a false positive) but never misses a key added to it.
  Until the first build, every key reads as absent.
  """

  require Logger

  @current {__MODULE__, :current}
  @pending {__MODULE__, :pending}
  @min_capacity 1_000_000
  @headroom 2
  @hash_range 4_294_967_296

  @type filter() :: :cuckoo_filter.cuckoo_filter()

  @doc """
  Returns `false` when the key has no key value, `true` when it may have one.
  """
  @spec maybe_exists?(integer(), String.t()) :: boolean()
  def maybe_exists?(user_id, key) do
    case :persistent_term.get(@current, nil) do
      nil -> false
      filter -> :cuckoo_filter.contains(filter, {user_id, key})
    end
  end

  @doc """
  Adds a key to the current filter and to a filter being built.
  """
  @spec add(integer(), String.t()) :: :ok
  def add(user_id, key) do
    for filter <- [:persistent_term.get(@current, nil), :persistent_term.get(@pending, nil)],
        filter != nil do
      add_absent(filter, {user_id, key})
    end

    :ok
  end

  @doc """
  Builds a filter sized for `expected_count` keys and swaps it in once `fun` returns.

  `fun` receives the new filter to add keys to with `add_all/2`. Keys added with `add/2` while it
  runs go into both filters.
  """
  @spec rebuild(non_neg_integer(), (filter() -> result)) :: result when result: term()
  def rebuild(expected_count, fun) do
    filter = new_filter(max(expected_count * @headroom, @min_capacity))
    :persistent_term.put(@pending, filter)

    try do
      result = fun.(filter)
      :persistent_term.put(@current, filter)
      result
    after
      :persistent_term.erase(@pending)
    end
  end

  @doc """
  Adds keys to a filter being built.
  """
  @spec add_all(filter(), [{integer(), String.t()}]) :: :ok
  def add_all(filter, pairs) do
    Enum.each(pairs, &add_absent(filter, &1))
  end

  @doc """
  Removes the filter, so every key reads as absent until the next build.
  """
  @spec clear() :: :ok
  def clear do
    :persistent_term.erase(@current)
    :persistent_term.erase(@pending)
    :ok
  end

  @doc """
  The number of keys in the current filter and its capacity, or `nil` before the first build.
  """
  @spec info() :: %{size: non_neg_integer(), capacity: pos_integer()} | nil
  def info do
    case :persistent_term.get(@current, nil) do
      nil -> nil
      filter -> %{size: :cuckoo_filter.size(filter), capacity: :cuckoo_filter.capacity(filter)}
    end
  end

  defp new_filter(capacity) do
    :cuckoo_filter.new(capacity, hash_function: &hash/1)
  end

  defp hash(element) do
    Bitwise.bor(
      Bitwise.bsl(:erlang.phash2(element, @hash_range), 32),
      :erlang.phash2({__MODULE__, element}, @hash_range)
    )
  end

  defp add_absent(filter, element) do
    with false <- :cuckoo_filter.contains(filter, element),
         {:error, :not_enough_space} <- :cuckoo_filter.add(filter, element) do
      Logger.error("Key value filter is full, the key reads as absent until the next build",
        capacity: :cuckoo_filter.capacity(filter)
      )
    end
  end
end

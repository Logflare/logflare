defmodule Logflare.KeyValues.Cache do
  @moduledoc false

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.KeyValues
  alias Logflare.KeyValues.CacheWarmer
  alias Logflare.Repo

  @usage_touch_window :timer.minutes(20)
  @usage_touch_chunk 1_000

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 10_000_000,
      ttl: to_timeout(day: 1),
      purge_interval: to_timeout(hour: 1),
      compressed: true,
      warmer: {KeyValues.CacheWarmer, interval: :timer.hours(1)}
    )
  end

  @spec count(integer()) :: non_neg_integer()
  def count(user_id) do
    cache_key = {:count, user_id}

    Cachex.fetch(__MODULE__, cache_key, fn _key ->
      {:commit, {:cached, Repo.apply_with_replica(KeyValues, :count_key_values, [user_id])}}
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  @spec lookup(integer(), String.t()) :: map() | nil
  def lookup(user_id, key) do
    Cachex.fetch(__MODULE__, {:lookup, [user_id, key, nil]}, fn _key ->
      {:commit, {:cached, Repo.apply_with_replica(KeyValues, :lookup, [user_id, key])}}
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  @spec lookup(integer(), String.t(), KeyValues.prepared_accessor() | nil) :: term() | nil
  def lookup(user_id, key, nil), do: lookup(user_id, key)

  def lookup(user_id, key, {raw_path, _prepared} = accessor_path) when is_binary(raw_path) do
    lookup(user_id, key)
    |> KeyValues.extract_value(accessor_path)
  end

  def lookup(_user_id, _key, other) do
    raise ArgumentError,
          "expected accessor path to be nil or a prepared accessor, got: #{inspect(other)}"
  end

  @doc """
  Records which key-values were recently in use so the next full warm can
  prioritise them.

  Usage is inferred from entries that entered this node's cache since the last
  run, excluding those loaded by the warmer itself. The signal is approximate:
  it captures that a key was needed, not how often it was read.

  Returns the number of `{user_id, key}` pairs whose usage was recorded.
  """
  @spec touch_recent_usages(DateTime.t()) :: {:ok, non_neg_integer()}
  def touch_recent_usages(now \\ DateTime.utc_now()) do
    query = Cachex.Query.build(where: {:>=, :modified, touch_cutoff(now)}, output: :key)

    __MODULE__
    |> Cachex.stream!(query)
    |> Stream.flat_map(fn
      {:lookup, [user_id, key, nil]} -> [{user_id, key}]
      _ -> []
    end)
    |> Stream.chunk_every(@usage_touch_chunk)
    |> Enum.reduce(0, fn pairs, acc ->
      KeyValues.bump_usages(pairs, now)
      acc + length(pairs)
    end)
    |> then(&{:ok, &1})
  end

  defp touch_cutoff(now) do
    window_start = DateTime.to_unix(now, :millisecond) - @usage_touch_window

    case CacheWarmer.warmed_at() do
      nil -> window_start
      warmed_at -> max(window_start, DateTime.to_unix(warmed_at, :millisecond) + 1)
    end
  end

  @impl ContextCache
  def keys_to_bust(kw) do
    user_id = Keyword.get(kw, :user_id)
    key = Keyword.get(kw, :key)

    entries = if user_id, do: [{:count, user_id}], else: []

    if user_id && key do
      [{:lookup, [user_id, key, nil]} | entries]
    else
      entries
    end
  end
end

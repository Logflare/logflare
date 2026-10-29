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
    lookup(user_id, key, nil)
  end

  @spec lookup(integer(), String.t(), KeyValues.accessor() | nil) :: term() | nil
  def lookup(user_id, key, accessor_path) do
    cache_path =
      case accessor_path do
        {path, _prepared} when is_binary(path) ->
          path

        path when is_binary(path) or is_nil(path) ->
          path

        path ->
          raise ArgumentError,
                "expected accessor path to be a string, nil, or a prepared accessor, got: #{inspect(path)}"
      end

    Cachex.execute!(__MODULE__, fn worker ->
      Cachex.fetch(worker, {:lookup, [user_id, key, cache_path]}, fn _key ->
        lookup_fallback(worker, [user_id, key, accessor_path])
      end)
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  defp lookup_fallback(worker, [user_id, key, accessor_path] = key_args) do
    case Cachex.get(worker, {:lookup, [user_id, key, nil]}) do
      {:ok, {:cached, full_value}} ->
        {:commit, {:cached, KeyValues.extract_value(full_value, accessor_path)}}

      {:ok, nil} ->
        {:commit, {:cached, Repo.apply_with_replica(KeyValues, :lookup, key_args)}}
    end
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
      {:lookup, [user_id, key, _accessor]} -> [{user_id, key}]
      _ -> []
    end)
    |> Stream.uniq()
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
      lookup_keys = find_lookup_keys(user_id, key)
      lookup_keys ++ entries
    else
      entries
    end
  end

  defp find_lookup_keys(user_id, key) do
    {:ok, keys} = Cachex.keys(__MODULE__)

    Enum.filter(keys, fn
      {:lookup, [^user_id, ^key | _]} -> true
      _ -> false
    end)
  end
end

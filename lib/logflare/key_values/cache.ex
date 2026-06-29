defmodule Logflare.KeyValues.Cache do
  @moduledoc """
  Read-through cache for the `Logflare.KeyValues` context.

  Caller-facing facade over a Nebulex multi-level cache
  (`Logflare.KeyValues.Cache.Multilevel`). Today the only level is a local,
  Cachex-backed cache (`Logflare.KeyValues.Cache.L1`); a distributed level can
  be added later (O11Y-1504) without touching callers.
  """

  alias Logflare.ContextCache
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache.L1
  alias Logflare.KeyValues.Cache.Multilevel
  alias Logflare.Repo
  alias Logflare.Utils

  import Cachex.Spec

  @behaviour ContextCache

  @cache_stats_default false
  @cache_limit 10_000_000

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {Multilevel, :start_link, [multilevel_opts()]}
    }
  end

  @spec count(integer()) :: non_neg_integer()
  def count(user_id) do
    fetch_or_store({:count, user_id}, fn ->
      Repo.apply_with_replica(KeyValues, :count_key_values, [user_id])
    end)
  end

  @spec lookup(integer(), String.t()) :: map() | nil
  def lookup(user_id, key) do
    lookup(user_id, key, nil)
  end

  @spec lookup(integer(), String.t(), String.t() | nil) :: term() | nil
  def lookup(user_id, key, accessor_path) do
    fetch_or_store({:lookup, [user_id, key, accessor_path]}, fn ->
      Repo.apply_with_replica(KeyValues, :lookup, [user_id, key, accessor_path])
    end)
  end

  @doc """
  Writes entries through the cache. Used by `Logflare.KeyValues.CacheWarmer`.
  """
  @spec put_all([{term(), term()}]) :: :ok | {:error, term()}
  def put_all([]), do: :ok
  def put_all(entries), do: Multilevel.put_all(entries)

  @impl ContextCache
  def bust_by(kw) do
    user_id = Keyword.get(kw, :user_id)
    key = Keyword.get(kw, :key)

    keys = bust_keys(user_id, key)
    # delete by exact keys so the bust propagates to every cache level
    Multilevel.delete_all(in: keys)
  end

  defp fetch_or_store(cache_key, getter) do
    case Multilevel.fetch(cache_key) do
      {:ok, value} ->
        value

      {:error, %Nebulex.KeyError{}} ->
        value = getter.()
        Multilevel.put(cache_key, value)
        value
    end
  end

  defp bust_keys(nil, _key), do: []

  defp bust_keys(user_id, nil), do: [{:count, user_id}]

  defp bust_keys(user_id, key) do
    [{:count, user_id} | lookup_keys(user_id, key)]
  end

  # Gathers `{:lookup, [user_id, key, _accessor]}` keys for every cached accessor
  # variant. Streams keys (not values) from the local level and filters them with
  # a compiled match, which benchmarks faster than an interpreted ETS guard.
  defp lookup_keys(user_id, key) do
    [select: :key]
    |> L1.stream!()
    |> Stream.filter(fn
      {:lookup, [^user_id, ^key | _]} -> true
      _ -> false
    end)
    |> Enum.to_list()
  end

  defp multilevel_opts do
    [
      inclusion_policy: :inclusive,
      levels: [{L1, l1_opts()}]
    ]
  end

  defp l1_opts do
    stats = Application.get_env(:logflare, :cache_stats, @cache_stats_default)

    [
      stats: stats,
      compressed: true,
      warmers: [
        warmer(
          required: false,
          module: KeyValues.CacheWarmer,
          name: KeyValues.CacheWarmer,
          interval: :timer.hours(1)
        )
      ],
      hooks: [Utils.cache_limit(@cache_limit)],
      expiration: Utils.cache_expiration_min(1440, 60)
    ]
  end
end

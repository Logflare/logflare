defmodule Logflare.KeyValues.Cache do
  @moduledoc false

  alias Logflare.ContextCache
  alias Logflare.KeyValues
  alias Logflare.Utils

  @behaviour ContextCache

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start: {
        Cachex,
        :start_link,
        [
          __MODULE__,
          [
            hooks:
              [
                if(stats, do: Utils.cache_stats()),
                Utils.cache_limit(500_000)
              ]
              |> Enum.filter(& &1),
            expiration: Utils.cache_expiration_min(1440, 60)
          ]
        ]
      }
    }
  end

  @spec count(integer()) :: non_neg_integer()
  def count(user_id) do
    cache_key = {:count, user_id}

    Cachex.fetch(__MODULE__, cache_key, fn _key ->
      {:commit, {:cached, KeyValues.count_key_values(user_id)}}
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

  @spec lookup(integer(), String.t(), String.t() | nil) :: term() | nil
  def lookup(user_id, key, accessor_path) do
    cache_key = {:lookup, [user_id, key, accessor_path]}

    Cachex.fetch(__MODULE__, cache_key, fn _key ->
      {:commit, {:cached, KeyValues.lookup(user_id, key, accessor_path)}}
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
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

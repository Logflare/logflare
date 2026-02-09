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

  @spec lookup(integer(), String.t()) :: String.t() | nil
  def lookup(user_id, key) do
    cache_key = {:lookup, [user_id, key]}

    Cachex.fetch(__MODULE__, cache_key, fn _key ->
      {:commit, {:cached, KeyValues.lookup(user_id, key)}}
    end)
    |> case do
      {:commit, {:cached, v}} -> v
      {:ok, {:cached, v}} -> v
    end
  end

  @impl ContextCache
  def bust_by(kw) do
    entries = bust_entries(kw)

    Cachex.execute(__MODULE__, fn worker ->
      Enum.reduce(entries, 0, fn k, acc ->
        acc + delete_and_count(worker, k)
      end)
    end)
  end

  defp bust_entries(kw) do
    user_id = Keyword.get(kw, :user_id)
    key = Keyword.get(kw, :key)

    entries = if user_id, do: [{:count, user_id}], else: []
    if user_id && key, do: [{:lookup, [user_id, key]} | entries], else: entries
  end

  defp delete_and_count(cache, key) do
    case Cachex.take(cache, key) do
      {:ok, nil} -> 0
      {:ok, _value} -> 1
    end
  end
end

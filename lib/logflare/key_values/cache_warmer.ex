defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyValue
  alias Logflare.KeyValues.KeyValueUsage
  alias Logflare.Repo

  require Logger
  import Ecto.Query

  @pt_key {__MODULE__, :initialized}

  @impl true
  def execute(_state) do
    if initialized?() do
      warm_recent()
    else
      try do
        __MODULE__.warm_top_n()
        :persistent_term.put(@pt_key, true)
      rescue
        e ->
          Logger.error("Error performing full KeyValues.Cache warming: #{inspect(e)}")
      end
    end

    :ignore
  end

  def warm_top_n do
    limit =
      Application.get_env(:logflare, __MODULE__, [])
      |> Keyword.get(:warm_limit, 500_000)

    ordered =
      KeyValue
      |> join(:left, [kv], u in KeyValueUsage, on: u.key_value_id == kv.id)
      |> order_by([kv, u], desc_nulls_last: u.last_used_at, desc: kv.updated_at)
      |> limit(^limit)

    Repo.transaction(fn ->
      ordered
      |> Repo.stream()
      |> Stream.chunk_every(500)
      |> Enum.each(fn chunk ->
        entries = Enum.map(chunk, &to_cache_entry/1)
        Cachex.put_many(Cache, entries)
      end)
    end)
  end

  def warm_recent do
    entries =
      KeyValue
      |> where([kv], kv.updated_at >= ago(1, "hour"))
      |> Repo.all()
      |> Enum.map(&to_cache_entry/1)

    if entries != [], do: Cachex.put_many(Cache, entries)
  end

  defp to_cache_entry(%KeyValue{} = kv) do
    {{:lookup, [kv.user_id, kv.key, nil]}, {:cached, kv.value}}
  end

  defp initialized? do
    :persistent_term.get(@pt_key, false)
  end
end

defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyValue
  alias Logflare.Repo

  import Ecto.Query

  @pt_key {__MODULE__, :initialized}

  @impl true
  def execute(_state) do
    if initialized?() do
      warm_recent()
    else
      warm_full()
      :persistent_term.put(@pt_key, true)
    end

    :ignore
  end

  defp warm_full do
    Repo.transaction(fn ->
      KeyValue
      |> Repo.stream()
      |> Stream.chunk_every(500)
      |> Enum.each(fn chunk ->
        entries = Enum.map(chunk, &to_cache_entry/1)
        Cachex.put_many(Cache, entries)
      end)
    end)
  end

  defp warm_recent do
    entries =
      KeyValue
      |> where([kv], kv.inserted_at >= ago(1, "hour"))
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

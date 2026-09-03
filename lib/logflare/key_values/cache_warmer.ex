defmodule Logflare.KeyValues.CacheWarmer do
  @moduledoc false

  use Cachex.Warmer

  import Ecto.Query

  require Logger

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.KeyValue
  alias Logflare.Repo

  @generation_key {__MODULE__, :invalidation_generation}
  @initialized_key {__MODULE__, :initialized}
  @on_load :init_generation

  @doc false
  def init_generation do
    unless is_reference(:persistent_term.get(@generation_key, nil)) do
      :persistent_term.put(@generation_key, :atomics.new(1, signed: false))
    end

    :ok
  end

  @impl true
  def execute(_state) do
    mode = if initialized?(), do: :recent, else: :full

    case safely_warm(mode) do
      :ok ->
        if mode == :full do
          :persistent_term.put(@initialized_key, Process.whereis(Cache))
        end

      {:error, reason} ->
        if mode == :full, do: clear_cache()
        Logger.error("Error performing #{mode} KeyValues.Cache warming: #{reason}")
    end

    :ignore
  end

  @spec warm_full() :: :ok | {:error, term()}
  def warm_full do
    generation = invalidation_generation()

    transaction_result = Repo.transaction(fn -> warm_stream(generation) end)

    case transaction_result do
      {:ok, :ok} ->
        if generation == invalidation_generation(), do: :ok, else: clear_cache()

      {:ok, :invalidated} ->
        clear_cache()

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec warm_recent() :: :ok | {:error, term()}
  def warm_recent do
    generation = invalidation_generation()

    entries =
      KeyValue
      |> where([kv], kv.updated_at >= ago(2, "hour"))
      |> Repo.all()
      |> Enum.map(&to_cache_entry/1)

    case put_if_unchanged(entries, generation) do
      :invalidated -> :ok
      result -> result
    end
  end

  @doc false
  @spec put_if_unchanged([{term(), term()}], non_neg_integer()) ::
          :ok | :invalidated | {:error, term()}
  def put_if_unchanged(entries, generation) do
    with true <- generation == invalidation_generation(),
         :ok <- put_entries(entries) do
      discard_if_invalidated(entries, generation)
    else
      false -> :invalidated
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec mark_invalidation() :: :ok
  def mark_invalidation do
    :atomics.add_get(generation_ref(), 1, 1)
    :ok
  end

  @doc false
  @spec invalidation_generation() :: non_neg_integer()
  def invalidation_generation do
    :atomics.get(generation_ref(), 1)
  end

  defp safely_warm(mode) do
    function = if mode == :full, do: :warm_full, else: :warm_recent
    result = apply(__MODULE__, function, [])

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, inspect(reason)}
      other -> {:error, "unexpected return: #{inspect(other)}"}
    end
  rescue
    error -> {:error, Exception.format(:error, error, __STACKTRACE__)}
  catch
    kind, reason -> {:error, Exception.format(kind, reason, __STACKTRACE__)}
  end

  defp warm_stream(generation) do
    KeyValue
    |> Repo.stream()
    |> Stream.chunk_every(500)
    |> Enum.reduce_while(:ok, fn chunk, status -> warm_chunk(chunk, status, generation) end)
  end

  defp warm_chunk(chunk, :ok, generation) do
    case put_if_unchanged(Enum.map(chunk, &to_cache_entry/1), generation) do
      :ok -> {:cont, :ok}
      :invalidated -> {:halt, :invalidated}
      {:error, reason} -> Repo.rollback({:cache_write_failed, reason})
    end
  end

  defp discard_if_invalidated(entries, generation) do
    if generation == invalidation_generation() do
      :ok
    else
      with :ok <- delete_entries(entries), do: :invalidated
    end
  end

  defp put_entries([]), do: :ok

  defp put_entries(entries) do
    case Cachex.put_many(Cache, entries) do
      {:ok, _written?} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_entries(entries) do
    Enum.reduce_while(entries, :ok, fn {key, _value}, :ok ->
      case Cachex.del(Cache, key) do
        {:ok, _deleted?} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp clear_cache do
    case Cachex.clear(Cache) do
      {:ok, _count} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp to_cache_entry(%KeyValue{} = kv) do
    {{:lookup, [kv.user_id, kv.key, nil]}, {:cached, kv.value}}
  end

  defp initialized? do
    cache_pid = Process.whereis(Cache)
    is_pid(cache_pid) and :persistent_term.get(@initialized_key, nil) == cache_pid
  end

  defp generation_ref do
    :persistent_term.get(@generation_key)
  end
end

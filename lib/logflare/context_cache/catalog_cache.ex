defmodule Logflare.ContextCache.CatalogCache do
  @moduledoc """
  Primary-backed catalog fills fenced by per-owner generations.

  Generation tokens have bounded retention. Losing a token creates a new
  generation rather than making an older cached snapshot current again.
  """

  alias Logflare.Repo
  alias Logflare.Utils

  def child_spec(_) do
    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks: [Utils.cache_limit(100_000)],
             expiration: Utils.cache_expiration_min(60, 5)
           ]
         ]}
    }
  end

  @spec fetch(atom(), term(), (-> list())) :: list()
  def fetch(cache, key, loader) do
    generation = generation(cache, key)
    entry_key = {:catalog, key, generation}

    {_status, {:cached, values}} =
      Cachex.fetch(cache, entry_key, fn ->
        case Repo.with_primary(loader) do
          [] -> {:ignore, {:cached, []}}
          values -> {:commit, {:cached, values}}
        end
      end)

    if generation == generation(cache, key), do: values, else: Repo.with_primary(loader)
  end

  @spec bust(atom(), [term()]) :: {:ok, non_neg_integer()}
  def bust(cache, keys) do
    count =
      keys
      |> Enum.uniq()
      |> Enum.reduce(0, fn key, count -> count + bust_key(cache, key) end)

    {:ok, count}
  end

  defp generation(cache, key) do
    metadata_key = {cache, key}

    case Cachex.get!(__MODULE__, metadata_key) do
      nil ->
        {:commit, generation} =
          Cachex.get_and_update(__MODULE__, metadata_key, fn generation ->
            {:commit, generation || make_ref()}
          end)

        generation

      generation ->
        generation
    end
  end

  defp bust_key(cache, key) do
    metadata_key = {cache, key}

    {:ok, generation} =
      Cachex.transaction(__MODULE__, [metadata_key], fn worker ->
        previous = Cachex.get!(worker, metadata_key)
        Cachex.put!(worker, metadata_key, make_ref())
        previous
      end)

    case Cachex.take!(cache, {:catalog, key, generation}) do
      nil -> 0
      _value -> 1
    end
  end
end

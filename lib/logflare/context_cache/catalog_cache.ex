defmodule Logflare.ContextCache.CatalogCache do
  @moduledoc """
  Primary-backed catalog fills fenced by per-owner generations.

  Generation tokens have bounded retention. Losing a token creates a new
  generation rather than making an older cached snapshot current again.
  """

  use Logflare.Cache

  alias Logflare.Cache.CachexOps
  alias Logflare.Repo

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(hour: 1),
      purge_interval: to_timeout(minute: 5),
      transactions: true
    )
  end

  @spec fetch(atom(), term(), (-> list())) :: list()
  def fetch(cache, key, loader) do
    generation = generation(cache, key)
    entry_key = {:catalog, key, generation}

    {_status, {:cached, values}} =
      Cachex.fetch(cache, entry_key, fn ->
        {:commit, {:cached, Repo.with_primary(loader)}}
      end)

    if generation == generation(cache, key), do: values, else: Repo.with_primary(loader)
  end

  @spec keys_to_bust(atom(), [term()]) :: [term()]
  def keys_to_bust(cache, keys) do
    keys
    |> Enum.uniq()
    |> Enum.map(&advance_generation(cache, &1))
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

  defp advance_generation(cache, key) do
    metadata_key = {cache, key}

    {:ok, generation} =
      Cachex.transaction(__MODULE__, [metadata_key], fn worker ->
        previous = Cachex.get!(worker, metadata_key)
        Cachex.put!(worker, metadata_key, make_ref())
        previous
      end)

    {:catalog, key, generation}
  end
end

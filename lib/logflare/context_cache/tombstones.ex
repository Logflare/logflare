defmodule Logflare.ContextCache.Tombstones do
  @moduledoc """
  Short-lived markers of recently updated or deleted records, kept in `Logflare.ContextCache.Tombstones.Cache`.

  Incoming cache multicasts check them to determine if their payload could be stale, see
  `Logflare.ContextCache.Gossip`.
  """

  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones

  @doc """
  Records the tombstones `c:Logflare.ContextCache.tombstones/1` derives for each busted change.

  A primary key is passed to it as `[id: pkey]`.
  """
  @spec record(list()) :: :ok
  def record(context_pkeys) when is_list(context_pkeys) do
    Enum.each(context_pkeys, fn
      # don't need to tombstone new records
      {_context, :not_found} ->
        :ignore

      {context, pkey_or_kw} ->
        cache = ContextCache.cache_name(context)

        for {tombstone, ttl} <- cache.tombstones(to_kw(pkey_or_kw)) do
          Tombstones.Cache.put_tombstone(cache, tombstone, ttl)
        end
    end)
  end

  @doc """
  Returns a tombstone with the default time-to-live for the `:id`, if there is one.
  """
  @spec pkey_tombstones(keyword()) :: [{integer(), nil}]
  def pkey_tombstones(kw) do
    case Keyword.fetch(kw, :id) do
      {:ok, id} -> [{id, nil}]
      :error -> []
    end
  end

  defp to_kw(kw) when is_list(kw), do: kw
  defp to_kw(pkey), do: [id: pkey]
end

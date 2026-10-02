defmodule Logflare.ContextCache.PeerWarmer.Store do
  @moduledoc """
  Storage adapter used by `Logflare.ContextCache.PeerWarmer` to read entries from a peer's
  cache and write them into the local one, independent of the caching library behind it.
  """

  @type target :: atom()
  @type entry :: term()

  @callback stream(target()) :: Enumerable.t()
  @callback key(entry()) :: term()
  @callback value(entry()) :: term()
  @callback exists?(target(), key :: term()) :: boolean()
  @callback put_entries(target(), [entry()]) :: :ok
  @callback size(target()) :: non_neg_integer()
end

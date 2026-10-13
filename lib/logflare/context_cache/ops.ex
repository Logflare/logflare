defmodule Logflare.ContextCache.Ops do
  @moduledoc """
  Implementation of the `Logflare.ContextCache` callbacks on one storage backend.

  Selected with `use Logflare.ContextCache, impl: module`, which also requires the module to
  implement `Logflare.Cache.Ops`. Every function takes the cache module the callback was called on.
  """

  @callback keys_to_bust(cache :: module(), keyword()) :: Enumerable.t()
  @callback delete_keys(cache :: module(), Enumerable.t()) :: {:ok, non_neg_integer()}
  @callback fetch(cache :: module(), key :: term(), getter :: (-> term())) :: term()
  @callback update(cache :: module(), key :: term(), value :: term()) :: :ok
end

defmodule Logflare.ContextCache.Ops do
  @moduledoc """
  Implementation of the `Logflare.ContextCache` callbacks on one storage backend.

  Selected with `use Logflare.ContextCache, impl: module`, which also requires the module to
  implement `Logflare.Cache.Ops`. Every function takes the cache module the callback was called on.
  """

  @callback bust_by(cache :: module(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  @callback fetch(cache :: module(), key :: term(), getter :: (-> term())) :: term()
  @callback update(cache :: module(), key :: term(), value :: term()) :: :ok
  @callback entries(cache :: module()) :: Enumerable.t(Logflare.ContextCache.entry())
  @callback put_entries(cache :: module(), [Logflare.ContextCache.entry()]) :: :ok
  @callback cached?(cache :: module(), key :: term()) :: boolean()
  @callback entry(cache :: module(), key :: term()) :: Logflare.ContextCache.entry() | nil
  @callback expiry(cache :: module(), key :: term()) ::
              {remaining :: non_neg_integer(), total :: pos_integer()} | nil
  @callback size(cache :: module()) :: non_neg_integer()
end

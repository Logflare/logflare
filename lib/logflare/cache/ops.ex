defmodule Logflare.Cache.Ops do
  @moduledoc """
  Implementation of the `Logflare.Cache` callbacks on one storage backend.

  Selected with `use Logflare.Cache, impl: module`. Every function takes the cache module
  the callback was called on.
  """

  @callback healthy?(cache :: module()) :: boolean()
  @callback stats(cache :: module()) :: Logflare.Cache.stats()
  @callback reset(cache :: module()) :: :ok
end

defmodule Logflare.KeyValues.Cache.L1 do
  @moduledoc """
  Local (L1) level of the `Logflare.KeyValues.Cache` multi-level cache.

  Backed by Cachex through `Nebulex.Adapters.Cachex`, preserving the original
  Cachex configuration (compression, warmers, size limit, stats, expiration).
  Start options are provided by `Logflare.KeyValues.Cache` when the multi-level
  cache boots its levels.
  """

  use Nebulex.Cache,
    otp_app: :logflare,
    adapter: Nebulex.Adapters.Cachex
end

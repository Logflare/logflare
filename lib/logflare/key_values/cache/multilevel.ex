defmodule Logflare.KeyValues.Cache.Multilevel do
  @moduledoc """
  Nebulex multi-level cache backing `Logflare.KeyValues.Cache`.

  Currently configured with a single local level (`Logflare.KeyValues.Cache.L1`,
  Cachex-backed). It provides the seam for adding a distributed L2 level later
  (see O11Y-1504) without changing callers, which always go through the
  `Logflare.KeyValues.Cache` facade.
  """

  use Nebulex.Cache,
    otp_app: :logflare,
    adapter: Nebulex.Adapters.Multilevel
end

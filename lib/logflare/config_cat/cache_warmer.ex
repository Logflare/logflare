defmodule Logflare.ConfigCat.CacheWarmer do
  @moduledoc """
  Periodically refreshes ConfigCat percentage-based feature flag cache entries.

  Uses `ConfigCat.get_all_value_details/0` to discover which flags use percentage
  rollouts, then pre-populates the cache for all 100 discrete hash buckets (0-99)
  matching the key format used by `Logflare.Utils.cached_flag_value_pct_of_identifiers/2`.
  """

  use Cachex.Warmer

  @bucket_count Logflare.Utils.flag_percent_hash_limit()
  @flags ~w(key_values)

  @impl true
  def execute(_state) do
    pairs =
      for flag <- @flags, hash <- 0..(@bucket_count - 1) do
        cache_key = "#{flag}:#{hash}"
        user = ConfigCat.User.new(cache_key)
        {cache_key, ConfigCat.get_value(flag, false, user)}
      end

    {:ok, pairs}
  end
end

defmodule Logflare.KeyValues.KeyValueUsage do
  @moduledoc """
  A helper table storing the last-touched timestamp for `KeyValue` entries.

  This table is intentionally **excluded from the `logflare_pub` publication**, so
  upserting `last_used_at` does not flow through `ContextCache.CacheBuster` and does
  not evict entries from `KeyValues.Cache` — which is precisely the cache whose access
  patterns drive the usage signal. Storing usage on the `key_values` row itself would
  cause every touch to bust the cache entry it is trying to track.
  """
  use TypedEctoSchema

  alias Logflare.KeyValues.KeyValue

  typed_schema "key_value_usages" do
    field :last_used_at, :utc_datetime_usec

    belongs_to :key_value, KeyValue
  end
end

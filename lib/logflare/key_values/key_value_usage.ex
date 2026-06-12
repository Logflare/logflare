defmodule Logflare.KeyValues.KeyValueUsage do
  @moduledoc false
  use TypedEctoSchema

  alias Logflare.KeyValues.KeyValue

  typed_schema "key_value_usages" do
    field :last_used_at, :utc_datetime_usec

    belongs_to :key_value, KeyValue
  end
end

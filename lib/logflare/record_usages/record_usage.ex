defmodule Logflare.RecordUsages.RecordUsage do
  @moduledoc false
  use TypedEctoSchema

  typed_schema "record_usages" do
    field :oauth_access_token_id, :id
    field :last_used_at, :utc_datetime_usec
  end
end

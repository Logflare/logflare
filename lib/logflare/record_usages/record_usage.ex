defmodule Logflare.RecordUsages.RecordUsage do
  @moduledoc """
  Schema for the latest persisted usage timestamp of each tracked resource.

  Usage is stored separately from resource records to avoid invalidating their caches.
  """

  use TypedEctoSchema

  typed_schema "record_usages" do
    field :oauth_access_token_id, :id
    field :legacy_api_key_user_id, :id
    field :last_used_at, :utc_datetime
  end
end

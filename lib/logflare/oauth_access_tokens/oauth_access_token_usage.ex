defmodule Logflare.OauthAccessTokens.OauthAccessTokenUsage do
  @moduledoc false
  use TypedEctoSchema

  @primary_key {:access_token_id, :id, autogenerate: false}
  typed_schema "oauth_access_token_usages" do
    field :last_used_at, :utc_datetime_usec
  end
end

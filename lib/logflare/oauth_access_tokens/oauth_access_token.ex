defmodule Logflare.OauthAccessTokens.OauthAccessToken do
  @moduledoc false
  use TypedEctoSchema
  use ExOauth2Provider.AccessTokens.AccessToken, otp_app: :logflare

  alias Logflare.OauthAccessTokens.OauthAccessTokenUsage

  @derive {Jason.Encoder,
           only: [
             :id,
             :token,
             :scopes,
             :inserted_at,
             :description
           ]}

  typed_schema "oauth_access_tokens" do
    access_token_fields()
    field(:description, :string)
    has_one(:usage, OauthAccessTokenUsage, foreign_key: :access_token_id)
    timestamps()
  end
end

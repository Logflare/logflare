defmodule Logflare.OauthAccessTokens.OauthAccessToken do
  use TypedEctoSchema
  use ExOauth2Provider.AccessTokens.AccessToken, otp_app: :logflare

  typed_schema "oauth_access_tokens" do
    access_token_fields()

    timestamps()
  end
end

defmodule Logflare.OauthAccessTokens.PartnerOauthAccessToken do
  @moduledoc false
  use TypedEctoSchema
  use ExOauth2Provider.AccessTokens.AccessToken, otp_app: :logflare

  typed_schema "oauth_access_tokens" do
    belongs_to(:resource_owner, Logflare.Partners.Partner)

    access_token_fields()
    field(:description, :string)
    timestamps()
  end
end

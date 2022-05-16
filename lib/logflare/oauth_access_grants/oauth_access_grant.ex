defmodule Logflare.OauthAccessGrants.OauthAccessGrant do
  use TypedEctoSchema
  use ExOauth2Provider.AccessGrants.AccessGrant, otp_app: :logflare

  typed_schema "oauth_access_grants" do
    access_grant_fields()

    timestamps(updated_at: false)
  end
end

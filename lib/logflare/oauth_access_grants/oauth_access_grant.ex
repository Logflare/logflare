defmodule Logflare.OauthAccessGrants.OauthAccessGrant do
  use Ecto.Schema
  use ExOauth2Provider.AccessGrants.AccessGrant, otp_app: :logflare

  schema "oauth_access_grants" do
    access_grant_fields()

    timestamps(updated_at: false)
  end
end

defmodule Logflare.OauthApplications.OauthApplication do
  use TypedEctoSchema
  use ExOauth2Provider.Applications.Application, otp_app: :logflare

  typed_schema "oauth_applications" do
    application_fields()

    timestamps()
  end
end

defmodule Logflare.OauthApplications.PartnerOauthApplication do
  @moduledoc false
  use TypedEctoSchema
  use ExOauth2Provider.Applications.Application, otp_app: :logflare

  typed_schema "oauth_applications" do
    belongs_to :owner, Logflare.Partners.Partner

    application_fields()

    timestamps()
  end
end

defmodule Logflare.OauthApplications.OauthApplication do
  use Ecto.Schema
  use ExOauth2Provider.Applications.Application

  schema "oauth_applications" do
    application_fields()

    timestamps()
  end
end

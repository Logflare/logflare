defmodule Logflare.Repo.Migrations.RedirectUriToText do
  use Ecto.Migration

  def change do
    alter table(:oauth_access_grants) do
      modify :redirect_uri, :text
    end
  end
end

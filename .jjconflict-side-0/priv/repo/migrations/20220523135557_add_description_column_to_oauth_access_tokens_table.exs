defmodule Logflare.Repo.Migrations.AddDescriptionColumnToOauthAccessTokensTable do
  use Ecto.Migration

  def change do
    alter table("oauth_access_tokens") do
      add :description, :text
    end
  end
end

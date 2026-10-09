defmodule Logflare.Repo.Migrations.AddAccessTokenUsageTracking do
  use Ecto.Migration

  def change do
    create table(:oauth_access_token_usages, primary_key: false) do
      add :access_token_id, references(:oauth_access_tokens, on_delete: :delete_all),
        primary_key: true

      add :last_used_at, :utc_datetime_usec, null: false
    end
  end
end

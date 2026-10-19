defmodule Logflare.Repo.Migrations.CreateRecordUsages do
  use Ecto.Migration

  def change do
    create table(:record_usages, options: "WITH (fillfactor=70)") do
      add :oauth_access_token_id, references(:oauth_access_tokens, on_delete: :delete_all)
      add :legacy_api_key_user_id, references(:users, on_delete: :delete_all)
      add :last_used_at, :utc_datetime, null: false
    end

    create unique_index(:record_usages, [:oauth_access_token_id])
    create unique_index(:record_usages, [:legacy_api_key_user_id])

    create constraint(:record_usages, :record_usages_exactly_one_resource,
             check: "num_nonnulls(oauth_access_token_id, legacy_api_key_user_id) = 1"
           )
  end
end

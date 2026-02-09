defmodule Logflare.Repo.Migrations.CreateKeyValuesTable do
  use Ecto.Migration

  def change do
    create table(:key_values) do
      add :user_id, references(:users, on_delete: :delete_all), null: false
      add :key, :text, null: false
      add :value, :text, null: false
    end

    create unique_index(:key_values, [:user_id, :key])
    create index(:key_values, [:user_id, :value])
  end
end

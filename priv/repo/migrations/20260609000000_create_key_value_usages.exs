defmodule Logflare.Repo.Migrations.CreateKeyValueUsages do
  use Ecto.Migration

  def change do
    create table(:key_value_usages) do
      add :key_value_id, references(:key_values, on_delete: :delete_all), null: false
      add :last_used_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    create unique_index(:key_value_usages, [:key_value_id])
    create index(:key_value_usages, [:last_used_at])
  end
end

defmodule Logflare.Repo.Migrations.AddTokenToRules do
  use Ecto.Migration

  def up do
    alter table(:rules) do
      add :token, :uuid, default: fragment("gen_random_uuid()")
    end
    execute "UPDATE rules SET token=gen_random_uuid() WHERE token is null"
    create unique_index(:rules, [:token])
  end

  def down do
    drop index(:rules, [:token])
    alter table(:rules) do
      remove(:token, :uuid)
    end
  end
end

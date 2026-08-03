defmodule Logflare.Repo.Migrations.SetTokenDefaultValues do
  use Ecto.Migration

  @disable_ddl_transaction true
  @disable_migration_lock true

  def change do
    alter table(:teams) do
      modify :token, :string, default: fragment("gen_random_uuid()")
    end

    execute "UPDATE teams SET token=gen_random_uuid() WHERE token is null"
  end
end

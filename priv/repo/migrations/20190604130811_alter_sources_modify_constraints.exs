defmodule Logflare.Repo.Migrations.AlterSourcesModifyConstraints do
  @moduledoc false
  use Ecto.Migration

  alias Logflare.Repo.Migrator

  def up do
    Migrator.with_replicated_execute(fn ->
      alter table(:sources) do
        modify :token, :uuid, null: false
        modify :user_id, :integer, null: false
      end

      execute "drop index sources_name_index;"

      execute "create unique index if not exists sources_name_index
    on sources (id, name)"
    end)
  end

  def down do
    Migrator.with_replicated_execute(fn ->
      execute "create unique index if not exists sources_name_index on sources (name);"

      alter table(:sources) do
        modify :token, :uuid
        modify :user_id, :integer
      end
    end)
  end
end

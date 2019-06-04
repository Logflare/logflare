defmodule Logflare.Repo.Migrations.AlterSourcesModifyConstraints do
  use Ecto.Migration

  def up do
    alter table(:sources) do
      modify :token, :uuid, null: false
      modify :user_id, :integer, null: false
      remove :overflow_source
    end

    execute "drop index sources_name_index;"
    execute "create unique index if not exists sources_name_index
    on sources (id, name)"
  end

  def down do
    execute "create unique index if not exists sources_name_index on sources (name);"

    alter table(:sources) do
      modify :token, :uuid
      modify :user_id, :integer
      remove :overflow_source
    end
  end
end

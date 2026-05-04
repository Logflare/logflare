defmodule Logflare.Repo.Migrations.AlterSourcesModifyConstraints do
  @moduledoc false
  use Ecto.Migration

  def up do
    alter table(:sources) do
      modify :token, :uuid, null: false
      modify :user_id, :integer, null: false
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
    end
  end
end

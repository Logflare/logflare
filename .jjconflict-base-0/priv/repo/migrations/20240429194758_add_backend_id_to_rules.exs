defmodule Logflare.Repo.Migrations.AddBackendIdToRules do
  use Ecto.Migration

  def change do
    alter table("rules") do
      add :backend_id, references(:backends, on_delete: :delete_all)
      modify(:sink, references(:sources, column: :token, type: :uuid, on_delete: :delete_all), null: true, from: {references(:sources, column: :token, type: :uuid, on_delete: :delete_all), null: false})
    end
  end
end

defmodule Logflare.Repo.Migrations.AddIssueTypeFieldsToAlertQueries do
  use Ecto.Migration

  def change do
    alter table(:alert_queries) do
      add :rule_key, :string
      add :kind, :string, null: false, default: "scheduled"
      add :managed, :boolean, null: false, default: false
      add :severity, :string, null: false, default: "warning"
      add :category, :string, null: false, default: "health"
      add :signal_mapping, :map, null: false, default: %{}
      add :scope, :map, null: false, default: %{}
      add :group_by, {:array, :string}, null: false, default: []
      add :resolve_after_runs, :integer, null: false, default: 2
    end

    create unique_index(:alert_queries, [:user_id, :rule_key],
             where: "rule_key IS NOT NULL",
             name: :alert_queries_user_id_rule_key_index
           )
  end
end

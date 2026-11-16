defmodule Logflare.Repo.Migrations.CreateAlertIssueSilences do
  use Ecto.Migration

  def change do
    create table(:alert_issue_silences) do
      add :user_id, references(:users, on_delete: :delete_all), null: false
      add :rule_key, :string
      add :matchers, :map, null: false, default: %{}
      add :comment, :text
      add :created_by, :string

      timestamps(type: :utc_datetime_usec)
    end

    create index(:alert_issue_silences, [:user_id])
  end
end

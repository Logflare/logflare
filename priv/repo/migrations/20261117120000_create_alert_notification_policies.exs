defmodule Logflare.Repo.Migrations.CreateAlertNotificationPolicies do
  use Ecto.Migration

  def change do
    create table(:alert_notification_policies) do
      add :user_id, references(:users, on_delete: :delete_all), null: false
      add :name, :string, null: false
      add :enabled, :boolean, null: false, default: true
      add :rule_keys, {:array, :string}, null: false, default: []
      add :min_severity, :string
      add :categories, {:array, :string}, null: false, default: []
      add :identifiers, :map, null: false, default: %{}
      add :events, {:array, :string}, null: false, default: ["opened", "reopened", "repeat"]
      add :repeat_interval_seconds, :integer
      add :receivers, {:array, :string}, null: false, default: []

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:alert_notification_policies, [:user_id, :name])
  end
end

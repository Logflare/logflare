defmodule Logflare.Backends.Adaptor.Postgres.Repo.Migrations.AddLogEvents do
  use Ecto.Migration

  def up do
    create table(:log_events, primary_key: false) do
      add(:id, :string, primary_key: true)
      add(:metadata, :map)
      add(:event_message, :string)
      add(:timestamp, :utc_datetime_usec)
    end
  end

  def down do
    drop(table(:log_events))
  end
end

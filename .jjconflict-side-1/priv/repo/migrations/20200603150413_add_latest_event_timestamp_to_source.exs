defmodule Logflare.Repo.Migrations.AddLatestEventTimestampToSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :log_events_updated_at, :naive_datetime
    end
  end
end

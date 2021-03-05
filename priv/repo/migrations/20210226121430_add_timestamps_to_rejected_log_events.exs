defmodule Logflare.Repo.Migrations.AddTimestampsToRejectedLogEvents do
  use Ecto.Migration

  def change do
    alter table(:rejected_log_events) do
      timestamps()
    end
  end
end

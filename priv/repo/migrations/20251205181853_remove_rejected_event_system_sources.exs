defmodule Logflare.Repo.Migrations.RemoveRejectedEventSystemSources do
  use Ecto.Migration

  def up do
    execute("DELETE FROM sources WHERE system_source_type = 'rejected_events'")
  end

  def down do
    :ok
  end
end

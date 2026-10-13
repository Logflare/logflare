defmodule Logflare.Repo.Migrations.AddSystemTracesSources do
  use Ecto.Migration

  def up do
    execute("""
    INSERT INTO sources (name, token, user_id, system_source, system_source_type, favorite, inserted_at, updated_at)
    SELECT 'system.traces', gen_random_uuid(), id, true, 'traces', true, now(), now()
    FROM users
    WHERE system_monitoring = true
    ON CONFLICT DO NOTHING
    """)
  end

  def down do
    execute("DELETE FROM sources WHERE system_source_type = 'traces'")
  end
end

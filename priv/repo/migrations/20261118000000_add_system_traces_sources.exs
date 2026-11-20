defmodule Logflare.Repo.Migrations.AddSystemTracesSources do
  use Ecto.Migration

  def up do
    execute("""
    INSERT INTO sources (
      name,
      token,
      user_id,
      favorite,
      api_quota,
      bq_table_partition_type,
      default_ingest_backend_enabled,
      system_source,
      system_source_type,
      inserted_at,
      updated_at
    )
    SELECT
      'system.traces',
      gen_random_uuid(),
      users.id,
      true,
      25,
      'timestamp',
      false,
      true,
      'traces',
      now() AT TIME ZONE 'utc',
      now() AT TIME ZONE 'utc'
    FROM users
    WHERE users.system_monitoring = true
    ON CONFLICT (user_id, system_source_type) DO NOTHING
    """)
  end

  def down do
    execute("""
    DELETE FROM sources_backends
    WHERE source_id IN (SELECT id FROM sources WHERE system_source_type = 'traces')
    """)

    execute("""
    DELETE FROM source_backends
    WHERE source_id IN (SELECT id FROM sources WHERE system_source_type = 'traces')
    """)

    execute("DELETE FROM sources WHERE system_source_type = 'traces'")
  end
end

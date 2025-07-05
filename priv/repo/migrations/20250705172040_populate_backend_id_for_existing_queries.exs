defmodule Logflare.Repo.Migrations.PopulateBackendIdForExistingQueries do
  use Ecto.Migration

  def up do
    # Only update where a query.user has exactly one backend matching the query.language
    # Others should probably be worked out manually
    execute """
      UPDATE endpoint_queries
      SET backend_id = (
        SELECT b.id FROM backends b
        WHERE b.user_id = endpoint_queries.user_id
        AND ((endpoint_queries.language = 'bq_sql' AND b.type = 'bigquery') OR
             (endpoint_queries.language = 'pg_sql' AND b.type = 'postgres'))
      )
      WHERE backend_id IS NULL
      AND (
        SELECT COUNT(*) FROM backends b
        WHERE b.user_id = endpoint_queries.user_id
        AND ((endpoint_queries.language = 'bq_sql' AND b.type = 'bigquery') OR
             (endpoint_queries.language = 'pg_sql' AND b.type = 'postgres'))
      ) = 1
    """
  end
end

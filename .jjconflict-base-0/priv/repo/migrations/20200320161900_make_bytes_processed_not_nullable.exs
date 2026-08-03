defmodule Logflare.Repo.Migrations.MakeBytesProcessedNotNullable do
  use Ecto.Migration

  def change do
    alter table(:users) do
      modify(:bigquery_processed_bytes_limit, :bigint, null: false)
    end
  end
end

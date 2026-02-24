defmodule Logflare.Repo.Migrations.AddCustomBigqueryBytesLimit do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :bigquery_processed_bytes_limit, :bigint, default: 10_000_000_000, nullable: false
    end
  end
end

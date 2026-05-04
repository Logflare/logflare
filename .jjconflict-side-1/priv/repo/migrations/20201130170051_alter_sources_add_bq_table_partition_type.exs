defmodule Logflare.Repo.Migrations.AlterSourcesAddBqTablePartitionType do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :bq_table_partition_type, :text
    end
  end
end

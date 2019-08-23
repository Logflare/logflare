defmodule Logflare.Repo.Migrations.AddBqDatasetId do
  @moduledoc false
  use Ecto.Migration

  def change do
    alter table(:users) do
      add(:bigquery_dataset_id, :string)
    end
  end
end

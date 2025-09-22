defmodule Logflare.Repo.Migrations.AddBigQueryReservationColumnsToUsers do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :bigquery_reservation_search, :string
      add :bigquery_reservation_alerts, :string
    end
  end
end

defmodule Logflare.Repo.Migrations.AddBigqueryReservationsToEndpointQueries do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :bigquery_reservations, :text
    end
  end
end

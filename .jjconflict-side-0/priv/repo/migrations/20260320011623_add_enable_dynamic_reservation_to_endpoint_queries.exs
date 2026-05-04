defmodule Logflare.Repo.Migrations.AddEnableDynamicReservationToEndpointQueries do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add(:enable_dynamic_reservation, :boolean, default: false, null: false)
    end
  end
end

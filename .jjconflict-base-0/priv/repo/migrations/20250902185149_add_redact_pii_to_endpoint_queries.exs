defmodule Logflare.Repo.Migrations.AddRedactPiiToEndpointQueries do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add(:redact_pii, :boolean, default: false, null: false)
    end
  end
end

defmodule Logflare.Repo.Migrations.AddEndpointQueryDescriptionColumn do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add(:description, :string)
    end
  end
end

defmodule Logflare.Repo.Migrations.AlterEndpointQueryDescriptionToText do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      modify :description, :text, from: :string
    end
  end
end

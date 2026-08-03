defmodule Logflare.Repo.Migrations.AddLanguageColumnToEndpointQuery do
  use Ecto.Migration

  def change do

    alter table(:endpoint_queries) do
      add(:language, :string)
    end
  end
end

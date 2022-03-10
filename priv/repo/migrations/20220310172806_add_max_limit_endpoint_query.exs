defmodule Logflare.Repo.Migrations.AddMaxLimitEndpointQuery do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :max_limit, :integer, default: 1_000, nullable: false
    end
  end
end

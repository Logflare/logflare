defmodule Logflare.Repo.Migrations.ConfigurableEndpointCaching do
  use Ecto.Migration

  def change do
    alter table(:endpoint_queries) do
      add :cache_duration_seconds, :integer, default: 3_600
      add :proactive_requerying_seconds, :integer, default: 1_800
    end
  end
end

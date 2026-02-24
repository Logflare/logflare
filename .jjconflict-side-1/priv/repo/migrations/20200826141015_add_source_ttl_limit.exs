defmodule Logflare.Repo.Migrations.AddSourceTtlLimit do
  use Ecto.Migration

  def change do
    alter table(:plans) do
      add :limit_source_ttl, :bigint, default: :timer.hours(72), nullable: false
    end
  end
end

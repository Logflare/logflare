defmodule Logflare.Repo.Migrations.AddPlanType do
  use Ecto.Migration

  def change do
    alter table(:plans) do
      add :type, :string, default: "standard", nullable: false
    end
  end
end

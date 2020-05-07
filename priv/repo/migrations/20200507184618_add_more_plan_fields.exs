defmodule Logflare.Repo.Migrations.AddMorePlanFields do
  use Ecto.Migration

  def change do
    alter table(:plans) do
      add :period, :string
      add :price, :integer
    end
  end
end

defmodule Logflare.Repo.Migrations.CreatePlans do
  use Ecto.Migration

  def change do
    create table(:plans) do
      add :name, :string
      add :stripe_id, :string

      timestamps()
    end
  end
end

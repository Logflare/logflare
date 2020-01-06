defmodule Logflare.Repo.Migrations.AddCompanyTeamNames do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add(:company, :string)
      add(:team, :string)
    end
  end
end

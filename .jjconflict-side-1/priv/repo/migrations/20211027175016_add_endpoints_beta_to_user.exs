defmodule Logflare.Repo.Migrations.AddEndpointsBetaToUser do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :endpoints_beta, :boolean, default: false
    end
  end
end

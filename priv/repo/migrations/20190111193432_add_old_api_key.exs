defmodule Logflare.Repo.Migrations.AddOldApiKey do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :old_api_key, :string
    end
  end
end

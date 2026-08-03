defmodule Logflare.Repo.Migrations.AddTokenFieldToBackendsTable do
  use Ecto.Migration

  def change do

    alter table(:backends) do
      add :token, :uuid, null: false
    end
  end
end

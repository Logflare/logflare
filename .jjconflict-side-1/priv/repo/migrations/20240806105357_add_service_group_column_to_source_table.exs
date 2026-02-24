defmodule Logflare.Repo.Migrations.AddServiceGroupColumnToSourceTable do
  use Ecto.Migration

  def change do

    alter table(:sources) do
      add :service_name, :string
    end
  end
end

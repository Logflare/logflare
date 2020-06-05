defmodule Logflare.Repo.Migrations.AddSourceSchema do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :schema, :binary
    end
  end
end

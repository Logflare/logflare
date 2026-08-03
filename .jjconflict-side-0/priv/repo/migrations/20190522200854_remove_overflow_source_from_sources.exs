defmodule Logflare.Repo.Migrations.RemoveOverflowSourceFromSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      remove :overflow_source, references(:sources, column: :token, type: :uuid)
    end
  end
end

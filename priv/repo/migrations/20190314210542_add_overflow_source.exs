defmodule Logflare.Repo.Migrations.AddOverflowSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:overflow_source, references(:sources, column: :token, type: :uuid))
    end
  end
end

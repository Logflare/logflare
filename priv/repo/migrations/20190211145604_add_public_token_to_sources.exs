defmodule Logflare.Repo.Migrations.AddPublicTokenToSources do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:public_token, :string)
    end
  end
end

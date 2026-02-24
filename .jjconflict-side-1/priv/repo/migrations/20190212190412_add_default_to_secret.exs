defmodule Logflare.Repo.Migrations.AddDefaultToSecret do
  use Ecto.Migration

  def change do
    alter table(:oauth_applications) do
      modify(:secret, :string, null: false, default: "")
    end
  end
end

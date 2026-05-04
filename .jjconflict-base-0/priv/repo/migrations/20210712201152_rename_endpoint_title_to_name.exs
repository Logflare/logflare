defmodule Logflare.Repo.Migrations.RenameEndpointTitleToName do
  use Ecto.Migration

  def change do
    rename table(:endpoint_queries), :title, to: :name
  end
end

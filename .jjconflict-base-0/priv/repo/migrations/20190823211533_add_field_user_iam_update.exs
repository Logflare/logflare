defmodule Logflare.Repo.Migrations.AddFieldUserIamUpdate do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add(:valid_google_account, :boolean)
    end
  end
end

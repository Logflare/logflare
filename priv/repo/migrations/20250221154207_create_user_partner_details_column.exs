defmodule Logflare.Repo.Migrations.CreateUserPartnerDetailsColumn do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :partner_details, :map
      add :partner_id, references(:partners), null: true, on_delete: :delete_all
    end
  end
end

defmodule Logflare.Repo.Migrations.CreateUserPartnerDetailsColumn do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :partner_upgraded, :boolean, default: false
      add :partner_id, references(:partners), null: true, on_delete: :delete_all
    end
  end
end

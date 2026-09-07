defmodule Logflare.Repo.Migrations.AddCascadeDeleteForPartnerUsersPartnerId do
  use Ecto.Migration

  def change do
    alter table(:partner_users) do
      modify :partner_id, references(:partners, on_delete: :delete_all),
        from: references(:partners)
    end

    alter table(:users) do
      modify :partner_id, references(:partners, on_delete: :delete_all),
        from: references(:partners)
    end
  end
end

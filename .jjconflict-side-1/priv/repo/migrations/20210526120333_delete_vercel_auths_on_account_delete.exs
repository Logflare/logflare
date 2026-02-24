defmodule Logflare.Repo.Migrations.DeleteVercelAuthsOnAccountDelete do
  use Ecto.Migration

  def change do
    alter table(:vercel_auths) do
      modify :user_id, references(:users, on_delete: :delete_all),
        from: references(:users)
    end
  end
end

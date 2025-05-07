defmodule Logflare.Repo.Migrations.UsersTableHasManyDeleteAll do
  use Ecto.Migration

  def change do
    alter table("billing_counts") do
      modify(:user_id, references(:users, on_delete: :delete_all),
        from: references(:users, on_delete: :nothing)
      )
    end

    alter table("endpoint_queries") do
      modify(:user_id, references(:users, on_delete: :delete_all),
        from: references(:users, on_delete: :nothing)
      )
    end

    alter table("alert_queries") do
      modify(:user_id, references(:users, on_delete: :delete_all),
        from: references(:users, on_delete: :nothing)
      )
    end
  end
end

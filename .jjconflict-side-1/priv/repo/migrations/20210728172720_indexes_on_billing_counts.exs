defmodule Logflare.Repo.Migrations.IndexesOnBillingCounts do
  use Ecto.Migration

  def change do
    drop table(:billing_counts)
    create table(:billing_counts) do
      add :node, :string
      add :count, :integer
      add :user_id, references(:users, on_delete: :delete_all)
      add :source_id, references(:sources, on_delete: :nothing)

      timestamps()
    end

    create index(:billing_counts, [:user_id])
    create index(:billing_counts, [:source_id])
    create index(:billing_counts, [:inserted_at])
  end
end

defmodule Logflare.Repo.Migrations.CreateBillingCounts do
  use Ecto.Migration

  def change do
    create table(:billing_counts) do
      add :node, :string
      add :count, :integer
      add :user_id, references(:users, on_delete: :delete_all)
      add :source_id, references(:sources, on_delete: :nothing)

      timestamps()
    end

    create index(:billing_counts, [:user_id])
    create index(:billing_counts, [:source_id])
  end
end

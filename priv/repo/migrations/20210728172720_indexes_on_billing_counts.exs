defmodule Logflare.Repo.Migrations.IndexesOnBillingCounts do
  use Ecto.Migration

  def change do
    create index(:billing_counts, [:inserted_at])
  end
end

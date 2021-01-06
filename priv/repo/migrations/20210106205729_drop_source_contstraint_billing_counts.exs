defmodule Logflare.Repo.Migrations.DropSourceContstraintBillingCounts do
  use Ecto.Migration

  def change do
    drop constraint("billing_counts", "billing_counts_source_id_fkey")
  end
end

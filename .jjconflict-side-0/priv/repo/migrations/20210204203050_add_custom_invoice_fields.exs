defmodule Logflare.Repo.Migrations.AddCustomInvoiceFields do
  use Ecto.Migration

  def change do
    alter table(:billing_accounts) do
        add :custom_invoice_fields, {:array, :map}, default: [], nullable: false
    end
  end
end

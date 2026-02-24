defmodule Logflare.Repo.Migrations.MakeInvoiceFieldsNotNullable do
  use Ecto.Migration

  def change do
    alter table(:billing_accounts) do
        modify :custom_invoice_fields, {:array, :map}, default: [], null: false
    end
  end
end

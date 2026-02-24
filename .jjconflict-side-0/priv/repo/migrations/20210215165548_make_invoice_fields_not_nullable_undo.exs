defmodule Logflare.Repo.Migrations.MakeInvoiceFieldsNotNullableUndo do
  use Ecto.Migration

  def change do
    alter table(:billing_accounts) do
        modify :custom_invoice_fields, {:array, :map}, default: [], null: true
    end
  end
end

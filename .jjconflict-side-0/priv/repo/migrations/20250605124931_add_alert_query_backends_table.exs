defmodule Logflare.Repo.Migrations.AddAlertQueryBackendsTable do
  use Ecto.Migration

  def change do
    create table("alert_queries_backends") do
      add :alert_query_id, references("alert_queries", on_delete: :delete_all)
      add :backend_id, references("backends", on_delete: :delete_all)
    end

    create unique_index("alert_queries_backends", [:alert_query_id, :backend_id])
  end
end

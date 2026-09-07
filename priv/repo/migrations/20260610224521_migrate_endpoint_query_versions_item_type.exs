defmodule Logflare.Repo.Migrations.MigrateEndpointQueryVersionsItemType do
  use Ecto.Migration

  @index_name :versions_queries_item_id_version_number_index

  def up do
    drop_version_number_index()
    execute("UPDATE versions SET item_type = 'EndpointQuery' WHERE item_type = 'Query'")
    create_version_number_index()
  end

  def down do
    drop_version_number_index()
    execute("UPDATE versions SET item_type = 'Query' WHERE item_type = 'EndpointQuery'")
    create_version_number_index()
  end

  defp drop_version_number_index do
    execute("DROP INDEX IF EXISTS #{@index_name}")
  end

  defp create_version_number_index do
    create unique_index(:versions, [:item_type, :item_id, "((meta->>'version_number')::integer)"],
             where: "meta->>'version_number' IS NOT NULL",
             name: @index_name
           )
  end
end

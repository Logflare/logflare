defmodule Logflare.Repo.Migrations.UpdateEndpointQueryLanguageDefaultValue do
  use Ecto.Migration

  def change do
    execute(fn ->
      # set all current existing endpoints
      Logflare.Repo.update_all("endpoint_queries", set: [language: "bq_sql"])
    end, fn ->
      nil
    end)

    alter table("endpoint_queries") do
      modify :language, :string, null: false, from: {:string, null: true}
    end
  end
end

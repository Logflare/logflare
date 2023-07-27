defmodule Logflare.Repo.Migrations.UpdateEndpointQueryLanguageDefaultValue do
  use Ecto.Migration

  def up do
    # set all current values
    Logflare.Repo.update_all("endpoint_queries", set: [language: :bq_sql])
  end
end

defmodule Logflare.EctoSQLUnboxedHelpers do
  alias Logflare.Repo

  def truncate_all() do
    tables = [
      "users",
      "sources",
      "source_schemas",
      "rules",
      "plans",
      "billing_accounts",
      "billing_counts",
      "saved_searches",
      "saved_search_counters",
      "team_users",
      "teams"
    ]

    for table <- tables do
      Repo.query!("TRUNCATE #{table} CASCADE;")
    end
  end
end

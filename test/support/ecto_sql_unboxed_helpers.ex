defmodule Logflare.EctoSQLUnboxedHelpers do
  alias Logflare.Repo
  alias Logflare.Mnesia
  alias Logflare.Changefeeds

  @tables [
    "users",
    "sources",
    "source_schemas",
    "rules",
    # "plans",
    "billing_accounts",
    "billing_counts",
    "saved_searches",
    # "saved_search_counters",
    "team_users",
    "teams",
    "log_events"
  ]

  def truncate_all() do
    Repo.query!("TRUNCATE #{Enum.join(@tables, ", ")} CASCADE;", [])

    truncate_all(:mnesia)
  end

  def truncate_all(:mnesia) do
    for t <- @tables, do: {:atomic, :ok} = Mnesia.clear_table(String.to_atom(t))
  end
end

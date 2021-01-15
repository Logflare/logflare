defmodule Logflare.MemoryRepo do
  use Ecto.Repo,
    otp_app: :logflare,
    adapter: Ecto.Adapters.Mnesia

  def tables() do
    for config <- Application.get_env(:logflare, Logflare.MemoryRepo)[:tables] do
      case config do
        {k, v} -> {k, Module.concat(Logflare, v)}
        {k, v, opts} -> {k, Module.concat(Logflare, v), opts}
      end
    end
  end

  def tables_no_sync() do
    for {k, v} <- Application.get_env(:logflare, Logflare.MemoryRepo)[:tables_no_sync] do
      {k, Module.concat(Logflare, v)}
    end
  end

  def table_to_schema(table) do
    Map.fetch!(Enum.into(tables, Map.new), table)
  end

  def list_changefeeds() do
    for {table, _} <- tables() do
      "#{table}_changefeed"
    end
  end
end

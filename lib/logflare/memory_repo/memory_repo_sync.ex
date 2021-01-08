defmodule Logflare.MemoryRepo.Sync do
  @moduledoc """
  Synchronized Repo data with MemoryRepo data for
  """
  use Logflare.Commons
  use GenServer
  alias Logflare.EctoSchemaReflection
  import Ecto.Query, warn: false
  require Logger

  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []}
    }
  end

  def start_link(args \\ %{}, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(init_arg) do
    MemoryRepo.Migrations.run()
    run()
    {:ok, init_arg}
  end

  def run() do
    sync_table(Team)
    sync_table(TeamUser)
    sync_table(User)
    sync_table(Source)
    sync_table(Rule)
    sync_table(SavedSearch)
  end

  def sync_table(schema) do
    for x <- Repo.all(schema) do
      {:ok, _} = MemoryRepo.insert(x)
    end

    Logger.debug("Synced repo for #{schema} schema")

    :ok
  end
end

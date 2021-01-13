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
    validate_all_triggers_exist()
    validate_all_changefeed_changesets_exists()
    sync_all()
    {:ok, init_arg}
  end

  def validate_all_changefeed_changesets_exists() do
    for {table, schema} <- MemoryRepo.tables() do
      unless schema.__info__(:functions)[:changefeed_changeset] do
        throw("Module #{schema} doesn't implement changefeed_changeset")
      end
    end
  end

  def validate_all_triggers_exist() do
    %{rows: rows} =
      Repo.query!("""
      SELECT
        event_object_table                                              AS table_name,
        trigger_name                                                    AS trigger_name,
        string_agg(event_manipulation, ',' ORDER BY event_manipulation) AS event,
        action_timing                                                   AS timing,
        action_statement                                                AS definition
      FROM information_schema.triggers
      WHERE event_object_schema = 'public'
      AND trigger_schema = 'public'
      GROUP BY 1, 2, 4, 5
      """)

    result =
      for [table_name, trigger_name, event, timing, definition] <- rows do
        %{
          "definition" => definition,
          "event" => event,
          "table_name" => table_name,
          "timing" => timing,
          "trigger_name" => trigger_name
        }
      end
      |> Enum.sort()

    expected =
      for {table, _schema} <- MemoryRepo.tables() do
        %{
          "definition" => "EXECUTE FUNCTION changefeed_notify()",
          "event" => "DELETE,INSERT,UPDATE",
          "table_name" => table,
          "timing" => "AFTER",
          "trigger_name" => table <> "_changefeed_trigger"
        }
      end
      |> Enum.sort()

    compared = result -- expected

    unless Enum.empty?(compared) do
      throw("Not all changefeed triggers exist: #{inspect(compared)}")
    end
  end

  def sync_all() do
    for {_table, schema} <- MemoryRepo.tables() do
      sync_table(schema)
    end
  end

  def sync_table(schema) do
    for x <- Repo.all(schema) |> replace_assocs_with_nils(schema) do
      {:ok, _} = MemoryRepo.insert(x)
    end

    Logger.debug("Synced repo for #{schema} schema")

    :ok
  end

  def replace_assocs_with_nils(xs, schema) do
    for x <- xs do
      for af <- EctoSchemaReflection.associations(schema), reduce: x do
        acc ->
          case schema.__schema__(:association, af) do
            %Ecto.Association.BelongsTo{cardinality: :one} -> Map.replace!(acc, af, nil)
            %Ecto.Association.Has{cardinality: :one} -> Map.replace!(acc, af, nil)
            %Ecto.Association.Has{cardinality: :many} -> Map.replace!(acc, af, [])
          end
      end
    end
  end
end

defmodule Logflare.MemoryRepo.Sync do
  @moduledoc """
  Synchronized Repo data with MemoryRepo data for
  """
  use Logflare.Commons
  use GenServer
  alias Logflare.EctoSchemaReflection
  alias Logflare.Changefeeds
  alias Logflare.Changefeeds.ChangefeedSubscription
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
    sync_all_changefeed_tables()
    {:ok, init_arg}
  end

  def validate_all_changefeed_changesets_exists() do
    for %{schema: schema} <- Changefeeds.list_changefeed_subscriptions() do
      unless EctoSchemaReflection.changefeed_changeset_exists?(schema) do
        throw("Error: #{schema} doesn't implement changefeed_changeset")
      end
    end
  end

  def validate_all_triggers_exist() do
    in_db_triggers =
      from("information_schema.triggers")
      |> where([t], t.event_object_schema == "public" and t.trigger_schema == "public")
      |> select([t],
        table_name: t.event_object_table,
        trigger_name: t.trigger_name,
        event:
          fragment("string_agg(?, ',' ORDER BY ?)", t.event_manipulation, t.event_manipulation),
        timing: t.action_timing,
        definition: t.action_statement
      )
      |> group_by([t], [1, 2, 4, 5])
      |> Repo.all()

    events = "DELETE,INSERT,UPDATE"
    timing = "AFTER"

    expected =
      Changefeeds.list_changefeed_subscriptions()
      |> Enum.map(fn
        %ChangefeedSubscription{table: table, id_only: id_only} = chgsub ->
          definition =
            if id_only do
              {"EXECUTE FUNCTION changefeed_id_only_notify()"}
            else
              {"EXECUTE FUNCTION changefeed_notify()"}
            end

          %{
            :definition => definition,
            :event => events,
            :table_name => table,
            :timing => timing,
            :trigger_name => Changefeeds.trigger_name(chgsub)
          }
      end)

    compared = expected -- in_db_triggers

    unless Enum.empty?(compared) do
      compared_string =
        for %{"table" => table, "trigger_name" => trigger_name} <- compared,
            do: "#{trigger_name} for #{table} table \n"

      throw("
      The following triggers don't exist or their definition doesn't match the expected: \n
      #{compared_string}
      ")
    end
  end

  def sync_all_changefeed_tables() do
    for chgf <- Changefeeds.list_changefeed_subscriptions() do
      sync_table(chgf)
    end
  end

  def sync_table(%ChangefeedSubscription{schema: schema}) do
    virtual_schema = Module.concat(schema, Virtual)
    loaded? = Code.ensure_loaded?(virtual_schema)

    for x <- Repo.all(schema) |> replace_assocs_with_nils(schema) do
      {:ok, struct} = MemoryRepo.insert(x)

      if loaded? do
        virtual_struct =
          struct(
            virtual_schema,
            %{schema.compute_virtual_fields(struct) | id: struct.id} |> Map.to_list()
          )

        {:ok, _} =
          MemoryRepo.insert(virtual_struct, on_conflict: :replace_all, conflict_target: :id)
      end
    end

    Logger.debug("Synced memory repo for #{schema} schema")

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

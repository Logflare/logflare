defmodule Logflare.MemoryRepo.Migrations do
  use Logflare.Commons
  alias Logflare.Changefeeds.ChangefeedSubscription
  alias Logflare.EctoSchemaReflection
  use GenServer

  def start_link(args \\ %{}, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(args) do
    run()
    {:ok, args}
  end

  def run() do
    for %ChangefeedSubscription{schema: schema, table: table} <-
          Changefeeds.list_changefeed_subscriptions() do
      # delete_table_for_schema(schema)
      create_table_for_schema(schema)
    end

    for {table, schema} <- Changefeeds.tables() do
      create_table_for_schema(schema)
    end

    for %{schema: schema} <- Changefeeds.list_changefeed_subscriptions() do
      create_table_for_schema_virtual_fields(schema)
    end
  end

  def delete_table_for_schema(schema) do
    :mnesia.delete_table(EctoSchemaReflection.source(schema))
  end

  def create_table_for_schema(schema) do
    attributes =
      EctoSchemaReflection.fields_no_embeds(schema) ++
        EctoSchemaReflection.embeds(schema)

    table = schema |> EctoSchemaReflection.source() |> String.to_atom()

    {:atomic, :ok} =
      :mnesia.create_table(table,
        ram_copies: [node()],
        record_name: schema,
        attributes: attributes,
        type: :ordered_set
      )
  end

  def create_table_for_schema_virtual_fields(schema) do
    attributes = EctoSchemaReflection.virtual_fields(schema)
    virtual_schema = Module.concat(schema, Virtual)

    if not Enum.empty?(attributes) do
      table = :"#{EctoSchemaReflection.source(schema)}_virtual"

      {:atomic, :ok} =
        :mnesia.create_table(table,
          ram_copies: [node()],
          record_name: virtual_schema,
          attributes: attributes ++ [:id],
          type: :ordered_set
        )
    end
  end
end

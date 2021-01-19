defmodule Logflare.MemoryRepo.Migrations do
  use Logflare.Commons
  alias Logflare.EctoSchemaReflection

  def run() do
    for {table, schema} <- MemoryRepo.tables() ++ MemoryRepo.tables_no_sync() do
      create_table_from_schema(:"#{table}", schema)
    end
    for %{schema: schema} <- Changefeeds.list_changefeed_subscriptions() do
      create_table_for_schema_virtual_fields(schema)
    end
  end
  end

  def create_table_from_schema(ecto_table, schema) do
    attributes =
      EctoSchemaReflection.fields_no_embeds(schema) ++
        EctoSchemaReflection.embeds(schema)

    :mnesia.create_table(ecto_table,
      ram_copies: [node()],
      record_name: schema,
      attributes: Enum.uniq(attributes),
      type: :ordered_set
    )
  end
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

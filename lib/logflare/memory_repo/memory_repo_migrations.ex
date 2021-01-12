defmodule Logflare.MemoryRepo.Migrations do
  use Logflare.Commons
  alias Logflare.EctoSchemaReflection

  def run() do
    for {table, schema} <- MemoryRepo.tables() do
      create_table_from_schema(:"#{table}", schema)
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

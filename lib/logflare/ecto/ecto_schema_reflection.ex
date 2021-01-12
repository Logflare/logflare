defmodule Logflare.EctoSchemaReflection do
  def embeds(schema) do
    get(schema, :embeds)
  end

  def fields_no_embeds(schema) do
    get(schema, :fields) -- embeds(schema)
  end

  def associations(schema) do
    get(schema, :associations)
  end

  defp get(schema, key) do
    schema.__schema__(key)
  end

  def columns() do
  end
end

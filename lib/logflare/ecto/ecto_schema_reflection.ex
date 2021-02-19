defmodule Logflare.EctoSchemaReflection do
  @spec embeds(module() | struct()) :: [atom]
  def embeds(struct) when is_struct(struct), do: get_for_struct(struct, :embeds)
  def embeds(schema), do: get(schema, :embeds)

  @spec fields(module()) :: [atom]
  def fields(schema), do: get(schema, :fields)

  @spec fields_no_embeds(module() | struct()) :: [atom]
  def fields_no_embeds(schema) when is_atom(schema) do
    get(schema, :fields) -- embeds(schema)
  end

  def fields_no_embeds(struct) when is_struct(struct) do
    get_for_struct(struct, :fields) -- embeds(struct)
  end

  @spec associations(module() | struct()) :: [atom]
  def associations(module) when is_struct(module), do: get_for_struct(module, :associations)
  def associations(schema), do: get(schema, :associations)

  @spec source(module()) :: String.t()
  def source(schema) do
    get(schema, :source)
  end

  def functions(schema) do
    get(schema, :functions)
  end

  @spec changefeed_changeset_exists?(module()) :: boolean()
  def changefeed_changeset_exists?(schema) do
    functions = schema.__info__(:functions)

    {:changefeed_changeset, 2} in functions
  end

  def virtual_field_type(schema, field) when is_atom(field) do
    Map.get(schema.__changeset__, field)
  end

  @spec virtual_fields(module()) :: [atom]
  def virtual_fields(schema) do
    schema.__changeset__
    |> Map.drop(fields(schema) ++ associations(schema))
    |> Map.keys()
  end

  defp get(schema, key) do
    schema.__schema__(key)
  end

  defp get_for_struct(struct, key) do
    %schema{} = struct
    get(schema, key)
  end
end

defmodule Logflare.BigQuery.TableSchemaBuilder do
  require Logger
  alias GoogleApi.BigQuery.V2.Model
  alias Model.TableFieldSchema, as: TFS

  @doc """
  Builds table schema from event metadata and prev schema.

  Arguments:

  * metadata: event metadata
  * old_schema: existing Model.TableFieldSchema,

  Accepts both metadata map and metadata map wrapped in a list.
  """
  @spec build_table_schema([map], TFS.t()) :: TFS.t()
  def build_table_schema([metadata], old_schema) do
    build_table_schema(metadata, old_schema)
  end

  @spec build_table_schema(map, TFS.t()) :: TFS.t()
  def build_table_schema(metadata, %{fields: old_fields}) do
    old_metadata_schema = Enum.find(old_fields, &(&1.name == "metadata")) || %{}

    %Model.TableSchema{
      fields: [
        %TFS{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        build_metadata_fields_schemas(metadata, old_metadata_schema)
      ]
    }
    |> deep_sort_by_fields_name()
  end

  @spec build_metadata_fields_schemas(map, TFS.t()) :: TFS.t()
  defp build_metadata_fields_schemas(metadata, old_metadata_schema) do
    new_metadata_schema = build_fields_schemas({"metadata", metadata})

    old_metadata_schema
    # DeepMerge resolver is implemented for Model.TableFieldSchema structs
    |> DeepMerge.deep_merge(new_metadata_schema)
  end

  @spec build_fields_schemas({String.t(), map}) :: TFS.t()
  defp build_fields_schemas({params_key, params_val}) when is_map(params_val) do
    %TFS{
      description: nil,
      mode: "REPEATED",
      name: params_key,
      type: "RECORD",
      fields: Enum.map(params_val, &build_fields_schemas/1)
    }
  end

  defp build_fields_schemas({params_key, params_value}) do
    case to_schema_type(params_value) do
      "ARRAY" ->
        %TFS{
          name: params_key,
          type: "STRING",
          mode: "REPEATED"
        }

      type ->
        %TFS{
          name: params_key,
          type: type,
          mode: "NULLABLE"
        }
    end
  end

  @spec deep_sort_by_fields_name(TFS.t()) :: TFS.t()
  def deep_sort_by_fields_name(%{fields: nil} = schema), do: schema

  def deep_sort_by_fields_name(%{fields: fields} = schema) when is_list(fields) do
    sorted_fields =
      fields
      |> Enum.sort_by(& &1.name)
      |> Enum.map(&deep_sort_by_fields_name/1)

    %{schema | fields: sorted_fields}
  end

  defp to_schema_type(literal_value) when is_map(literal_value), do: "RECORD"
  defp to_schema_type(literal_value) when is_integer(literal_value), do: "INTEGER"
  defp to_schema_type(literal_value) when is_binary(literal_value), do: "STRING"
  defp to_schema_type(literal_value) when is_boolean(literal_value), do: "BOOLEAN"
  defp to_schema_type(literal_value) when is_list(literal_value), do: "ARRAY"

  defimpl DeepMerge.Resolver, for: Model.TableFieldSchema do
    @doc """
    Implements merge for schema key conflicts.
    Overwrites fields schemas that are present BOTH in old and new TFS structs and keeps fields schemas present ONLY in old.
    """
    def resolve(
          %TFS{fields: oldfs} = old,
          %TFS{fields: newfs} = new,
          resolver
        )
        when is_list(oldfs) and is_list(newfs) do
      new_field_schemas_names = Enum.map(new.fields, & &1.name)

      only_old_field_schemas =
        for ofs <- old.fields, ofs.name not in new_field_schemas_names, do: ofs

      new_with_all_field_schemas =
        new
        |> Map.from_struct()
        |> Map.put(:fields, new.fields ++ only_old_field_schemas)

      Map.merge(old, new_with_all_field_schemas, resolver)
    end

    def resolve(old, new, resolver) when is_map(new) do
      Map.merge(old, new, resolver)
    end
  end
end

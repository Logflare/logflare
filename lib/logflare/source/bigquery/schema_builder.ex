defmodule Logflare.Source.BigQuery.SchemaBuilder do
  @moduledoc false
  require Logger
  alias GoogleApi.BigQuery.V2.Model
  alias Model.TableFieldSchema, as: TFS
  import Logflare.Google.BigQuery.SchemaUtils, only: [deep_sort_by_fields_name: 1]
  alias Logflare.BigQuery.SchemaTypes

  @doc """
  Builds table schema from event metadata and prev schema.

  Arguments:

  * metadata: event metadata
  * old_schema: existing Model.TableFieldSchema,

  Accepts both metadata map and metadata map wrapped in a list.

  By default, will generate 4 top-level fields:
  - metadata
  - id
  - timestamp
  - event_message

    iex> %TS{fields: fields} = SchemaBuilder.build_table_schema(%{}, @default_schema)
    iex> length(fields)
    4
    iex> fields |> Enum.map( &(&1.name)) |> Enum.sort()
    ["event_message", "id", "metadata", "timestamp"]

  The metadata field will ways be of `RECORD` type and `REPEATED` mode.
    iex> %TS{fields: fields} = SchemaBuilder.build_table_schema(%{}, @default_schema)
    iex> Enum.find(fields, &(&1.name == "metadata"))
    %TFS{name: "metadata", fields: [], type: "RECORD", mode: "REPEATED"}

  Metadata map will result in nested fields on the respective `fields` key on the `TableFieldSchema`.any()

  For nested string fields:

    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> "some thing"}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "STRING" }

  For nested integer fields:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> 1}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "INTEGER" }


  For nested float fields:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> 1.0}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "FLOAT" }

  For nested boolean fields:

    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> true}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "BOOL" }


  ### Maps

  For nested fields, the intermediate level will be a `RECORD`
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> 1.0}}, @default_schema)
    iex> b_schema = TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a")
    %TFS{ name: "a", mode: "REPEATED", type: "RECORD", fields: [b_schema] }

  When there is an array of maps, it results in the following:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> [
    ...>  %{"b1"=> "seomthing"},
    ...>  %{"b2"=> 1.0},
    ...>]}, @default_schema)
    iex> b1_schema =  TestUtils.get_bq_field_schema(schema, "metadata.a.b1")
    %TFS{ name: "b1", mode: "NULLABLE", type: "STRING" }
    iex> b2_schema =  TestUtils.get_bq_field_schema(schema, "metadata.a.b2")
    %TFS{ name: "b2", mode: "NULLABLE", type: "FLOAT" }
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a")
    %TFS{ name: "a", mode: "REPEATED", type: "RECORD", fields: [b1_schema, b2_schema] }

  Notice that for both cases, the `a` key is set to `REPEATED`

  ### Arrays
  For arrays fields, the mode will be repeated, and the array type set to the `:type` key:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> [1.0]}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    %TFS{ name: "b", mode: "REPEATED", type: "FLOAT" }

  Likewise, the same occurs for string arrays:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> ["something"]}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    %TFS{ name: "b", mode: "REPEATED", type: "STRING" }



  ### Empty Maps
  For empty maps, there will not be any inner fields created for the record created:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> %{}}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata.a.b")
    %TFS{fields: [], mode: "REPEATED", name: "b", type: "RECORD"}

  ### Exceptions
  There are certain cases where the inner field types are ambiguous and an error is raised.
  - Single nested arrays `[]`
  - Double nested arrays `[[]]`

    iex> func = &(fn -> SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> &1}}, @default_schema) end)
    iex> assert_raise ArgumentError, func.([])
    %ArgumentError{message: "errors were found at the given arguments:\\n\\n  * 1st argument: not a nonempty list\\n"}
    iex> assert_raise ArgumentError, func.([[]])
    %ArgumentError{message: "errors were found at the given arguments:\\n\\n  * 1st argument: not a nonempty list\\n"}

  """
  @spec build_table_schema([map], TFS.t()) :: TFS.t()
  def build_table_schema([metadata], old_schema) do
    build_table_schema(metadata, old_schema)
  end

  @spec build_table_schema(map, TFS.t()) :: TFS.t()
  def build_table_schema(metadata, %{fields: old_fields}) do
    old_metadata_schema = Enum.find(old_fields, &(&1.name == "metadata")) || %{}

    metadata_field = build_metadata_fields_schemas(metadata, old_metadata_schema)

    initial_table_schema()
    |> Map.update!(:fields, &Enum.concat(&1, [metadata_field]))
    |> deep_sort_by_fields_name()
  end

  def build_table_schema(metadata) do
    metadata_field = build_fields_schemas({"metadata", metadata})

    initial_table_schema()
    |> Map.update!(:fields, &Enum.concat(&1, [metadata_field]))
    |> deep_sort_by_fields_name()
  end

  def initial_table_schema() do
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
          name: "id",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        }
      ]
    }
  end

  @spec build_metadata_fields_schemas(map, TFS.t()) :: TFS.t()
  defp build_metadata_fields_schemas(metadata, old_metadata_schema) do
    new_metadata_schema = build_fields_schemas({"metadata", metadata})

    old_metadata_schema
    # DeepMerge resolver is implemented for Model.TableFieldSchema structs
    |> DeepMerge.deep_merge(new_metadata_schema)
  end

  defp build_fields_schemas({params_key, params_val}) when is_map(params_val) do
    %TFS{
      description: nil,
      mode: "REPEATED",
      name: params_key,
      type: "RECORD",
      fields: Enum.map(params_val, &build_fields_schemas/1)
    }
  end

  defp build_fields_schemas(maps) when is_list(maps) do
    maps
    |> Enum.reduce(%{}, &DeepMerge.deep_merge/2)
    |> Enum.reject(fn
      {_, v} when v == [] when v == %{} when v == [[]] -> true
      _ -> false
    end)
    |> Enum.map(&build_fields_schemas/1)
  end

  defp build_fields_schemas({params_key, params_value}) do
    case SchemaTypes.to_schema_type(params_value) do
      {"ARRAY", "RECORD"} ->
        %TFS{
          name: params_key,
          type: "RECORD",
          mode: "REPEATED",
          fields: build_fields_schemas(params_value)
        }

      {"ARRAY", inner_type} ->
        %TFS{
          name: params_key,
          type: inner_type,
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

  defimpl DeepMerge.Resolver, for: Model.TableFieldSchema do
    @doc """
    Implements merge for schema key conflicts.
    Overwrites fields schemas that are present BOTH in old and new TFS structs and keeps fields schemas present ONLY in old.
    """

    @spec resolve(TFS.t(), TFS.t(), fun) :: TFS.t()
    def resolve(old, new, _standard_resolver) do
      resolve(old, new)
    end

    @spec resolve(TFS.t(), TFS.t()) :: TFS.t()
    def resolve(
          %TFS{fields: old_fields},
          %TFS{fields: new_fields} = new_tfs
        )
        when is_list(old_fields)
        when is_list(new_fields) do
      # collect all names for new fields schemas
      new_fields_names = Enum.map(new_fields || [], & &1.name)

      # filter field schemas that are present only in old table field schema
      uniq_old_fs = for fs <- old_fields, fs.name not in new_fields_names, do: fs

      %{new_tfs | fields: resolve_list(old_fields, new_fields) ++ uniq_old_fs}
    end

    def resolve(_old, %TFS{} = new) do
      new
    end

    @spec resolve_list(list(TFS.t()), list(TFS.t())) :: list(TFS.t())
    def resolve_list(old_fields, new_fields)
        when is_list(old_fields)
        when is_list(new_fields) do
      for %TFS{} = new_field <- new_fields do
        old_fields
        |> maybe_find_with_name(new_field)
        |> resolve(new_field)
      end
    end

    @spec maybe_find_with_name(list(TFS.t()), TFS.t()) :: TFS.t() | nil
    def maybe_find_with_name(enumerable, %TFS{name: name}) do
      Enum.find(enumerable, &(&1.name === name))
    end
  end
end

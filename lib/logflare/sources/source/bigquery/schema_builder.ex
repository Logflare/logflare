defmodule Logflare.Sources.Source.BigQuery.SchemaBuilder do
  @moduledoc false

  import Logflare.Google.BigQuery.SchemaUtils, only: [deep_sort_by_fields_name: 1]

  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.BigQuery.SchemaTypes
  alias Model.TableFieldSchema, as: TFS

  @initial_table_schema %Model.TableSchema{
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

  @doc """
  Builds table schema from event metadata and prev schema.

  Arguments:

  * metadata: event metadata
  * old_schema: existing Model.TableFieldSchema,

  Accepts both metadata map and metadata map wrapped in a list.

  By default, will generate 3 top-level fields:
  - id
  - timestamp
  - event_message


    iex> %TS{fields: fields} = SchemaBuilder.build_table_schema(%{}, @default_schema)
    iex> length(fields)
    3
    iex> fields |> Enum.map( &(&1.name)) |> Enum.sort()
    ["event_message", "id", "timestamp"]

  However, all other top-level fields are recognized as well.

    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> "something"}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "metadata")
    nil
    iex> TestUtils.get_bq_field_schema(schema, "a")
    %TFS{name: "a", mode: "NULLABLE", type: "STRING"}


  The nested object fields will ways be of `RECORD` type and `REPEATED` mode.
    iex> %TS{fields: fields} = SchemaBuilder.build_table_schema(%{"a"=> %{}}, @default_schema)
    iex> Enum.find(fields, &(&1.name == "a"))
    %TFS{name: "a", fields: [], type: "RECORD", mode: "REPEATED"}

  Metadata map will result in nested fields on the respective `fields` key on the `TableFieldSchema`.any()

  For nested string fields:

    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> "some thing"}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "STRING" }

  For nested integer fields:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> 1}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "INTEGER" }


  For nested float fields:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> 1.0}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "FLOAT" }

  For nested boolean fields:

    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> true}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
    %TFS{ name: "b", mode: "NULLABLE", type: "BOOL" }


  ### Maps

  For nested fields, the intermediate level will be a `RECORD`
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> 1.0}}, @default_schema)
    iex> b_schema = TestUtils.get_bq_field_schema(schema, "a.b")
    iex> TestUtils.get_bq_field_schema(schema, "a")
    %TFS{ name: "a", mode: "REPEATED", type: "RECORD", fields: [b_schema] }

  When there is an array of maps, it results in the following:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> [
    ...>  %{"b1"=> "seomthing"},
    ...>  %{"b2"=> 1.0},
    ...>]}, @default_schema)
    iex> b1_schema =  TestUtils.get_bq_field_schema(schema, "a.b1")
    %TFS{ name: "b1", mode: "NULLABLE", type: "STRING" }
    iex> b2_schema =  TestUtils.get_bq_field_schema(schema, "a.b2")
    %TFS{ name: "b2", mode: "NULLABLE", type: "FLOAT" }
    iex> TestUtils.get_bq_field_schema(schema, "a")
    %TFS{ name: "a", mode: "REPEATED", type: "RECORD", fields: [b1_schema, b2_schema] }

  Notice that for both cases, the `a` key is set to `REPEATED`

  ### Arrays
  For arrays fields, the mode will be repeated, and the array type set to the `:type` key:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> [1.0]}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
    %TFS{ name: "b", mode: "REPEATED", type: "FLOAT" }

  Likewise, the same occurs for string arrays:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> ["something"]}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
    %TFS{ name: "b", mode: "REPEATED", type: "STRING" }



  ### Empty Maps
  For empty maps, there will not be any inner fields created for the record created:
    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> %{}}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
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
  @spec build_table_schema([map()] | map(), TFS.t()) :: TFS.t()

  def build_table_schema(params, %{fields: old_fields}) do
    initial_schema = initial_table_schema()
    old_fields_by_name = Map.new(old_fields, &{&1.name, &1})
    is_otel = otel_data?(params)

    new_fields =
      Enum.reduce(params, [], fn {param_key, param_value}, acc ->
        if protected_key?(param_key) do
          acc
        else
          prev_field_schema = Map.get(old_fields_by_name, param_key)
          new_field_schema = build_fields_schemas({param_key, param_value}, is_otel)

          [merge_field_schema(prev_field_schema, new_field_schema) | acc]
        end
      end)
      |> Enum.reverse()

    # reject old fields that are now included in the params
    unrejected_fields = Enum.reject(old_fields, &Map.has_key?(params, &1.name))

    updated_fields =
      (unrejected_fields ++ new_fields ++ initial_schema.fields)
      |> Enum.uniq_by(fn f -> f.name end)

    initial_schema
    |> Map.put(:fields, updated_fields)
    |> deep_sort_by_fields_name()
  end

  def initial_table_schema do
    @initial_table_schema
  end

  defp protected_key?("event_message"), do: true
  defp protected_key?("id"), do: true
  defp protected_key?("timestamp"), do: true
  defp protected_key?(_key), do: false

  defp build_fields_schemas({params_key, params_val}, _is_otel) when is_map(params_val) do
    %TFS{
      description: nil,
      mode: "REPEATED",
      name: params_key,
      type: "RECORD",
      fields: Enum.map(params_val, &build_fields_schemas(&1, false))
    }
  end

  defp build_fields_schemas(maps, _is_otel) when is_list(maps) do
    maps
    |> Enum.reduce(%{}, &merge_payload_maps/2)
    |> Enum.reject(fn
      {_, v} when v == [] when v == %{} when v == [[]] -> true
      _ -> false
    end)
    |> Enum.map(&build_fields_schemas(&1, false))
  end

  defp build_fields_schemas({params_key, params_value}, is_otel) do
    type = determine_type(params_key, params_value, is_otel)

    case type do
      {"ARRAY", "RECORD"} ->
        %TFS{
          name: params_key,
          type: "RECORD",
          mode: "REPEATED",
          fields: build_fields_schemas(params_value, false)
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

  defp determine_type(field_name, value, is_otel) do
    base_type = SchemaTypes.to_schema_type(value)

    if is_otel and field_name in ["start_time", "end_time"] and base_type == "INTEGER" do
      "TIMESTAMP"
    else
      base_type
    end
  end

  defp otel_data?(params) do
    Map.has_key?(params, "resource") and Map.has_key?(params, "scope")
  end

  defp merge_payload_maps(map, acc) when is_map(map) do
    Map.merge(acc, map, fn _key, left, right ->
      if is_map(left) and is_map(right) do
        merge_payload_maps(right, left)
      else
        right
      end
    end)
  end

  defp merge_payload_maps(_value, acc), do: acc

  defp merge_field_schema(nil, %TFS{} = new), do: new

  defp merge_field_schema(%TFS{fields: old_fields}, %TFS{fields: new_fields} = new)
       when is_list(old_fields) and is_list(new_fields) do
    old_fields_by_name = Map.new(old_fields, &{&1.name, &1})
    new_field_names = MapSet.new(new_fields, & &1.name)

    merged_new_fields =
      Enum.map(new_fields, fn %TFS{} = new_field ->
        old_fields_by_name
        |> Map.get(new_field.name)
        |> merge_field_schema(new_field)
      end)

    old_only_fields = Enum.reject(old_fields, &MapSet.member?(new_field_names, &1.name))

    %{new | fields: merged_new_fields ++ old_only_fields}
  end

  defp merge_field_schema(%TFS{fields: old_fields} = old, %TFS{fields: new_fields})
       when is_list(old_fields) or is_list(new_fields) do
    raise Protocol.UndefinedError, protocol: DeepMerge.Resolver, value: old
  end

  defp merge_field_schema(_old, %TFS{} = new), do: new
end

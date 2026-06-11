defmodule Logflare.Sources.Source.BigQuery.SchemaBuilder do
  @moduledoc false

  import Logflare.Google.BigQuery.SchemaUtils, only: [deep_sort_by_fields_name: 1]

  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.BigQuery.SchemaTypes
  alias Model.TableFieldSchema, as: TFS
  alias Model.TableSchema, as: TS

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
  @initial_fields_by_name Map.new(@initial_table_schema.fields, &{&1.name, &1})

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

  ### Nested arrays

  BigQuery has no array-of-arrays type, so a populated nested array cannot be
  represented as a column. Such a field is dropped from the schema (only that
  field — sibling fields are still built) rather than producing an invalid
  field schema:

    iex> schema = SchemaBuilder.build_table_schema(%{"a"=> %{"b"=> [["x"], ["y"]], "c"=> 1}}, @default_schema)
    iex> TestUtils.get_bq_field_schema(schema, "a.b")
    nil
    iex> TestUtils.get_bq_field_schema(schema, "a.c")
    %TFS{ name: "c", mode: "NULLABLE", type: "INTEGER" }

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
  @spec build_table_schema([map()] | map(), TS.t()) :: TS.t()
  def build_table_schema(params, old_schema) do
    {schema, _changed?} = build_table_schema_with_change(params, old_schema)

    schema
  end

  @spec build_table_schema_with_change([map()] | map(), TS.t()) :: {TS.t(), boolean()}
  def build_table_schema_with_change(params, %{fields: old_fields} = old_schema) do
    initial_schema = initial_table_schema()

    old_fields_by_name =
      Enum.reduce(old_fields, %{}, fn field, fields_by_name ->
        Map.put_new(fields_by_name, field.name, field)
      end)

    is_otel = otel_data?(params)

    {new_fields, changed?} =
      Enum.reduce(
        params,
        {[], false},
        &merge_param(&1, &2, old_fields_by_name, is_otel)
      )

    missing_initial_field? =
      Enum.any?(initial_schema.fields, &(not Map.has_key?(old_fields_by_name, &1.name)))

    changed? =
      changed? or missing_initial_field? or map_size(old_fields_by_name) != length(old_fields) or
        not fields_deeply_sorted_by_name?(old_fields) or
        old_schema != %{initial_schema | fields: old_fields}

    if changed? do
      new_fields = Enum.reverse(new_fields)
      updated_fields = updated_fields(old_fields, params, new_fields, initial_schema)
      schema = Map.put(initial_schema, :fields, updated_fields)
      schema = deep_sort_by_fields_name(schema)

      {schema, true}
    else
      {old_schema, false}
    end
  end

  defp updated_fields(old_fields, params, new_fields, initial_schema) do
    # reject old fields that are now included in the params
    unrejected_fields = Enum.reject(old_fields, &Map.has_key?(params, &1.name))
    field_names = MapSet.new(unrejected_fields ++ new_fields, & &1.name)

    missing_initial_fields =
      Enum.reject(initial_schema.fields, &MapSet.member?(field_names, &1.name))

    Enum.uniq_by(unrejected_fields ++ new_fields ++ missing_initial_fields, & &1.name)
  end

  def initial_table_schema do
    @initial_table_schema
  end

  defp merge_param(
         {param_key, _param_value},
         {new_fields, changed?},
         old_fields_by_name,
         _is_otel
       )
       when param_key in ["event_message", "id", "timestamp"] do
    initial_field = Map.fetch!(@initial_fields_by_name, param_key)
    old_field = Map.get(old_fields_by_name, param_key)

    {new_fields, changed? or is_nil(old_field) or old_field != initial_field}
  end

  defp merge_param(
         {param_key, param_value},
         {new_fields, changed?},
         old_fields_by_name,
         is_otel
       ) do
    prev_field_schema = Map.get(old_fields_by_name, param_key)

    case build_fields_schemas({param_key, param_value}, is_otel) do
      nil ->
        {new_fields, changed? or not is_nil(prev_field_schema)}

      new_field_schema ->
        {merged_field_schema, field_changed?} =
          merge_field_schema(prev_field_schema, new_field_schema)

        {[merged_field_schema | new_fields], changed? or field_changed?}
    end
  end

  defp build_fields_schemas({params_key, params_val}, _is_otel) when is_map(params_val) do
    %TFS{
      description: nil,
      mode: "REPEATED",
      name: params_key,
      type: "RECORD",
      fields:
        params_val
        |> Enum.map(&build_fields_schemas(&1, false))
        |> Enum.reject(&is_nil/1)
    }
  end

  defp build_fields_schemas(maps, _is_otel) when is_list(maps) do
    maps
    |> Enum.reduce(%{}, &DeepMerge.deep_merge/2)
    |> Enum.reject(fn
      {_, v} when v == [] when v == %{} when v == [[]] -> true
      _ -> false
    end)
    |> Enum.map(&build_fields_schemas(&1, false))
    |> Enum.reject(&is_nil/1)
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

      # BigQuery has no array-of-arrays type. A nested array yields a tuple
      # inner type here (e.g. {"ARRAY", {"ARRAY", "STRING"}}); drop the field
      # rather than emit an unrepresentable field schema.
      {"ARRAY", inner_type} when is_tuple(inner_type) ->
        nil

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

  defp merge_field_schema(nil, %TFS{} = new), do: {new, true}

  defp merge_field_schema(%TFS{fields: old_fields} = old, %TFS{fields: new_fields} = new)
       when is_list(old_fields) and is_list(new_fields) do
    {merged_fields, children_changed?} = merge_fields(old_fields, new_fields)
    parent_changed? = %{new | fields: old_fields} != old
    changed? = parent_changed? or children_changed?

    if changed? do
      {%{new | fields: merged_fields}, true}
    else
      {old, false}
    end
  end

  defp merge_field_schema(%TFS{fields: old_fields} = old, %TFS{fields: new_fields})
       when is_list(old_fields) or is_list(new_fields) do
    raise Protocol.UndefinedError, protocol: DeepMerge.Resolver, value: old
  end

  defp merge_field_schema(old, %TFS{} = new) do
    if old == new, do: {old, false}, else: {new, true}
  end

  defp merge_fields(old_fields, new_fields) do
    new_fields_by_name = Map.new(new_fields, &{&1.name, &1})

    {merged_old_fields, {remaining_new_fields, changed?}} =
      Enum.map_reduce(old_fields, {new_fields_by_name, false}, fn old_field,
                                                                  {remaining, changed?} ->
        case Map.pop(remaining, old_field.name) do
          {nil, remaining} ->
            {old_field, {remaining, changed?}}

          {new_field, remaining} ->
            {merged_field, field_changed?} = merge_field_schema(old_field, new_field)
            {merged_field, {remaining, changed? or field_changed?}}
        end
      end)

    added_fields =
      Enum.filter(new_fields, &Map.has_key?(remaining_new_fields, &1.name))

    {merged_old_fields ++ added_fields, changed? or added_fields != []}
  end

  defp fields_sorted_by_name?([]), do: true

  defp fields_sorted_by_name?([first_field | fields]),
    do: fields_sorted_by_name?(fields, first_field.name)

  defp fields_sorted_by_name?([], _previous_name), do: true

  defp fields_sorted_by_name?([field | fields], previous_name)
       when previous_name <= field.name,
       do: fields_sorted_by_name?(fields, field.name)

  defp fields_sorted_by_name?(_fields, _previous_name), do: false

  defp fields_deeply_sorted_by_name?(fields) do
    fields_sorted_by_name?(fields) and
      Enum.all?(fields, fn
        %TFS{fields: nested_fields} when is_list(nested_fields) ->
          fields_deeply_sorted_by_name?(nested_fields)

        %TFS{} ->
          true
      end)
  end
end

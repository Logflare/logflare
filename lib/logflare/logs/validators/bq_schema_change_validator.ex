defmodule Logflare.Logs.Validators.BigQuerySchemaChange do
  @moduledoc false
  alias Logflare.{Source, Sources}
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  @spec validate(%{log: atom | %{metadata: map}, source: Source.t()}) ::
          :ok | {:error, String.t()}
  def validate(%{log: log, source: %Source{} = source}) do
    schema = Sources.Cache.get_bq_schema(source)

    if valid?(log.metadata, schema) do
      :ok
    else
      {:error, message()}
    end
  end

  def valid?(metadata, existing_schema) do
    to_typemap(metadata) == to_typemap(existing_schema, from: :bigquery_schema)
  end

  def to_typemap(%TS{fields: fields} = schema, from: :bigquery_schema) when is_map(schema) do
    to_typemap(fields, from: :bigquery_schema)
  end

  def to_typemap(fields, from: :bigquery_schema) when is_list(fields) do
    fields
    |> Enum.map(fn
      %TFS{fields: fields, name: n, type: t} ->
        k = String.to_atom(n)

        v = %{t: bq_type_to_ex(t)}

        v =
          if fields do
            Map.put(v, :fields, to_typemap(fields, from: :bigquery_schema))
          else
            v
          end

        {k, v}
    end)
    |> Enum.into(Map.new())
  end

  def to_typemap(metadata) when is_map(metadata) do
    for {k, v} <- metadata, into: Map.new() do
      v =
        cond do
          match?(%DateTime{}, v) or
              match?(%NaiveDateTime{}, v) ->
            %{t: :datetime}

          is_list(v) ->
            %{
              t: :map,
              fields: Enum.reduce(v, %{}, &Map.merge(&2, to_typemap(&1)))
            }

          is_map(v) ->
            %{t: :map, fields: to_typemap(v)}

          true ->
            %{t: type_of(v)}
        end

      k =
        if is_atom(k) do
          k
        else
          String.to_existing_atom(k)
        end

      {k, v}
    end
  end

  def message() do
    "Incoming metadata is not compatible with existing schema"
  end

  def bq_type_to_ex("TIMESTAMP"), do: :datetime
  def bq_type_to_ex("RECORD"), do: :map
  def bq_type_to_ex("INTEGER"), do: :integer
  def bq_type_to_ex("STRING"), do: :string
  def bq_type_to_ex("BOOLEAN"), do: :boolean
  def bq_type_to_ex("ARRAY"), do: :list

  defp type_of(arg) when is_binary(arg), do: :string
  defp type_of(arg) when is_map(arg), do: :map
  defp type_of(arg) when is_list(arg), do: :list
  defp type_of(arg) when is_bitstring(arg), do: :bitstring
  defp type_of(arg) when is_float(arg), do: :float
  defp type_of(arg) when is_function(arg), do: :function
  defp type_of(arg) when is_integer(arg), do: :integer
  defp type_of(arg) when is_pid(arg), do: :pid
  defp type_of(arg) when is_port(arg), do: :port
  defp type_of(arg) when is_reference(arg), do: :reference
  defp type_of(arg) when is_tuple(arg), do: :tuple
  defp type_of(arg) when arg in [true, false], do: :boolean
  defp type_of(arg) when is_atom(arg), do: :atom
end

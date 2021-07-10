defmodule Logflare.Logs.Validators.BigQuerySchemaChange do
  @moduledoc false
  alias Logflare.LogEvent, as: LE
  alias Logflare.{Source, Sources}
  alias Logflare.Source.BigQuery.SchemaBuilder

  import Logflare.Google.BigQuery.SchemaUtils,
    only: [to_typemap: 1, to_typemap: 2, bq_schema_to_flat_typemap: 1]

  @spec validate(LE.t()) :: :ok | {:error, String.t()}
  def validate(%LE{body: body, source: %Source{} = source}) do
    schema = Sources.Cache.get_bq_schema(source)

    schema_flatmap =
      schema
      |> bq_schema_to_flat_typemap()

    new_schema_flatmap =
      SchemaBuilder.build_table_schema(body.metadata)
      |> bq_schema_to_flat_typemap()

    try do
      merge_flat_typemaps(schema_flatmap, new_schema_flatmap)
      :ok
    rescue
      e ->
        {:error, e.message}
    end
  end

  def merge_flat_typemaps(nil, new), do: new
  def merge_flat_typemaps(original, nil), do: original
  def merge_flat_typemaps(_, new) when new === %{}, do: new

  def merge_flat_typemaps(original, new) do
    Map.merge(original, new, fn k, v1, v2 ->
      if v1 != v2,
        do:
          raise(
            "Type error! Field `#{k}` has type of `#{v1}`. Incoming metadata has type of `#{v2}`."
          ),
        else: v2
    end)
  end
end

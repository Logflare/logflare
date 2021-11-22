defmodule Logflare.Logs.Validators.BigQuerySchemaChange do
  @moduledoc false
  alias Logflare.LogEvent, as: LE
  alias Logflare.{Source, SourceSchemas}
  alias Logflare.Source.BigQuery.SchemaBuilder

  import Logflare.Google.BigQuery.SchemaUtils

  @spec validate(LE.t()) :: :ok | {:error, String.t()}
  def validate(%LE{body: body, source: %Source{lock_schema: true} = source}) do
    :ok
  end

  @spec validate(LE.t()) :: :ok | {:error, String.t()}
  def validate(%LE{body: body, source: %Source{} = source}) do
    # Convert to a flat type map
    # We're missing the cache too much here.
    schema_flatmap =
      if source.id, do: SourceSchemas.Cache.get_source_schema_by(source_id: source.id), else: %{}

    # Convert to a flat type map
    metadata_flatmap =
      to_typemap(%{metadata: body.metadata})
      |> flatten_typemap()

    try_merge(metadata_flatmap, schema_flatmap)
  end

  def try_merge(metadata_flatmap, schema_flatmap) do
    try do
      merge_flat_typemaps(metadata_flatmap, schema_flatmap)
      :ok
    rescue
      e ->
        {:error, e.message}
    end
  end

  def merge_flat_typemaps(nil, original), do: original
  def merge_flat_typemaps(new, nil), do: new
  def merge_flat_typemaps(new, original) when new == %{}, do: original
  def merge_flat_typempas(new, original) when new == original, do: original

  def merge_flat_typemaps(new, original) do
    Map.merge(new, original, fn k, v1, v2 ->
      if v1 != v2,
        do:
          raise(
            "Type error! Field `#{k}` has type of `#{inspect(v2)}`. Incoming metadata has type of `#{inspect(v1)}`."
          ),
        else: v2
    end)
  end

  # Currently for tests. Change tests.
  def valid?(metadata, schema) do
    schema_flatmap = bq_schema_to_flat_typemap(schema)

    new_schema_flatmap =
      to_typemap(%{metadata: metadata})
      |> flatten_typemap()

    try do
      merge_flat_typemaps(new_schema_flatmap, schema_flatmap)
      true
    rescue
      _e ->
        false
    end
  end
end

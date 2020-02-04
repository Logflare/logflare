defmodule Logflare.Logs.Validators.BigQuerySchemaChange do
  @moduledoc false
  alias Logflare.LogEvent, as: LE
  alias Logflare.{Source, Sources}
  import Logflare.Google.BigQuery.SchemaUtils, only: [to_typemap: 1, to_typemap: 2]

  def validate(%LE{body: body, source: %Source{} = source}) do
    schema = Sources.Cache.get_bq_schema(source)

    if valid?(body.metadata, schema) do
      :ok
    else
      {:error, message()}
    end
  end

  def valid?(nil, _), do: true
  def valid?(_, nil), do: true
  def valid?(m, _) when m === %{}, do: true

  def valid?(metadata, existing_schema) do
    resolver = fn
      _, original, override when is_atom(original) and is_atom(override) ->
        if original != override, do: raise("type_error")

      _, _original, _override ->
        DeepMerge.continue_deep_merge()
    end

    existing_typemap = to_typemap(existing_schema, from: :bigquery_schema)

    existing_metadata_typemap =
      case existing_typemap do
        %{metadata: %{fields: _}} -> existing_typemap.metadata.fields
        _ -> %{}
      end

    new_metadata_typemap = to_typemap(metadata)

    try do
      DeepMerge.deep_merge(new_metadata_typemap, existing_metadata_typemap, resolver)
    rescue
      _e -> false
    else
      _ -> true
    end
  end

  def message() do
    "Incoming metadata is not compatible with existing schema"
  end
end

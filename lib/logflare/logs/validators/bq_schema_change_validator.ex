defmodule Logflare.Logs.Validators.BigQuerySchemaChange do
  @moduledoc false

  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.SourceSchemas
  alias Logflare.Sources.Source

  # Logflare injects "timestamp" as integer microseconds in LogEvent.mapper/1,
  # but the BQ schema types it as TIMESTAMP (:datetime). Skip at the top level
  # so the integer/datetime mismatch doesn't trip validation. "id" and
  # "event_message" are strings on both sides, so they don't need the skip.
  @skip_top_level_keys ~w(timestamp)

  @spec validate(LE.t(), Source.t()) :: :ok | {:error, String.t()}
  def validate(%LE{body: _body}, %Source{validate_schema: false}), do: :ok

  def validate(%LE{body: body}, %Source{} = source) do
    schema_flat_map =
      case source.id && SourceSchemas.Cache.get_source_schema_by(source_id: source.id) do
        %_{schema_flat_map: flat_map} when is_map(flat_map) -> flat_map
        _ -> %{}
      end

    check_body(body, schema_flat_map)
  end

  @spec valid?(map, map) :: boolean
  def valid?(body, schema) do
    schema_flat_map = SchemaUtils.bq_schema_to_flat_typemap(schema)
    check_body(body, schema_flat_map) == :ok
  end

  @spec check_body(map, map) :: :ok | {:error, String.t()}
  defp check_body(_body, schema_flat_map) when map_size(schema_flat_map) == 0, do: :ok

  defp check_body(body, schema_flat_map) when is_map(body) do
    walk_map(body, "", schema_flat_map)
    :ok
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp walk_map(map, prefix, schema_flat_map) do
    :maps.fold(&walk_entry/3, {prefix, schema_flat_map}, map)
  end

  defp walk_entry(_k, [], acc), do: acc
  defp walk_entry(_k, m, acc) when is_map(m) and map_size(m) == 0, do: acc
  defp walk_entry(_k, [[]], acc), do: acc
  defp walk_entry(_k, [m], acc) when is_map(m) and map_size(m) == 0, do: acc
  defp walk_entry(k, _v, {"", _} = acc) when k in @skip_top_level_keys, do: acc

  defp walk_entry(k, v, {prefix, schema_flat_map} = acc) do
    check_field(v, join_key(prefix, k), schema_flat_map)
    acc
  end

  defp check_field(%DateTime{}, key, schema_flat_map),
    do: enforce_type(:datetime, key, schema_flat_map)

  defp check_field(%NaiveDateTime{}, key, schema_flat_map),
    do: enforce_type(:datetime, key, schema_flat_map)

  defp check_field(v, key, schema_flat_map) when is_list(v) do
    case hd(v) do
      head when is_map(head) ->
        enforce_type(:map, key, schema_flat_map)
        walk_maps(v, key, schema_flat_map)

      head ->
        enforce_type({:list, SchemaTypes.type_of(head)}, key, schema_flat_map)
    end
  end

  defp check_field(v, key, schema_flat_map) when is_map(v) do
    enforce_type(:map, key, schema_flat_map)
    walk_map(v, key, schema_flat_map)
  end

  defp check_field(v, key, schema_flat_map),
    do: enforce_type(SchemaTypes.type_of(v), key, schema_flat_map)

  defp walk_maps([], _prefix, _schema_flat_map), do: :ok

  defp walk_maps([h | t], prefix, schema_flat_map) do
    walk_map(h, prefix, schema_flat_map)
    walk_maps(t, prefix, schema_flat_map)
  end

  defp enforce_type(incoming, key, schema_flat_map) do
    case Map.fetch(schema_flat_map, key) do
      {:ok, ^incoming} ->
        :ok

      {:ok, existing} ->
        raise(
          "Type error! Field `#{key}` has type of `#{inspect(existing)}`. Incoming metadata has type of `#{inspect(incoming)}`."
        )

      :error ->
        :ok
    end
  end

  defp join_key("", key), do: to_string(key)
  defp join_key(prefix, key), do: prefix <> "." <> to_string(key)
end

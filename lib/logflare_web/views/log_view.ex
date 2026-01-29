defmodule LogflareWeb.LogView do
  use LogflareWeb, :view

  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Lql
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SourceSchemas.Cache
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.Utils
  alias LogflareWeb.JSONViewerComponent

  import LogflareWeb.CoreComponents, only: [log_event_permalink: 1]

  def render("index.json", %{message: message}) do
    %{message: message}
  end

  # @spec append_to_query(String.t(), map(), term(), Logflare.Source.t()) :: String.t()
  def append_to_query(lql, %{key: key, path: path, value: value}, source) do
    flat_map = source_schema_flat_map(source)

    case normalize_array_key(key, path, flat_map) do
      {normalized_key, list_includes?} ->
        path = resolve_lql_path(normalized_key, flat_map)
        append_filter(lql, path, value, source, list_includes?)

      _ ->
        ""
    end
  end

  defp append_filter(lql, path, value, source, list_includes?) do
    value = normalize_timestamp_value(path, value)

    filter_rule =
      FilterRule.build(
        path: path,
        operator: if(list_includes?, do: :list_includes, else: :=),
        value: value,
        modifiers: if(is_binary(value), do: %{quoted_string: true}, else: %{})
      )

    updated_lql =
      lql
      |> Lql.decode!(lql_schema(source))
      |> Kernel.++([filter_rule])
      |> Lql.encode!()

    ~p"/sources/#{source}/search?#{%{querystring: updated_lql, tailing?: false}}"
  end

  defp lql_schema(source) do
    case Cache.get_source_schema_by(source_id: source.id) do
      %_{bigquery_schema: schema} when not is_nil(schema) -> schema
      _ -> SchemaBuilder.initial_table_schema()
    end
  end

  # Checks the path + key exists.
  # If it doesn't the keypath maybe an array index, so recursively drops the last path segment until it finds the key.
  defp normalize_array_key(nil, _path, _flat_map), do: :not_found

  defp normalize_array_key(key, path, flat_map) do
    keypath =
      (path ++ [key])
      |> Enum.join(".")

    case Map.get(flat_map, keypath) do
      {:list, _} ->
        {keypath, true}

      nil ->
        {key, path} = List.pop_at(path, -1)
        normalize_array_key(key, path, flat_map)

      _ ->
        {keypath, false}
    end
  end

  defp resolve_lql_path(key, schema_flat_map) do
    cond do
      String.starts_with?(key, "metadata.") ->
        key

      Map.has_key?(schema_flat_map, key) ->
        key

      Map.has_key?(schema_flat_map, "metadata.#{key}") ->
        "metadata.#{key}"

      true ->
        "metadata.#{key}"
    end
  end

  defp source_schema_flat_map(source) do
    case Cache.get_source_schema_by(source_id: source.id) do
      %_{schema_flat_map: flatmap} when is_map(flatmap) -> flatmap
      _ -> SchemaBuilder.initial_table_schema() |> SchemaUtils.bq_schema_to_flat_typemap()
    end
  end

  defp normalize_timestamp_value("timestamp", value) when is_integer(value) do
    value
    |> Utils.to_microseconds()
    |> DateTime.from_unix!(:microsecond)
    |> DateTime.to_naive()
  end

  defp normalize_timestamp_value("timestamp", %DateTime{} = value) do
    DateTime.to_naive(value)
  end

  defp normalize_timestamp_value("timestamp", %NaiveDateTime{} = value), do: value

  defp normalize_timestamp_value(_path, value), do: value
end

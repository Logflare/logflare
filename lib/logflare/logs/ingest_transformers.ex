defmodule Logflare.Logs.IngestTransformers do
  @moduledoc false
  import Logflare.EnumDeepUpdate, only: [update_all_keys_deep: 2]

  @alphanumeric_regex ~r/\W/

  @spec transform(map, list(atom) | atom) :: map
  def transform(log_params, :to_bigquery_column_spec) when is_map(log_params) do
    transform(log_params, [
      :strip_bq_prefixes,
      :dashes_to_underscores,
      :alter_leading_numbers,
      :alphanumeric_only,
      {:field_length, max: 128}
    ])
  end

  def transform(log_params, rules) when is_map(log_params) and is_list(rules) do
    Enum.reduce(rules, log_params, &do_transform(&2, &1))
  end

  @spec do_transform(map, atom) :: map
  defp do_transform(log_params, {:field_length, max: max}) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) and byte_size(key) > max ->
        "_" <> String.slice(key, 0..(max - 1))

      key ->
        key
    end)
  end

  defp do_transform(log_params, :alphanumeric_only) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) ->
        case Regex.match?(@alphanumeric_regex, key) do
          true -> "_" <> String.replace(key, @alphanumeric_regex, "_")
          false -> key
        end

      key ->
        key
    end)
  end

  defp do_transform(log_params, :strip_bq_prefixes) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      "_TABLE_" <> _rest = key -> "_" <> key
      "_FILE_" <> _rest = key -> "_" <> key
      "_PARTITION_" <> _rest = key -> "_" <> key
      key -> key
    end)
  end

  defp do_transform(log_params, :dashes_to_underscores) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) ->
        case String.contains?(key, "-") do
          true -> "_" <> String.replace(key, "-", "_")
          false -> key
        end

      key ->
        key
    end)
  end

  defp do_transform(log_params, :alter_leading_numbers) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      <<symbol::binary-size(1), rest::binary>> when symbol in ~w(0 1 2 3 4 5 6 7 8 9) ->
        "_" <> symbol <> rest

      key ->
        key
    end)
  end
end

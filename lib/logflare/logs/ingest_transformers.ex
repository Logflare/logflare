defmodule Logflare.Logs.IngestTransformers do
  @moduledoc false
  import Logflare.EnumDeepUpdate, only: [update_all_keys_deep: 2]

  @alphanumeric_regex ~r/\W/
  @max_field_length 128

  @spec transform(map, list(atom) | atom) :: map
  def transform(log_params, :to_bigquery_column_spec) when is_map(log_params) do
    update_all_keys_deep(log_params, &to_bigquery_column_spec/1)
  end

  def transform(log_params, rules) when is_map(log_params) and is_list(rules) do
    Enum.reduce(rules, log_params, &do_transform(&2, &1))
  end

  # Single-pass key transformation applying all BigQuery column spec rules.
  # This works because the rules target independent character patterns — no
  # rule's output triggers a subsequent rule.
  # If adding a new rule, verify that still holds, otherwise fall back to multi-pass via transform/2.
  @spec to_bigquery_column_spec(term()) :: term()
  defp to_bigquery_column_spec(key) when is_binary(key) do
    key
    |> strip_bq_prefix()
    |> dashes_to_underscores()
    |> alter_leading_number()
    |> alphanumeric_only()
    |> enforce_field_length()
  end

  defp to_bigquery_column_spec(key), do: key

  @spec strip_bq_prefix(String.t()) :: String.t()
  defp strip_bq_prefix("_TABLE_" <> _rest = key), do: "_" <> key
  defp strip_bq_prefix("_FILE_" <> _rest = key), do: "_" <> key
  defp strip_bq_prefix("_PARTITION_" <> _rest = key), do: "_" <> key
  defp strip_bq_prefix(key), do: key

  @spec dashes_to_underscores(String.t()) :: String.t()
  defp dashes_to_underscores(key) do
    if String.contains?(key, "-") do
      "_" <> String.replace(key, "-", "_")
    else
      key
    end
  end

  @spec alter_leading_number(String.t()) :: String.t()
  defp alter_leading_number(<<symbol::binary-size(1), rest::binary>>)
       when symbol in ~w(0 1 2 3 4 5 6 7 8 9),
       do: "_" <> symbol <> rest

  defp alter_leading_number(key), do: key

  @spec alphanumeric_only(String.t()) :: String.t()
  defp alphanumeric_only(key) do
    if Regex.match?(@alphanumeric_regex, key) do
      "_" <> String.replace(key, @alphanumeric_regex, "_")
    else
      key
    end
  end

  @spec enforce_field_length(String.t()) :: String.t()
  defp enforce_field_length(key) when byte_size(key) > @max_field_length,
    do: "_" <> String.slice(key, 0..(@max_field_length - 1))

  defp enforce_field_length(key), do: key

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

defmodule Logflare.Logs.IngestTransformers do
  import Logflare.EnumDeepUpdate, only: [update_all_keys_deep: 2]

  @spec transform(map, list(atom)) :: map
  def transform(log_params, :to_bigquery_column_spec) when is_map(log_params) do
    transform(log_params, [
      :alphanumeric_only,
      :strip_bq_prefixes,
      :dashes_to_underscores,
      :alter_leading_numbers,
      {:field_length, max: 128}
    ])
  end

  def transform(log_params, rules) when is_map(log_params) and is_list(rules) do
    Map.update(log_params, "metadata", %{}, fn m ->
      Enum.reduce(rules, m, &do_transform(&2, &1))
    end)
  end

  @spec do_transform(map, atom) :: map
  defp do_transform(log_params, {:field_length, max: max}) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) and byte_size(key) > max -> String.slice(key, 0..(max - 1))
      key -> key
    end)
  end

  defp do_transform(log_params, :alphanumeric_only) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) -> String.replace(key, ~r/\W/, "")
      key -> key
    end)
  end

  defp do_transform(log_params, :strip_bq_prefixes) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      "_TABLE_" <> rest -> rest
      "_FILE_" <> rest -> rest
      "_PARTITION_" <> rest -> rest
      x -> x
    end)
  end

  defp do_transform(log_params, :dashes_to_underscores) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) ->
        String.replace(key, "-", "_")

      key ->
        key
    end)
  end

  defp do_transform(log_params, :alter_leading_numbers) when is_map(log_params) do
    update_all_keys_deep(log_params, fn
      "0" <> rest ->
        "zero" <> rest

      "1" <> rest ->
        "one" <> rest

      "2" <> rest ->
        "two" <> rest

      "3" <> rest ->
        "three" <> rest

      "4" <> rest ->
        "four" <> rest

      "5" <> rest ->
        "five" <> rest

      "6" <> rest ->
        "six" <> rest

      "7" <> rest ->
        "seven" <> rest

      "8" <> rest ->
        "eight" <> rest

      "9" <> rest ->
        "nine" <> rest

      key ->
        key
    end)
  end
end

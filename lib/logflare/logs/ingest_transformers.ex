defmodule Logflare.Logs.IngestTransformers do
  def transform(batch, rules) do
    for log_params <- batch do
      Enum.reduce(rules, log_params, &do_transform(&2, &1))
    end
  end

  defp do_transform(log_params, :alphanumeric_only) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) -> String.replace(key, ~r/\W/, "")
      key -> key
    end)
  end

  defp do_transform(log_params, :strip_bq_prefixes) do
    update_all_keys_deep(log_params, fn
      "_TABLE_" <> rest -> rest
      "_FILE_" <> rest -> rest
      "_PARTITION_" <> rest -> rest
      x -> x
    end)
  end

  defp do_transform(log_params, :dashes_to_underscores) do
    update_all_keys_deep(log_params, fn
      key when is_binary(key) ->
        String.replace(key, "-", "_")

      key ->
        key
    end)
  end

  defp do_transform(log_params, :alter_leading_numbers) do
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

  defp update_all_keys_deep(data, fun) when is_map(data) do
    data
    |> Enum.map(fn
      {k, v} when is_map(v) or is_list(v) ->
        {fun.(k), update_all_keys_deep(v, fun)}

      {k, v} ->
        {fun.(k), v}
    end)
    |> Map.new()
  end

  defp update_all_keys_deep(data, fun) when is_list(data) do
    Enum.map(data, &update_all_keys_deep(&1, fun))
  end

  defp update_all_keys_deep(data, _), do: data
end

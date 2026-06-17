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

  # Rewrites a map key into a valid BigQuery standard column name in a single
  # pass. Standard names allow only [A-Za-z0-9_], cannot start with a digit,
  # cannot use a reserved prefix, and must be valid UTF-8. Classification is per
  # Unicode codepoint (not per byte), so every codepoint outside [A-Za-z0-9_] —
  # including all multibyte characters — collapses to a single "_" and the
  # result is always valid UTF-8.
  #
  # Each applicable rule prepends at most one leading underscore; the leading-
  # digit rule is suppressed when a reserved prefix or dash has already added one.
  @spec to_bigquery_column_spec(term()) :: term()
  defp to_bigquery_column_spec(key) when is_binary(key) do
    {body, dash?, non_alnum?} = walk_bq_key(key, <<>>, false, false)
    prefix? = bq_reserved_prefix?(key)
    digit? = not prefix? and not dash? and leading_digit?(key)

    underscores =
      bool_int(prefix?) + bool_int(dash?) + bool_int(digit?) + bool_int(non_alnum?)

    body
    |> prepend_underscores(underscores)
    |> enforce_field_length()
  end

  defp to_bigquery_column_spec(key), do: key

  @compile {:inline, bool_int: 1}
  defp bool_int(true), do: 1
  defp bool_int(false), do: 0

  defp prepend_underscores(body, 0), do: body
  defp prepend_underscores(body, n), do: <<:binary.copy("_", n)::binary, body::binary>>

  # BigQuery reserves these column-name prefixes; a single prepended underscore
  # breaks the match (e.g. "_TABLE_" → "__TABLE_", "__ROOT__" → "___ROOT__").
  defp bq_reserved_prefix?("_TABLE_" <> _), do: true
  defp bq_reserved_prefix?("_FILE_" <> _), do: true
  defp bq_reserved_prefix?("_PARTITION" <> _), do: true
  defp bq_reserved_prefix?("_ROW_TIMESTAMP" <> _), do: true
  defp bq_reserved_prefix?("__ROOT__" <> _), do: true
  defp bq_reserved_prefix?("_COLIDENTIFIER" <> _), do: true
  defp bq_reserved_prefix?("_CHANGE_SEQUENCE_NUMBER" <> _), do: true
  defp bq_reserved_prefix?("_CHANGE_TYPE" <> _), do: true
  defp bq_reserved_prefix?("_CHANGE_TIMESTAMP" <> _), do: true
  defp bq_reserved_prefix?(_), do: false

  defp leading_digit?(<<b, _::binary>>) when b in ?0..?9, do: true
  defp leading_digit?(_), do: false

  defp walk_bq_key(<<>>, acc, dash?, non_alnum?), do: {acc, dash?, non_alnum?}

  defp walk_bq_key(<<?-, rest::binary>>, acc, _dash?, non_alnum?),
    do: walk_bq_key(rest, <<acc::binary, ?_>>, true, non_alnum?)

  defp walk_bq_key(<<b, rest::binary>>, acc, dash?, non_alnum?)
       when b in ?0..?9 or b in ?A..?Z or b in ?a..?z or b == ?_,
       do: walk_bq_key(rest, <<acc::binary, b>>, dash?, non_alnum?)

  defp walk_bq_key(<<_c::utf8, rest::binary>>, acc, dash?, _non_alnum?),
    do: walk_bq_key(rest, <<acc::binary, ?_>>, dash?, true)

  defp walk_bq_key(<<_b, rest::binary>>, acc, dash?, _non_alnum?),
    do: walk_bq_key(rest, <<acc::binary, ?_>>, dash?, true)

  defp enforce_field_length(key) when byte_size(key) > @max_field_length,
    do: <<?_, binary_part(key, 0, @max_field_length)::binary>>

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

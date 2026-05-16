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

  # Single binary walk applying every BigQuery column-spec rule. The original
  # five-stage pipeline (strip_bq_prefix → dashes_to_underscores →
  # alter_leading_number → alphanumeric_only → enforce_field_length) each
  # rescanned the binary; fusing them into one pass reclaims the redundant
  # scan overhead flagged in O11Y-1828.
  #
  # Every rule contributes at most one leading underscore; the digit rule is
  # suppressed when a prefix or dash has already inserted one (matching the
  # original step ordering). Byte classification mirrors PCRE's byte-mode \w
  # table — including the Latin-1 letter ranges (170, 181, 186, 192..214,
  # 216..246, 248..255) — so non-ASCII keys produce the same column names as
  # the legacy `~r/\W/` replacement.
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

  defp bq_reserved_prefix?("_TABLE_" <> _), do: true
  defp bq_reserved_prefix?("_FILE_" <> _), do: true
  defp bq_reserved_prefix?("_PARTITION_" <> _), do: true
  defp bq_reserved_prefix?(_), do: false

  defp leading_digit?(<<b, _::binary>>) when b in ?0..?9, do: true
  defp leading_digit?(_), do: false

  defp walk_bq_key(<<>>, acc, dash?, non_alnum?), do: {acc, dash?, non_alnum?}

  defp walk_bq_key(<<?-, rest::binary>>, acc, _dash?, non_alnum?),
    do: walk_bq_key(rest, <<acc::binary, ?_>>, true, non_alnum?)

  defp walk_bq_key(<<b, rest::binary>>, acc, dash?, non_alnum?)
       when b in ?0..?9 or b in ?A..?Z or b in ?a..?z or b == ?_,
       do: walk_bq_key(rest, <<acc::binary, b>>, dash?, non_alnum?)

  defp walk_bq_key(<<b, rest::binary>>, acc, dash?, non_alnum?)
       when b == 170 or b == 181 or b == 186 or
              (b >= 192 and b <= 214) or
              (b >= 216 and b <= 246) or
              b >= 248,
       do: walk_bq_key(rest, <<acc::binary, b>>, dash?, non_alnum?)

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

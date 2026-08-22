defmodule Logflare.Logs.IngestTransformers do
  @moduledoc false
  import Logflare.EnumDeepUpdate, only: [update_all_keys_deep: 2]

  alias Logflare.Logs.Ingest.MetadataCleaner

  @alphanumeric_regex ~r/\W/
  @max_field_length 128

  @typep direct_transform :: :clean_to_bigquery_column_spec | :to_bigquery_column_spec
  @typep transform_rule ::
           :alphanumeric_only
           | :alter_leading_numbers
           | :dashes_to_underscores
           | :strip_bq_prefixes
           | {:field_length, max: pos_integer()}

  defguardp is_nil_or_empty(x) when x in [%{}, [], "", {}, nil]

  defguardp is_bq_safe_byte(byte)
            when byte in ?0..?9 or byte in ?A..?Z or byte in ?a..?z or byte == ?_

  @spec transform(map(), direct_transform() | [transform_rule()]) :: map()
  def transform(log_params, :clean_to_bigquery_column_spec) when is_map(log_params) do
    if clean_and_bq_safe?(log_params) do
      log_params
    else
      clean_and_to_bigquery_column_spec(log_params)
    end
  catch
    :throw, :normalized_bigquery_column_collision ->
      log_params
      |> MetadataCleaner.deep_reject_nil_and_empty()
      |> transform(:to_bigquery_column_spec)
  end

  def transform(log_params, :to_bigquery_column_spec) when is_map(log_params) do
    update_all_keys_deep(log_params, &to_bigquery_column_spec/1)
  end

  def transform(log_params, rules) when is_map(log_params) and is_list(rules) do
    Enum.reduce(rules, log_params, &do_transform(&2, &1))
  end

  defp clean_and_bq_safe?(%_{}), do: false

  defp clean_and_bq_safe?(data) when is_map(data) do
    :maps.fold(
      fn
        _k, _v, false ->
          false

        _k, v, true when is_nil_or_empty(v) ->
          false

        k, v, true when is_map(v) or is_list(v) ->
          bq_safe_key?(k) and clean_and_bq_safe?(v)

        k, _v, true ->
          bq_safe_key?(k)
      end,
      true,
      data
    )
  end

  defp clean_and_bq_safe?([head | tail]) do
    clean_list_is_unchanged?(head) and clean_and_bq_safe_list_tail?(tail)
  end

  defp clean_list_is_unchanged?(value) when is_nil_or_empty(value), do: false

  defp clean_list_is_unchanged?(value) when is_map(value) or is_list(value),
    do: clean_and_bq_safe?(value)

  defp clean_list_is_unchanged?(_value), do: true

  defp clean_and_bq_safe_list_tail?([]), do: true

  defp clean_and_bq_safe_list_tail?([head | tail]),
    do: clean_list_is_unchanged?(head) and clean_and_bq_safe_list_tail?(tail)

  defp clean_and_bq_safe_list_tail?(_improper_tail), do: false

  defp clean_and_to_bigquery_column_spec(data) when is_map(data) do
    :maps.fold(
      fn
        _k, v, acc when is_nil_or_empty(v) ->
          acc

        k, v, acc when is_map(v) or is_list(v) ->
          cleaned = clean_and_to_bigquery_column_spec(v)

          if is_nil_or_empty(cleaned) do
            acc
          else
            put_bigquery_column(acc, k, cleaned)
          end

        k, v, acc ->
          put_bigquery_column(acc, k, v)
      end,
      %{},
      data
    )
  end

  defp clean_and_to_bigquery_column_spec(data) when is_list(data) do
    data
    |> Enum.reduce([], fn
      x, acc when is_nil_or_empty(x) ->
        acc

      x, acc when is_map(x) or is_list(x) ->
        cleaned = clean_and_to_bigquery_column_spec(x)
        if is_nil_or_empty(cleaned), do: acc, else: [cleaned | acc]

      x, acc ->
        [x | acc]
    end)
    |> Enum.reverse()
  end

  @compile {:inline, put_bigquery_column: 3}
  @spec put_bigquery_column(map(), term(), term()) :: map()
  defp put_bigquery_column(acc, key, value) do
    result = Map.put(acc, to_bigquery_column_spec(key), value)

    if map_size(result) == map_size(acc), do: throw(:normalized_bigquery_column_collision)

    result
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
    if bq_safe_key?(key) do
      key
    else
      transform_bq_key(key)
    end
  end

  defp to_bigquery_column_spec(key), do: key

  defp transform_bq_key(key) do
    {body, dash?, non_alnum?} = walk_bq_key(key, <<>>, false, false)
    prefix? = bq_reserved_prefix?(key)
    digit? = not prefix? and not dash? and leading_digit?(key)

    underscores =
      bool_int(prefix?) + bool_int(dash?) + bool_int(digit?) + bool_int(non_alnum?)

    body
    |> prepend_underscores(underscores)
    |> enforce_field_length()
  end

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

  defp bq_safe_key?(key) when not is_binary(key), do: true
  defp bq_safe_key?(key) when byte_size(key) > @max_field_length, do: false
  defp bq_safe_key?(<<>>), do: true
  defp bq_safe_key?(<<b, _rest::binary>>) when b in ?0..?9, do: false

  defp bq_safe_key?(<<?_, _rest::binary>> = key),
    do: not bq_reserved_prefix?(key) and bq_safe_chars?(key)

  defp bq_safe_key?(key), do: bq_safe_chars?(key)

  defp bq_safe_chars?(<<a, b, c, d, rest::binary>>)
       when is_bq_safe_byte(a) and is_bq_safe_byte(b) and is_bq_safe_byte(c) and
              is_bq_safe_byte(d),
       do: bq_safe_chars?(rest)

  defp bq_safe_chars?(<<b, rest::binary>>) when is_bq_safe_byte(b),
    do: bq_safe_chars?(rest)

  defp bq_safe_chars?(<<>>), do: true
  defp bq_safe_chars?(_), do: false

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

  @spec do_transform(map(), transform_rule()) :: map()
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

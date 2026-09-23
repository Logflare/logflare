defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryErrorNormalizer do
  @moduledoc """
  Turns a `Ch.Error` into a `Logflare.Backends.QueryError`.

  Errors whose ClickHouse code is on the user-error allowlist become `:invalid_query`
  with a user-facing `description`. Everything else is a `:backend_error` with no
  description, so callers fall back to a generic message.

  The description is the ClickHouse message with everything that describes Logflare's
  rewritten query rather than the caller's SQL removed: the `In query` / `in scope` /
  `While processing` echo (which repeats the endpoint CTE and the physical `otel_*` table name), the
  syntax-error offset into that rewritten query, the `Received from <host>` replica
  address, and the version and stack-trace trailers. ClickHouse's `Maybe you meant`
  hint and the error name are kept, since they are what lets a caller (or an AI
  assistant) repair the query. Column references ClickHouse qualifies with a physical
  `otel_*_<backend token>` table lose the qualifier, and any other mention of such a
  table is reduced to its event type.
  """

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.QueryError

  @user_error_codes %{
    6 => "CANNOT_PARSE_TEXT",
    27 => "CANNOT_PARSE_INPUT_ASSERTION_FAILED",
    36 => "BAD_ARGUMENTS",
    38 => "CANNOT_PARSE_DATE",
    41 => "CANNOT_PARSE_DATETIME",
    42 => "NUMBER_OF_ARGUMENTS_DOESNT_MATCH",
    43 => "ILLEGAL_TYPE_OF_ARGUMENT",
    44 => "ILLEGAL_COLUMN",
    46 => "UNKNOWN_FUNCTION",
    47 => "UNKNOWN_IDENTIFIER",
    53 => "TYPE_MISMATCH",
    62 => "SYNTAX_ERROR",
    69 => "ARGUMENT_OUT_OF_BOUND",
    70 => "CANNOT_CONVERT_TYPE",
    72 => "CANNOT_PARSE_NUMBER",
    179 => "MULTIPLE_EXPRESSIONS_FOR_ALIAS",
    184 => "ILLEGAL_AGGREGATION",
    207 => "AMBIGUOUS_IDENTIFIER",
    215 => "NOT_AN_AGGREGATE",
    352 => "AMBIGUOUS_COLUMN_NAME"
  }
  @user_error_code_list Map.keys(@user_error_codes)
  @user_error_name_pattern ~r/\((#{Enum.map_join(@user_error_codes, "|", fn {_code, name} -> name end)})\)/

  @trailer ~r/(?:,?\s*Stack trace|\s*\(version ).*\z/s
  @error_code_name ~r/\s*\(([A-Z][A-Z0-9_]*)\)\z/
  @hint ~r/[\s.]*Maybe you meant: (\[[^\]]*\])\.?\z/
  @exception_prefix ~r/\ACode:\s*\d+\.\s*(?:(?:DB::Exception:|Received from \S+)\s*)*/
  @query_echo ~r/[\s.:,]*\b(?:[Ii]n (?:query|scope)|[Ww]hile processing)\b.*\z/s
  @syntax_error_position ~r/ at position \d+ \((.+?)\)(?=[:.\s]|\z)/
  @syntax_error_line_col ~r/ \(line \d+, col \d+\)/
  @trailing_punctuation ~r/[\s.:,]+\z/
  @physical_table_qualifier ~r/\b(?:\w+\.)?otel_(?:logs|metrics|traces)_[0-9a-f_]+\./
  @physical_table ~r/\b(?:\w+\.)?otel_(logs|metrics|traces)_[0-9a-f_]+\b/
  @missing_identifier ~r/Unknown (?:expression )?identifier:? [`"']?([^`"'\s,;]+)/
  @identifier_quotes ~r/^[`"'.]+|[`"'.,]+$/

  @spec normalize(Ch.Error.t()) :: QueryError.t()
  def normalize(%Ch.Error{} = error) do
    case user_error?(error) do
      true ->
        %QueryError{
          kind: :invalid_query,
          raw_error: error,
          backend: ClickHouseAdaptor,
          description: describe(error)
        }

      false ->
        %QueryError{kind: :backend_error, raw_error: error, backend: ClickHouseAdaptor}
    end
  end

  @spec user_error?(Ch.Error.t()) :: boolean()
  defp user_error?(%Ch.Error{code: code}) when code in @user_error_code_list, do: true

  defp user_error?(%Ch.Error{code: nil, message: message}) when is_non_empty_binary(message),
    do: Regex.match?(@user_error_name_pattern, message)

  defp user_error?(%Ch.Error{}), do: false

  @spec describe(Ch.Error.t()) :: String.t() | nil
  defp describe(%Ch.Error{message: message}) when is_non_empty_binary(message) do
    message =
      message
      |> String.replace_invalid()
      |> strip(@physical_table_qualifier)
      |> then(&Regex.replace(@physical_table, &1, "\\1"))

    {body, hint, code_name} = parse(message)

    case missing_identifier(message) do
      nil -> join([sentence(body), hint_sentence(hint), code_label(code_name)])
      field -> join([~s(Field "#{field}" does not exist.), hint_sentence(hint)])
    end
  end

  defp describe(%Ch.Error{}), do: nil

  @spec parse(String.t()) :: {String.t(), String.t() | nil, String.t() | nil}
  defp parse(message) do
    {code_name, message} =
      message
      |> String.trim()
      |> strip(@trailer)
      |> split_trailing(@error_code_name)

    {hint, message} = split_trailing(message, @hint)

    body =
      message
      |> strip(@exception_prefix)
      |> strip(@query_echo)
      |> then(&Regex.replace(@syntax_error_position, &1, ~S( at "\1")))
      |> strip(@syntax_error_line_col)
      |> strip(@trailing_punctuation)

    {body, hint, code_name}
  end

  defp missing_identifier(message) do
    case Regex.run(@missing_identifier, message, capture: :all_but_first) do
      [field] -> Regex.replace(@identifier_quotes, field, "")
      nil -> nil
    end
  end

  defp strip(message, pattern), do: Regex.replace(pattern, message, "")

  defp split_trailing(message, pattern) do
    case Regex.run(pattern, message, return: :index) do
      [{start, _length}, {capture_start, capture_length}] ->
        {binary_part(message, capture_start, capture_length), binary_part(message, 0, start)}

      nil ->
        {nil, message}
    end
  end

  defp sentence(""), do: nil
  defp sentence(body), do: body <> "."

  defp hint_sentence(nil), do: nil
  defp hint_sentence(hint), do: "Maybe you meant: #{hint}."

  defp code_label(nil), do: nil
  defp code_label(code_name), do: "(#{code_name})"

  defp join(parts) do
    case Enum.reject(parts, &is_nil/1) do
      [] -> nil
      parts -> Enum.join(parts, " ")
    end
  end
end

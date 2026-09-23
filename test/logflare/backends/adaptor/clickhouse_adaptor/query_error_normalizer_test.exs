defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryErrorNormalizerTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryErrorNormalizer
  alias Logflare.Backends.QueryError

  @cte "WITH logs AS (SELECT now() AS timestamp, 'x' AS event_message FROM system.one AS otel_logs_abc123def)"
  @version "(version 26.2.19.43 (official build))"

  @user_errors [
    {215,
     "Code: 215. DB::Exception: Column 'log_attributes' is not under aggregate function and not in GROUP BY keys. In query #{@cte} SELECT log_attributes, log_attributes['cf.country'] AS country, count() AS count FROM logs WHERE source = 'edge_logs' GROUP BY log_attributes['cf.country'] ORDER BY count DESC. (NOT_AN_AGGREGATE) #{@version}",
     "Column 'log_attributes' is not under aggregate function and not in GROUP BY keys. (NOT_AN_AGGREGATE)"},
    {47,
     "Code: 47. DB::Exception: Unknown expression identifier `evnt_message` in scope #{@cte} SELECT evnt_message FROM logs. Maybe you meant: ['event_message']. (UNKNOWN_IDENTIFIER) #{@version}",
     ~s(Field "evnt_message" does not exist. Maybe you meant: ['event_message'].)},
    {47,
     "Code: 47. DB::Exception: Unknown expression identifier `notthere` in scope SELECT notthere. (UNKNOWN_IDENTIFIER)",
     ~s(Field "notthere" does not exist.)},
    {46,
     "Code: 46. DB::Exception: Function with name `lowr` does not exist. In scope #{@cte} SELECT lowr(event_message) FROM logs. Maybe you meant: ['lower','floor']. (UNKNOWN_FUNCTION) #{@version}",
     "Function with name `lowr` does not exist. Maybe you meant: ['lower','floor']. (UNKNOWN_FUNCTION)"},
    {184,
     "Code: 184. DB::Exception: Aggregate function count() is found inside another aggregate function in query. (ILLEGAL_AGGREGATION) #{@version}",
     "Aggregate function count() is found inside another aggregate function. (ILLEGAL_AGGREGATION)"},
    {43,
     "Code: 43. DB::Exception: Illegal types UInt8 and String of arguments of function plus: In scope #{@cte} SELECT 1 + 'a' FROM logs. (ILLEGAL_TYPE_OF_ARGUMENT) #{@version}",
     "Illegal types UInt8 and String of arguments of function plus. (ILLEGAL_TYPE_OF_ARGUMENT)"},
    {53,
     "Code: 53. DB::Exception: Cannot convert string 'x' to type UInt8. (TYPE_MISMATCH) #{@version}",
     "Cannot convert string 'x' to type UInt8. (TYPE_MISMATCH)"},
    {41,
     "Code: 41. DB::Exception: Cannot parse datetime: Cannot parse DateTime from String. (CANNOT_PARSE_DATETIME) #{@version}",
     "Cannot parse datetime: Cannot parse DateTime from String. (CANNOT_PARSE_DATETIME)"},
    {62,
     "Code: 62. DB::Exception: Syntax error: failed at position 119 ((): (event_message from logs. Unmatched parentheses: (. (SYNTAX_ERROR) #{@version}",
     ~s[Syntax error: failed at "(": (event_message from logs. Unmatched parentheses: (. (SYNTAX_ERROR)]},
    {62,
     "Code: 62. DB::Exception: Syntax error: failed at position 139 (end of query): . Expected one of: expression with optional alias, lambda expression. (SYNTAX_ERROR) #{@version}",
     ~s[Syntax error: failed at "end of query": . Expected one of: expression with optional alias, lambda expression. (SYNTAX_ERROR)]},
    {62,
     "Code: 62. DB::Exception: Syntax error: failed at position 309 (() (line 8, col 44): (172\n  GROUP BY query_text\n  LIMIT 50. Unmatched parentheses: (. (SYNTAX_ERROR) #{@version}",
     ~s[Syntax error: failed at "(": (172\n  GROUP BY query_text\n  LIMIT 50. Unmatched parentheses: (. (SYNTAX_ERROR)]},
    {6,
     "Code: 6. DB::Exception: Cannot parse string 'x' as UInt8: syntax error at begin of string. Note: there are toUInt8OrZero and toUInt8OrNull functions, which returns zero/NULL instead of throwing exception. (CANNOT_PARSE_TEXT) #{@version}",
     "Cannot parse string 'x' as UInt8: syntax error at begin of string. Note: there are toUInt8OrZero and toUInt8OrNull functions, which returns zero/NULL instead of throwing exception. (CANNOT_PARSE_TEXT)"},
    {215,
     "Code: 215. DB::Exception: Column 'default.otel_logs_abc123def.event_message' is not under aggregate function and not in GROUP BY keys. In query SELECT event_message, count() FROM default.otel_logs_abc123def. (NOT_AN_AGGREGATE) #{@version}",
     "Column 'event_message' is not under aggregate function and not in GROUP BY keys. (NOT_AN_AGGREGATE)"},
    {36,
     "Code: 36. DB::Exception: Table default.otel_traces_abc123def does not support argument 'x'. (BAD_ARGUMENTS) #{@version}",
     "Table traces does not support argument 'x'. (BAD_ARGUMENTS)"},
    {43,
     "Code: 43. DB::Exception: Illegal type UInt8 of argument of function lower: While processing lower(n) AS x FROM otel_logs_abc123def WHERE project = 'abc'. (ILLEGAL_TYPE_OF_ARGUMENT) #{@version}",
     "Illegal type UInt8 of argument of function lower. (ILLEGAL_TYPE_OF_ARGUMENT)"},
    {47,
     "Code: 47. DB::Exception: Missing columns: 'notthere' while processing query: '#{@cte} SELECT notthere FROM logs', required columns: 'notthere'. (UNKNOWN_IDENTIFIER) #{@version}",
     "Missing columns: 'notthere'. (UNKNOWN_IDENTIFIER)"},
    {6,
     "Code: 6. DB::Exception: Cannot parse string '12x' as UInt8: syntax error at position 2 (parsed just '12'). Note: there are toUInt8OrZero and toUInt8OrNull functions, which returns zero/NULL instead of throwing exception. (CANNOT_PARSE_TEXT) #{@version}",
     "Cannot parse string '12x' as UInt8: syntax error at position 2 (parsed just '12'). Note: there are toUInt8OrZero and toUInt8OrNull functions, which returns zero/NULL instead of throwing exception. (CANNOT_PARSE_TEXT)"},
    {215,
     "Code: 215. DB::Exception: Received from 10.0.0.12:9000. DB::Exception: Column 'a' is not under aggregate function and not in GROUP BY keys. In query SELECT a, count() FROM otel_logs_abc123def. (NOT_AN_AGGREGATE) #{@version}",
     "Column 'a' is not under aggregate function and not in GROUP BY keys. (NOT_AN_AGGREGATE)"}
  ]

  describe "normalize/1 with allowlisted user errors" do
    for {{code, raw_message, expected}, index} <- Enum.with_index(@user_errors) do
      @raw_message raw_message
      @expected expected
      @code code

      test "describes user error #{index} (code #{code})" do
        error = %Ch.Error{code: @code, message: @raw_message}

        assert %QueryError{
                 kind: :invalid_query,
                 backend: ClickHouseAdaptor,
                 raw_error: ^error,
                 description: description
               } = QueryErrorNormalizer.normalize(error)

        assert description == @expected
        refute description =~ "otel_logs_abc123def"
        refute description =~ "WITH logs AS"
        refute description =~ "10.0.0.12"
        refute description =~ "version 26"
      end
    end

    test "classifies codeless errors by their allowlisted error name" do
      error = %Ch.Error{
        code: nil,
        message:
          "Code: 46. DB::Exception: Function with name `lowr` does not exist. (UNKNOWN_FUNCTION)"
      }

      assert %QueryError{
               kind: :invalid_query,
               description: "Function with name `lowr` does not exist. (UNKNOWN_FUNCTION)"
             } = QueryErrorNormalizer.normalize(error)
    end

    test "replaces invalid UTF-8 echoed from event data so the description stays encodable" do
      error = %Ch.Error{
        code: 6,
        message:
          "Code: 6. DB::Exception: Cannot parse string '" <>
            <<0xFF, 0xFE>> <> "A' as UInt8: syntax error at begin of string. (CANNOT_PARSE_TEXT)"
      }

      assert %QueryError{description: description} = QueryErrorNormalizer.normalize(error)
      assert String.valid?(description)
      assert {:ok, _json} = Jason.encode(%{error: description})
      assert description =~ "Cannot parse string '"
    end

    test "strips a stack trace trailer" do
      error = %Ch.Error{
        code: 46,
        message:
          "Code: 46. DB::Exception: Function with name `lowr` does not exist. (UNKNOWN_FUNCTION), Stack trace (when copying this message, always include the lines below):\n\n0. DB::Exception::Exception() @ 0x000"
      }

      assert %QueryError{
               description: "Function with name `lowr` does not exist. (UNKNOWN_FUNCTION)"
             } =
               QueryErrorNormalizer.normalize(error)
    end

    test "keeps the hint and error name when no message body remains after sanitizing" do
      error = %Ch.Error{
        code: 46,
        message:
          "Code: 46. DB::Exception: In scope SELECT lowr(1) FROM otel_logs_abc123def. Maybe you meant: ['lower']. (UNKNOWN_FUNCTION)"
      }

      assert %QueryError{
               kind: :invalid_query,
               description: "Maybe you meant: ['lower']. (UNKNOWN_FUNCTION)"
             } = QueryErrorNormalizer.normalize(error)
    end

    test "leaves the description empty when nothing remains after sanitizing" do
      error = %Ch.Error{code: 62, message: "Code: 62. DB::Exception: In query SELECT."}

      assert %QueryError{kind: :invalid_query, description: nil} =
               QueryErrorNormalizer.normalize(error)
    end
  end

  describe "normalize/1 with other errors" do
    test "keeps unknown table errors as backend errors without a description" do
      error = %Ch.Error{
        code: 60,
        message:
          "Code: 60. DB::Exception: Unknown table expression identifier 'otel_logs_abc123def' in scope SELECT 1. (UNKNOWN_TABLE)"
      }

      assert %QueryError{kind: :backend_error, raw_error: ^error, description: nil} =
               QueryErrorNormalizer.normalize(error)
    end

    test "does not classify by error name when a non-allowlisted code is present" do
      error = %Ch.Error{code: 999, message: "Backend server error (SYNTAX_ERROR)"}

      assert %QueryError{kind: :backend_error, description: nil} =
               QueryErrorNormalizer.normalize(error)
    end

    test "treats codeless errors without an allowlisted name as backend errors" do
      error = %Ch.Error{code: nil, message: "unexpected result for 'SELECT 1'"}

      assert %QueryError{kind: :backend_error, description: nil} =
               QueryErrorNormalizer.normalize(error)
    end
  end
end

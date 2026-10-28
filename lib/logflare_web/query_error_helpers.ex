defmodule LogflareWeb.QueryErrorHelpers do
  @moduledoc false

  import Logflare.Utils.Guards

  alias Logflare.Backends.QueryError
  alias LogflareWeb.Utils

  @generic_query_error_message "Backend error! Retry your query. Please contact support if this continues."
  @timeout_query_error_message "Query timed out. Retry your query or reduce the time range."
  @user_facing_sandbox_error_prefixes [
    "sql parser error: ",
    "Only SELECT queries allowed",
    "Only singular query allowed",
    "Restricted function ",
    "Restricted setting ",
    "restricted wildcard (*) in a result column"
  ]

  @doc """
  Returns a user-facing query error message from a backend %QueryError{}.

      iex> error = %Logflare.Backends.QueryError{
      ...>   kind: :invalid_query,
      ...>   raw_error: %{"message" => "Unrecognized name: notthere at [1:8]"},
      ...>   backend: Logflare.Backends.Adaptor.BigQueryAdaptor
      ...> }
      iex> LogflareWeb.QueryErrorHelpers.query_error_message(error)
      ~s(Field "notthere" does not exist.)

      iex> error = %Logflare.Backends.QueryError{
      ...>   kind: :invalid_query,
      ...>   raw_error: %Ch.Error{code: 215, message: "Code: 215. DB::Exception: Column 'a' is not under aggregate function and not in GROUP BY keys. In query SELECT a, count() FROM otel_logs_abc. (NOT_AN_AGGREGATE)"},
      ...>   backend: Logflare.Backends.Adaptor.ClickHouseAdaptor,
      ...>   description: "Column 'a' is not under aggregate function and not in GROUP BY keys. (NOT_AN_AGGREGATE)"
      ...> }
      iex> LogflareWeb.QueryErrorHelpers.query_error_message(error)
      "Column 'a' is not under aggregate function and not in GROUP BY keys. (NOT_AN_AGGREGATE)"

      iex> error = %Logflare.Backends.QueryError{
      ...>   kind: :invalid_query,
      ...>   raw_error: %Postgrex.Error{message: ~s|column "notthere" does not exist|},
      ...>   backend: Logflare.Backends.Adaptor.PostgresAdaptor
      ...> }
      iex> LogflareWeb.QueryErrorHelpers.query_error_message(error)
      ~s(Field "notthere" does not exist.)

      iex> error = %Logflare.Backends.QueryError{
      ...>   kind: :timeout,
      ...>   raw_error: %{"message" => "Job execution was cancelled: Job timed out"},
      ...>   backend: Logflare.Backends.Adaptor.BigQueryAdaptor
      ...> }
      iex> LogflareWeb.QueryErrorHelpers.query_error_message(error)
      "Query timed out. Retry your query or reduce the time range."
  """
  @spec query_error_message(QueryError.t()) :: String.t()
  def query_error_message(%QueryError{} = error) do
    classified_query_error_message(error) || generic_query_error_message()
  end

  @spec generic_query_error_message() :: String.t()
  def generic_query_error_message, do: @generic_query_error_message

  @doc """
  Returns a user-facing message for a sandboxed-query validation error, as
  returned by `Logflare.Endpoints.run_query/3` when the query fails validation
  before ever reaching a backend.

  The "unknown table" case is translated into a plain message, and errors that
  describe only the caller's own SQL (parse errors, non-SELECT statements,
  restricted functions/settings, wildcards) pass through unchanged. Anything
  else falls back to the generic message, since Logflare's internal CTE
  splicing would otherwise leak into a user-facing error and confuse callers
  who never wrote a CTE themselves.

      iex> LogflareWeb.QueryErrorHelpers.sandbox_query_error_message("Table not found in CTE: (function_logs)")
      ~s(Table "function_logs" does not exist.)

      iex> LogflareWeb.QueryErrorHelpers.sandbox_query_error_message("sql parser error: Expected: SELECT, VALUES, or a subquery in the query body, found: EOF")
      "sql parser error: Expected: SELECT, VALUES, or a subquery in the query body, found: EOF"

      iex> LogflareWeb.QueryErrorHelpers.sandbox_query_error_message("Multiple CTEs available (first_cte, second_cte). You must specify which one to query using `f:name`")
      "Backend error! Retry your query. Please contact support if this continues."
  """
  @spec sandbox_query_error_message(String.t()) :: String.t()
  def sandbox_query_error_message(message) when is_binary(message) do
    classified_sandbox_error_message(message) || generic_query_error_message()
  end

  defp classified_sandbox_error_message("Table not found in CTE: (" <> rest) do
    rest |> String.trim_trailing(")") |> unknown_table_message()
  end

  defp classified_sandbox_error_message(message) do
    if String.starts_with?(message, @user_facing_sandbox_error_prefixes),
      do: String.replace_invalid(message)
  end

  defp unknown_table_message(names) do
    case String.split(names, ", ") do
      [single] -> ~s(Table "#{single}" does not exist.)
      multiple -> "Tables #{Enum.map_join(multiple, ", ", &~s("#{&1}"))} do not exist."
    end
  end

  @doc """
  Whether a backend %QueryError{} was caused by a query timeout.

  Lets callers substitute timeout guidance that suits their surface.
  """
  @spec timeout_query_error?(QueryError.t()) :: boolean()
  def timeout_query_error?(%QueryError{kind: :timeout}), do: true
  def timeout_query_error?(%QueryError{kind: :connection_error, raw_error: :timeout}), do: true
  def timeout_query_error?(%QueryError{}), do: false

  defp classified_query_error_message(%QueryError{
         backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
         raw_error: %{"reason" => "billingTierLimitExceeded", "message" => message}
       }) do
    with [_match, limit] <-
           Regex.run(~r/Query exceeded limit for bytes billed:\s*(\d+)\./, message) do
      {size, units} = limit |> String.to_integer() |> Utils.humanize_bytes()

      "total bytes processed for this query is expected to be greater than #{round(size)} #{units}"
    end
  end

  defp classified_query_error_message(%QueryError{
         kind: :invalid_query,
         description: description
       })
       when is_non_empty_binary(description),
       do: description

  defp classified_query_error_message(%QueryError{
         kind: :invalid_query,
         backend: backend,
         raw_error: raw_error
       }) do
    case raw_error_message(raw_error) do
      message when is_binary(message) -> invalid_query_message(backend, message)
      nil -> nil
    end
  end

  defp classified_query_error_message(%QueryError{} = error) do
    case timeout_query_error?(error) do
      true -> @timeout_query_error_message
      false -> nil
    end
  end

  defp invalid_query_message(Logflare.Backends.Adaptor.BigQueryAdaptor, message) do
    case message do
      "Query without FROM clause cannot have a WHERE clause" <> _rest ->
        message

      _ ->
        missing_field_message(Logflare.Backends.Adaptor.BigQueryAdaptor, message) ||
          generic_query_error_message()
    end
  end

  defp invalid_query_message(backend, message) do
    missing_field_message(backend, message)
  end

  defp missing_field_message(backend, message) do
    case extract_missing_field(backend, message) do
      nil ->
        nil

      field ->
        ~s(Field "#{field}" does not exist.)
    end
  end

  defp extract_missing_field(
         Logflare.Backends.Adaptor.BigQueryAdaptor,
         "Unrecognized name: " <> rest
       ) do
    rest
    |> first_field_token()
    |> normalize_field()
  end

  defp extract_missing_field(
         Logflare.Backends.Adaptor.BigQueryAdaptor,
         "Field name " <> rest
       ) do
    case String.split(rest, " does not exist", parts: 2) do
      [field, _] -> normalize_field(field)
      _ -> nil
    end
  end

  defp extract_missing_field(Logflare.Backends.Adaptor.BigQueryAdaptor, _message) do
    nil
  end

  defp extract_missing_field(Logflare.Backends.Adaptor.PostgresAdaptor, message) do
    message
    |> extract_field(~r/column\s+["'`]?([^"'`\s]+)["'`]?\s+does not exist/)
    |> normalize_path_field()
  end

  defp extract_missing_field(_backend, _message), do: nil

  defp extract_field(message, pattern) do
    case Regex.run(pattern, message) do
      [_match, field] -> field
      nil -> nil
    end
  end

  defp raw_error_message(%{"message" => message}) when is_binary(message), do: message

  defp raw_error_message(%Postgrex.Error{postgres: %{message: message}}) when is_binary(message),
    do: message

  defp raw_error_message(%{message: message}) when is_binary(message), do: message
  defp raw_error_message(_raw_error), do: nil

  defp normalize_field(nil), do: nil

  defp normalize_field(field) do
    Regex.replace(~r/^[`"'.]+|[`"'.,]+$/, field, "")
  end

  defp first_field_token(field) do
    field
    |> String.split([" ", ",", ";"], parts: 2)
    |> List.first()
  end

  defp normalize_path_field(nil), do: nil

  defp normalize_path_field(field) do
    field
    |> normalize_field()
    |> String.split(".")
    |> List.last()
  end
end

defmodule LogflareWeb.QueryErrorHelpers do
  @moduledoc false

  alias Logflare.Backends.QueryError
  alias LogflareWeb.Utils

  @doc """
  Returns a user-facing query error message from a backend %QueryError{}.

      iex> error = %Logflare.Backends.QueryError{
      ...>   message: "Unrecognized name: notthere at [1:8]",
      ...>   code: :invalid_query,
      ...>   raw_error: %{"message" => "Unrecognized name: notthere at [1:8]"},
      ...>   backend: Logflare.Backends.Adaptor.BigQueryAdaptor
      ...> }
      iex> LogflareWeb.QueryErrorHelpers.query_error_message(error)
      ~s(Field "notthere" does not exist.)

      iex> error = %Logflare.Backends.QueryError{
      ...>   message: "Code: 47. DB::Exception: Unknown expression identifier `notthere` in scope SELECT notthere. (UNKNOWN_IDENTIFIER)",
      ...>   code: :invalid_query,
      ...>   raw_error: %Ch.Error{message: "Code: 47. DB::Exception: Unknown expression identifier `notthere` in scope SELECT notthere. (UNKNOWN_IDENTIFIER)"},
      ...>   backend: Logflare.Backends.Adaptor.ClickHouseAdaptor
      ...> }
      iex> LogflareWeb.QueryErrorHelpers.query_error_message(error)
      ~s(Field "notthere" does not exist.)

      iex> error = %Logflare.Backends.QueryError{
      ...>   message: ~s|ERROR 42703 (undefined_column) column "notthere" does not exist\\n\\n    query: select notthere|,
      ...>   code: :invalid_query,
      ...>   raw_error: %Postgrex.Error{message: ~s|column "notthere" does not exist|},
      ...>   backend: Logflare.Backends.Adaptor.PostgresAdaptor
      ...> }
      iex> LogflareWeb.QueryErrorHelpers.query_error_message(error)
      ~s(Field "notthere" does not exist.)
  """
  @spec query_error_message(QueryError.t()) :: String.t() | nil
  def query_error_message(%QueryError{
        backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
        raw_error: %{"reason" => "billingTierLimitExceeded"},
        message: message
      }) do
    with [_match, limit] <-
           Regex.run(~r/Query exceeded limit for bytes billed:\s*(\d+)\./, message) do
      {size, units} = limit |> String.to_integer() |> Utils.humanize_bytes()

      "total bytes processed for this query is expected to be greater than #{round(size)} #{units}"
    end
  end

  def query_error_message(%QueryError{backend: backend, message: message}),
    do: missing_field_message(backend, message)

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

  defp extract_missing_field(Logflare.Backends.Adaptor.ClickHouseAdaptor, message) do
    message
    |> extract_field(~r/Unknown (?:expression )?identifier:? [`"']?([^`"'\s,;]+)/)
    |> normalize_field()
  end

  defp extract_missing_field(Logflare.Backends.Adaptor.PostgresAdaptor, message) do
    message
    |> extract_field(~r/column\s+["'`]?([^"'`\s]+)["'`]?\s+does not exist/)
    |> normalize_path_field()
  end

  defp extract_field(message, pattern) do
    case Regex.run(pattern, message) do
      [_match, field] -> field
      nil -> nil
    end
  end

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

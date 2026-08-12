defmodule Logflare.Sql.DialectTransformer.ClickHouse do
  @moduledoc """
  ClickHouse-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  alias Logflare.Sql.Parser
  alias Logflare.User

  @impl true
  def quote_style, do: nil

  @impl true
  def dialect, do: "clickhouse"

  @impl true
  def transform_source_name(source_name, _data), do: source_name

  @doc """
  Applies an exact upper bound to the top-level result while preserving any
  existing, stricter limit and offset.
  """
  @spec apply_limit(String.t(), pos_integer()) :: {:ok, String.t()} | {:error, String.t()}
  def apply_limit(query, max_rows)
      when is_binary(query) and is_integer(max_rows) and max_rows > 0 do
    with {:ok, [%{"Query" => query_ast} = statement]} <- Parser.parse(dialect(), query),
         {:ok, limit} <- build_limit(query_ast["limit"], max_rows) do
      statement
      |> put_in(["Query", "limit"], limit)
      |> then(&Parser.to_string([&1]))
    else
      {:ok, _statements} -> {:error, "Expected one ClickHouse query"}
      {:error, _reason} = error -> error
    end
  end

  defp build_limit(nil, max_rows), do: parse_limit("SELECT 1 LIMIT #{max_rows}")

  defp build_limit(existing_limit, max_rows) do
    with {:ok, limit} <- parse_limit("SELECT 1 LIMIT least(1, #{max_rows})") do
      limited =
        update_in(limit, ["Function", "args", "List", "args"], fn [_placeholder, maximum] ->
          [%{"Unnamed" => %{"Expr" => existing_limit}}, maximum]
        end)

      {:ok, limited}
    end
  end

  defp parse_limit(query) do
    with {:ok, [%{"Query" => %{"limit" => limit}}]} <- Parser.parse(dialect(), query) do
      {:ok, limit}
    end
  end

  @doc """
  Builds transformation data for ClickHouse from a user and base data.

  Since ClickHouse does not require project/dataset metadata, we can just pass through the base data.
  """
  @spec build_transformation_data(User.t(), map()) :: map()
  def build_transformation_data(%User{}, base_data), do: base_data
end

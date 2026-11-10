defmodule Logflare.Sql.Parser do
  @moduledoc """
  Provides functionality for converting SQL queries into an AST and back to SQL strings.

  This leverages the [sqlparser](https://crates.io/crates/sqlparser) Rust crate to handle SQL parsing and AST generation.
  """

  import Logflare.Utils.Guards

  @valid_dialects ~w(bigquery clickhouse postgres)

  @max_query_bytes 1_048_576

  defmodule Native do
    @moduledoc false

    use Rustler, otp_app: :logflare, crate: "sqlparser_ex"

    # When your NIF is loaded, it will override this function.
    def parse(_dialect, _query), do: :erlang.nif_error(:nif_not_loaded)
    def parse_clickhouse_converted(_query), do: :erlang.nif_error(:nif_not_loaded)
    def to_string(_query), do: :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Attempts to parse a SQL query into AST based on the specified dialect.

  Queries larger than #{@max_query_bytes} bytes are refused with an error. The native parser
  also limits queries to 20,000 significant tokens and serialized ASTs to 16 MiB. AST JSON
  must fit serde_json's 128-level recursion budget: at most 127 nested objects/arrays,
  including the outer statement array. Exceeding a budget returns `{:error, reason}`.
  """
  @spec parse(dialect :: String.t(), query :: String.t()) ::
          {:ok, map() | [map()]} | {:error, String.t()}
  def parse(_dialect, query) when is_binary(query) and byte_size(query) > @max_query_bytes do
    {:error, too_large_error()}
  end

  def parse(dialect, query) when dialect in @valid_dialects and is_non_empty_binary(query) do
    with {:ok, json} <- Native.parse(dialect, query) do
      Jason.decode(json)
    end
  end

  @doc """
  Parses final internal ClickHouse SQL after parameter conversion, including native positional bindings.
  Customer SQL must use `parse/2` before conversion. The same byte, token, AST JSON size,
  and nesting budgets apply.
  """
  @spec parse_clickhouse_converted(String.t()) :: {:ok, [map()]} | {:error, String.t()}
  def parse_clickhouse_converted(query)
      when is_binary(query) and byte_size(query) > @max_query_bytes do
    {:error, too_large_error()}
  end

  def parse_clickhouse_converted(query) when is_non_empty_binary(query) do
    with {:ok, json} <- Native.parse_clickhouse_converted(query), do: Jason.decode(json)
  end

  @doc false
  @spec max_query_bytes() :: pos_integer()
  def max_query_bytes, do: @max_query_bytes

  defp too_large_error, do: "Query is too large to parse (limit #{@max_query_bytes} bytes)."

  @doc """
  Converts an AST or list of ASTs back into a SQL string.

  Returns an error when the AST cannot be deserialized by the parser crate, for
  example when a hand-built node is missing a required field.
  """
  @spec to_string(map() | [map()]) :: {:ok, String.t()} | {:error, String.t()}
  def to_string(ast) when is_map(ast), do: __MODULE__.to_string([ast])

  def to_string(asts) when is_list(asts) do
    asts
    |> Jason.encode!()
    |> Native.to_string()
  end
end

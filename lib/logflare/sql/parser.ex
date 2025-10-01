defmodule Logflare.Sql.Parser do
  @moduledoc """
  Provides functionality for converting SQL queries into an AST and back to SQL strings.

  This leverages the [sqlparser](https://crates.io/crates/sqlparser) Rust crate to handle SQL parsing and AST generation.

  """

  import Logflare.Utils.Guards

  @valid_dialects ~w(bigquery clickhouse postgres)

  defmodule Native do
    @moduledoc false

    use Rustler, otp_app: :logflare, crate: "sqlparser_ex"

    # When your NIF is loaded, it will override this function.
    def parse(_dialect, _query), do: :erlang.nif_error(:nif_not_loaded)
    def to_string(_query), do: :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Attempts to parse a SQL query into AST based on the specified dialect.
  """
  @spec parse(dialect :: String.t(), query :: String.t()) ::
          {:ok, map() | [map()]} | {:error, String.t()}
  def parse(dialect, query) when dialect in @valid_dialects and is_non_empty_binary(query) do
    with {:ok, json} <- Native.parse(dialect, query) do
      Jason.decode(json)
    end
  end

  @doc """
  Converts an AST or list of ASTs back into a SQL string.
  """
  @spec to_string(map() | [map()]) :: {:ok, String.t()}
  def to_string(ast) when is_map(ast), do: __MODULE__.to_string([ast])

  def to_string(asts) when is_list(asts) do
    asts
    |> Jason.encode!()
    |> Native.to_string()
  end
end

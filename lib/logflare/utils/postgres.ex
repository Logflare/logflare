defmodule Logflare.Utils.Postgres do
  @moduledoc """
  Helpers for working with raw PostgreSQL syntax, for the cases where bind
  parameters are not available (DDL, `SET`, and other utility statements).
  """

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  @max_identifier_bytes 63

  @unquoted_identifier ~r/^[\p{L}_][\p{L}\p{N}_$]*$/u
  @quoted_identifier ~r/^"(?:[^"\x00]|"")+"$/u

  @doc """
  Checks whether `value` is a syntactically valid PostgreSQL identifier.

  Follows the lexical rules in
  [SQL Syntax: Identifiers and Key Words](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS):

  ## Example

  ```elixir
  iex> #{__MODULE__}.valid_identifier?("public")
  true
  iex> #{__MODULE__}.valid_identifier?("my_schema$2")
  true
  iex> #{__MODULE__}.valid_identifier?(~s("my-schema"))
  true
  iex> #{__MODULE__}.valid_identifier?(~s("say ""hi\"""))
  true
  iex> #{__MODULE__}.valid_identifier?("schéma")
  true
  iex> #{__MODULE__}.valid_identifier?(String.duplicate("a", 63))
  true

  iex> #{__MODULE__}.valid_identifier?("my-schema")
  false
  iex> #{__MODULE__}.valid_identifier?("2fast")
  false
  iex> #{__MODULE__}.valid_identifier?(~s(un"quoted))
  false
  iex> #{__MODULE__}.valid_identifier?(~s("unterminated))
  false
  iex> #{__MODULE__}.valid_identifier?(~s(""))
  false
  iex> #{__MODULE__}.valid_identifier?(String.duplicate("a", 64))
  false
  iex> #{__MODULE__}.valid_identifier?("")
  false
  iex> #{__MODULE__}.valid_identifier?(:public)
  false
  ```
  """
  @spec valid_identifier?(term()) :: boolean()
  def valid_identifier?(value) when is_non_empty_binary(value) do
    byte_size(value) <= @max_identifier_bytes and
      (Regex.match?(@unquoted_identifier, value) or Regex.match?(@quoted_identifier, value))
  end

  def valid_identifier?(_value), do: false

  @doc """
  Splits a comma-separated list of PostgreSQL identifiers, raising on any
  invalid entry.

  Used for env vars such as `DB_SCHEMA` whose values are interpolated into
  statements that cannot use bind parameters. Entries are returned verbatim so
  the server's own case folding and quoting rules still apply.

  ## Example

  ```elixir
  iex> #{__MODULE__}.parse_identifier_list!("  public ,, logflare  ")
  ["public", "logflare"]
  iex> #{__MODULE__}.parse_identifier_list!(~s(MySchema,"my-schema"))
  ["MySchema", ~s("my-schema")]
  iex> #{__MODULE__}.parse_identifier_list!("")
  []
  iex> #{__MODULE__}.parse_identifier_list!("my-schema")
  ** (ArgumentError) invalid PostgreSQL identifier: "my-schema".
  ```
  """
  @spec parse_identifier_list!(String.t()) :: [String.t()]
  def parse_identifier_list!(value) when is_binary(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(fn identifier ->
      if valid_identifier?(identifier) do
        identifier
      else
        raise ArgumentError, "invalid PostgreSQL identifier: #{inspect(identifier)}."
      end
    end)
  end
end

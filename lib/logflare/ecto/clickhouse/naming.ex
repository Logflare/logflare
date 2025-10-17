defmodule Logflare.Ecto.ClickHouse.Naming do
  @moduledoc """
  Handles name generation, quoting, escaping, and aliasing for ClickHouse SQL generation.
  """

  import Logflare.Utils.Guards

  alias Ecto.SubQuery

  @doc """
  Creates names for all sources in a query.
  """
  @spec create_names(tuple(), list()) :: tuple()
  def create_names(sources, as_prefix) do
    sources |> create_names(0, tuple_size(sources), as_prefix) |> List.to_tuple()
  end

  @doc """
  Recursively creates names for sources at specific positions.
  """
  @spec create_names(tuple(), non_neg_integer(), non_neg_integer(), list()) :: list()
  def create_names(sources, pos, limit, as_prefix) when pos < limit do
    [create_name(sources, pos, as_prefix) | create_names(sources, pos + 1, limit, as_prefix)]
  end

  def create_names(_sources, pos, pos, _as_prefix), do: []

  @doc """
  Creates a subquery alias prefix.
  """
  @spec subquery_as_prefix(tuple()) :: list()
  def subquery_as_prefix(sources) do
    last_elem = :erlang.element(tuple_size(sources), sources)
    if is_list(last_elem), do: [?s | last_elem], else: [?s]
  end

  @doc """
  Creates a name tuple for a source at a given position.
  """
  @spec create_name(tuple(), non_neg_integer(), list()) ::
          {nil | iolist(), list(), nil | module()} | {iolist(), list(), module()}
  def create_name(sources, pos, as_prefix) do
    case elem(sources, pos) do
      {:fragment, _, _} ->
        {nil, as_prefix ++ [?f | Integer.to_string(pos)], nil}

      {:values, _, _} ->
        {nil, as_prefix ++ [?v | Integer.to_string(pos)], nil}

      {table, schema, prefix} ->
        name = as_prefix ++ [create_alias(table) | Integer.to_string(pos)]
        {quote_table(prefix, table), name, schema}

      %SubQuery{} ->
        {nil, as_prefix ++ [?s | Integer.to_string(pos)], nil}
    end
  end

  @doc """
  Creates a single-character alias from a table name.
  """
  @spec create_alias(binary()) :: char()
  def create_alias(<<first, _rest::bytes>>)
      when first in ?a..?z
      when first in ?A..?Z do
    <<first>>
  end

  def create_alias(_), do: ?t

  @doc """
  Quotes an identifier name for ClickHouse SQL.
  """
  @spec quote_name(term(), char() | nil) :: iolist() | []
  def quote_name(name, quoter \\ ?")
  def quote_name(nil, _), do: []

  def quote_name(names, quoter) when is_list(names) do
    names
    |> Enum.reject(&is_nil/1)
    |> intersperse_map(?., &quote_name(&1, nil))
    |> wrap_in(quoter)
  end

  def quote_name(name, quoter) when is_atom_value(name) do
    name |> Atom.to_string() |> quote_name(quoter)
  end

  def quote_name(name, quoter) do
    wrap_in(name, quoter)
  end

  @doc """
  Quotes a qualified name (source + field).
  """
  @spec quote_qualified_name(atom() | binary(), tuple(), non_neg_integer()) :: iolist()
  def quote_qualified_name(name, sources, ix) do
    {_, source, _} = elem(sources, ix)

    case source do
      nil -> quote_name(name)
      _other -> [source, ?. | quote_name(name)]
    end
  end

  @doc """
  Generates field access syntax.
  """
  @spec field_access(atom() | binary(), tuple(), non_neg_integer()) :: iolist()
  def field_access(field, sources, ix) when is_atom_value(field) do
    quote_qualified_name(field, sources, ix)
  end

  def field_access(field, sources, ix) when is_binary(field) do
    {_, name, _} = elem(sources, ix)
    [name, ?. | quote_name(field)]
  end

  @doc """
  Quotes a table name with optional prefix.
  """
  @spec quote_table(binary() | nil, binary()) :: iolist()
  def quote_table(prefix, name)
  def quote_table(nil, name), do: quote_name(name)
  def quote_table(prefix, name), do: [quote_name(prefix), ?., quote_name(name)]

  @doc """
  Escapes a string for ClickHouse SQL.
  """
  @spec escape_string(binary()) :: binary()
  def escape_string(value) when is_binary(value) do
    value
    |> :binary.replace("'", "''", [:global])
    |> :binary.replace("\\", "\\\\", [:global])
  end

  @doc """
  Escapes a JSON key for ClickHouse SQL.
  """
  @spec escape_json_key(binary()) :: binary()
  def escape_json_key(value) when is_binary(value) do
    value
    |> escape_string()
    |> :binary.replace("\"", "\\\"", [:global])
  end

  defp wrap_in(value, nil), do: value
  defp wrap_in(value, wrapper), do: [wrapper, value, wrapper]

  defp intersperse_map([elem], _separator, mapper), do: [mapper.(elem)]

  defp intersperse_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperse_map(rest, separator, mapper)]
  end

  defp intersperse_map([], _separator, _mapper), do: []
end

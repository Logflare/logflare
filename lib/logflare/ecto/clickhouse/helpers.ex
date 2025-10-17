defmodule Logflare.Ecto.ClickHouse.Helpers do
  @moduledoc """
  Generic utility functions for ClickHouse SQL generation.
  """

  alias Ecto.QueryError

  @doc """
  Maps over a list and intersperses the results with a separator.
  """
  @spec intersperse_map(list(), term(), (term() -> term())) :: list()
  def intersperse_map([elem], _separator, mapper), do: [mapper.(elem)]

  def intersperse_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperse_map(rest, separator, mapper)]
  end

  def intersperse_map([], _separator, _mapper), do: []

  @doc """
  Reduces over a list while interspersing a separator and maintaining an accumulator.
  """
  @spec intersperse_reduce(list(), term(), term(), (term(), term() -> {term(), term()}), list()) ::
          {list(), term()}
  def intersperse_reduce(list, separator, user_acc, reducer, acc \\ [])

  def intersperse_reduce([], _separator, user_acc, _reducer, acc),
    do: {acc, user_acc}

  def intersperse_reduce([elem], _separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    {[acc | elem], user_acc}
  end

  def intersperse_reduce([elem | rest], separator, user_acc, reducer, acc) do
    {elem, user_acc} = reducer.(elem, user_acc)
    intersperse_reduce(rest, separator, user_acc, reducer, [acc, elem, separator])
  end

  @doc """
  Generates SQL for time interval expressions.
  """
  @spec interval(integer() | float() | term(), atom(), tuple(), list(), term()) :: iolist()
  def interval(count, interval, _sources, _params, _query) when is_integer(count) do
    ["INTERVAL ", Integer.to_string(count), ?\s, interval]
  end

  def interval(count, interval, _sources, _params, _query) when is_float(count) do
    count = :erlang.float_to_binary(count, [:compact, decimals: 16])
    ["INTERVAL ", count, ?\s, interval]
  end

  def interval(count, interval, sources, params, query) do
    expr_fn = fn expr_val, sources_val, params_val, query_val ->
      apply(Logflare.Ecto.ClickHouse, :expr, [expr_val, sources_val, params_val, query_val])
    end

    [
      expr_fn.(count, sources, params, query),
      " * ",
      interval(1, interval, sources, params, query)
    ]
  end

  @doc """
  Converts Ecto types to ClickHouse type strings.
  """
  @spec ecto_to_db(term(), term()) :: String.t() | iolist()
  def ecto_to_db(:integer, _query), do: "Int64"
  def ecto_to_db(:binary, _query), do: "String"
  def ecto_to_db({:parameterized, {Ch, type}}, _query), do: Ch.Types.encode(type)

  def ecto_to_db({:array, type}, query), do: ["Array(", ecto_to_db(type, query), ?)]

  def ecto_to_db(type, _query) when type in [:uuid, :string, :date, :boolean] do
    Ch.Types.encode(type)
  end

  def ecto_to_db({_ix, _field}, _query), do: "String"

  def ecto_to_db(type, query) do
    raise QueryError,
      query: query,
      message: "unknown or ambiguous (for ClickHouse) Ecto type #{inspect(type)}"
  end
end

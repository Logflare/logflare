defmodule Logflare.Ecto.ClickHouse.Params do
  @moduledoc """
  Handles parameter conversion and type mapping for ClickHouse SQL generation.
  """

  @max_uint128 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
  @max_uint64 0xFFFFFFFFFFFFFFFF
  @max_int64 0x7FFFFFFFFFFFFFFF
  @min_int128 -0x80000000000000000000000000000000
  @min_int64 -0x8000000000000000

  @doc """
  Builds a parameter placeholder with type information.
  """
  @spec build_param(non_neg_integer(), term()) :: iolist()
  def build_param(ix, param) do
    ["{$", Integer.to_string(ix), ?:, param_type(param), ?}]
  end

  @doc """
  Builds multiple parameter placeholders.
  """
  @spec build_params(non_neg_integer(), pos_integer(), list()) :: iolist()
  def build_params(ix, len, params) when len > 1 do
    [build_param(ix, Enum.at(params, ix)), ?, | build_params(ix + 1, len - 1, params)]
  end

  def build_params(ix, 1 = _len, params), do: build_param(ix, Enum.at(params, ix))
  def build_params(_ix, 0 = _len, _params), do: []

  @doc """
  Converts an Elixir value to inline ClickHouse SQL syntax.
  """
  @spec inline_param(term()) :: iolist() | String.t()
  def inline_param(nil), do: "NULL"
  def inline_param(true), do: "true"
  def inline_param(false), do: "false"

  def inline_param(s) when is_binary(s) do
    [?', escape_string(s), ?']
  end

  def inline_param(i) when is_integer(i) and (i > @max_uint64 or i < @min_int64) do
    Integer.to_string(i) <> "::" <> param_type(i)
  end

  def inline_param(i) when is_integer(i) do
    Integer.to_string(i)
  end

  def inline_param(f) when is_float(f), do: Float.to_string(f)

  def inline_param(%NaiveDateTime{microsecond: {0, 0}} = naive) do
    [?', NaiveDateTime.to_string(naive), "'::datetime"]
  end

  def inline_param(%NaiveDateTime{microsecond: {_, precision}} = naive) do
    [?', NaiveDateTime.to_string(naive), "'::DateTime64(", Integer.to_string(precision), ?)]
  end

  def inline_param(%DateTime{microsecond: {0, 0}, time_zone: time_zone} = dt) do
    [
      ?',
      datetime_to_string(dt),
      "'::DateTime('",
      escape_string(time_zone),
      "')"
    ]
  end

  def inline_param(%DateTime{microsecond: {_, precision}, time_zone: time_zone} = dt) do
    [
      ?',
      datetime_to_string(dt),
      "'::DateTime64(",
      Integer.to_string(precision),
      ",'",
      escape_string(time_zone),
      "')"
    ]
  end

  def inline_param(%Date{year: year} = date) when year < 1970 or year > 2148 do
    [?', Date.to_string(date), "'::date32"]
  end

  def inline_param(%Date{} = date) do
    [?', Date.to_string(date), "'::date"]
  end

  def inline_param(%Decimal{} = dec), do: Decimal.to_string(dec, :normal)

  def inline_param(a) when is_list(a) do
    [?[, Enum.map_intersperse(a, ?,, &inline_param/1), ?]]
  end

  def inline_param(t) when is_tuple(t) do
    [?(, t |> Tuple.to_list() |> Enum.map_intersperse(?,, &inline_param/1), ?)]
  end

  def inline_param(%s{}) do
    raise ArgumentError, "struct #{inspect(s)} is not supported in params"
  end

  def inline_param(m) when is_map(m) do
    [
      "map(",
      Enum.map_intersperse(m, ?,, fn {k, v} ->
        [inline_param(k), ?,, inline_param(v)]
      end),
      ?)
    ]
  end

  @doc """
  Determines the ClickHouse type for a given value.
  """
  @spec param_type(term()) :: iolist() | String.t()
  def param_type(s) when is_binary(s), do: "String"

  def param_type(i) when is_integer(i) do
    cond do
      i > @max_uint128 -> "UInt256"
      i > @max_uint64 -> "UInt128"
      i > @max_int64 -> "UInt64"
      i < @min_int128 -> "Int256"
      i < @min_int64 -> "Int128"
      true -> "Int64"
    end
  end

  def param_type(f) when is_float(f), do: "Float64"
  def param_type(b) when is_boolean(b), do: "Bool"
  def param_type(%Date{}), do: "Date"

  def param_type(%DateTime{microsecond: {_val, precision}}) when precision > 0 do
    ["DateTime64(", Integer.to_string(precision), ?)]
  end

  def param_type(%DateTime{}), do: "DateTime64(6)"

  def param_type(%NaiveDateTime{microsecond: {_val, precision}}) when precision > 0 do
    ["DateTime64(", Integer.to_string(precision), ?)]
  end

  def param_type(%NaiveDateTime{}), do: "DateTime64(6)"

  def param_type(%Decimal{exp: exp}) do
    scale = if exp < 0, do: abs(exp), else: 0
    ["Decimal64(", Integer.to_string(scale), ?)]
  end

  def param_type([]), do: "Array(Nothing)"
  def param_type([v | _]), do: ["Array(", param_type(v), ?)]

  def param_type(%s{}) do
    raise ArgumentError, "struct #{inspect(s)} is not supported in params"
  end

  def param_type(m) when is_map(m) do
    case Map.keys(m) do
      [k | _] ->
        [v | _] = Map.values(m)
        ["Map(", param_type(k), ?,, param_type(v), ?)]

      [] ->
        "Map(Nothing,Nothing)"
    end
  end

  def param_type(nil) do
    raise ArgumentError, "param at index is nil - params list may be incorrectly built"
  end

  defp datetime_to_string(%DateTime{} = dt) do
    dt |> DateTime.to_naive() |> NaiveDateTime.to_string()
  end

  defp escape_string(value) when is_binary(value) do
    value
    |> :binary.replace("'", "''", [:global])
    |> :binary.replace("\\", "\\\\", [:global])
  end
end

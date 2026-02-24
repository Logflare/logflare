defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder do
  @moduledoc """
  Encodes Elixir data into column-oriented data blocks for ClickHouse INSERT operations.

  Produces complete client Data packets (type 2) in the native TCP wire format,
  ready to be sent over the socket.

  Uses `Protocol` primitives for all low-level encoding so no new wire format logic is introduced in this module.
  """

  import Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodingUtils, only: [sanitize_for_json: 1]

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol

  @type column :: {name :: String.t(), type :: String.t(), values :: [term()]}

  @doc """
  Encodes a data block with columns of data into a complete client Data packet.

  The `negotiated_rev` is the protocol revision negotiated during the handshake,
  used to determine whether to include the custom serialization flag per column.
  """
  @spec encode_data_block([column()], non_neg_integer()) :: iodata()
  def encode_data_block(columns, negotiated_rev) do
    [data_packet_prefix(), encode_block_body(columns, negotiated_rev)]
  end

  @doc """
  Encodes an empty block (end-of-data signal) as a complete client Data packet.
  """
  @spec encode_empty_block(non_neg_integer()) :: iodata()
  def encode_empty_block(_negotiated_rev) do
    [data_packet_prefix(), encode_empty_block_body()]
  end

  @doc """
  Encodes just the block body (BlockInfo + columns + data) without the packet
  type prefix or temp_table_name. Used by `Connection` when compression is
  enabled — the body gets compressed while the prefix stays uncompressed.
  """
  @spec encode_block_body([column()], non_neg_integer()) :: iodata()
  def encode_block_body(columns, negotiated_rev) do
    num_columns = length(columns)
    num_rows = columns |> List.first() |> elem(2) |> length()
    include_custom_serialization? = negotiated_rev >= 54_454 and num_rows > 0

    [
      encode_block_info(),
      Protocol.encode_varuint(num_columns),
      Protocol.encode_varuint(num_rows),
      Enum.map(columns, fn {name, type, values} ->
        [
          Protocol.encode_string(name),
          Protocol.encode_string(type),
          if(include_custom_serialization?, do: Protocol.encode_uint8(0), else: []),
          encode_column_values(type, values)
        ]
      end)
    ]
  end

  @doc """
  Encodes just the empty block body (BlockInfo + 0 columns + 0 rows).
  """
  @spec encode_empty_block_body() :: iodata()
  def encode_empty_block_body do
    [
      encode_block_info(),
      Protocol.encode_varuint(0),
      Protocol.encode_varuint(0)
    ]
  end

  @doc """
  Returns the Data packet prefix: packet type (VarUInt 2) + empty temp_table_name.
  """
  @spec data_packet_prefix() :: iodata()
  def data_packet_prefix do
    [
      Protocol.encode_varuint(Protocol.client_data()),
      Protocol.encode_string("")
    ]
  end

  @doc """
  Encodes a list of values for a single column type.
  """
  @spec encode_column_values(String.t(), [term()]) :: iodata()
  def encode_column_values("UInt8", values), do: Enum.map(values, &Protocol.encode_uint8/1)
  def encode_column_values("UInt16", values), do: Enum.map(values, &Protocol.encode_uint16/1)
  def encode_column_values("UInt32", values), do: Enum.map(values, &Protocol.encode_uint32/1)
  def encode_column_values("UInt64", values), do: Enum.map(values, &Protocol.encode_uint64/1)
  def encode_column_values("Int8", values), do: Enum.map(values, &Protocol.encode_int8/1)
  def encode_column_values("Int16", values), do: Enum.map(values, &Protocol.encode_int16/1)
  def encode_column_values("Int32", values), do: Enum.map(values, &Protocol.encode_int32/1)
  def encode_column_values("Int64", values), do: Enum.map(values, &Protocol.encode_int64/1)
  def encode_column_values("Float32", values), do: Enum.map(values, &Protocol.encode_float32/1)
  def encode_column_values("Float64", values), do: Enum.map(values, &Protocol.encode_float64/1)
  def encode_column_values("String", values), do: Enum.map(values, &Protocol.encode_string/1)
  def encode_column_values("Bool", values), do: Enum.map(values, &Protocol.encode_bool/1)
  def encode_column_values("DateTime", values), do: Enum.map(values, &Protocol.encode_datetime/1)
  def encode_column_values("UUID", values), do: Enum.map(values, &Protocol.encode_uuid/1)

  def encode_column_values("DateTime64" <> params, values) do
    precision = parse_datetime64_precision(params)
    Enum.map(values, &Protocol.encode_datetime64(&1, precision))
  end

  def encode_column_values("FixedString(" <> rest, values) do
    n = rest |> String.trim_trailing(")") |> String.to_integer()
    Enum.map(values, &Protocol.encode_fixed_string(&1, n))
  end

  def encode_column_values("LowCardinality(" <> rest, values) do
    inner_type = extract_inner_type(rest)
    encode_column_values(inner_type, values)
  end

  def encode_column_values("Nullable(" <> rest, values) do
    inner_type = extract_inner_type(rest)
    default = default_value_for_type(inner_type)

    null_mask =
      Enum.map(values, fn
        nil -> Protocol.encode_uint8(1)
        _ -> Protocol.encode_uint8(0)
      end)

    filled_values =
      Enum.map(values, fn
        nil -> default
        v -> v
      end)

    [null_mask, encode_column_values(inner_type, filled_values)]
  end

  def encode_column_values("Enum8(" <> _rest, values) do
    Enum.map(values, &Protocol.encode_enum8/1)
  end

  def encode_column_values("JSON" <> _rest, values) do
    json_strings = Enum.map(values, &(sanitize_for_json(&1) |> Jason.encode!()))
    encode_column_values("String", json_strings)
  end

  def encode_column_values("Array(" <> rest, values) do
    inner_type = extract_inner_type(rest)
    {offsets, flat} = build_array_offsets_and_data(values)

    [
      Enum.map(offsets, &Protocol.encode_uint64/1),
      encode_column_values(inner_type, flat)
    ]
  end

  @spec encode_block_info() :: iodata()
  defp encode_block_info do
    [
      Protocol.encode_varuint(1),
      Protocol.encode_uint8(0),
      Protocol.encode_varuint(2),
      Protocol.encode_int32(-1),
      Protocol.encode_varuint(0)
    ]
  end

  @spec parse_datetime64_precision(String.t()) :: non_neg_integer()
  defp parse_datetime64_precision("(" <> rest) do
    rest
    |> String.split([",", ")"], parts: 2)
    |> List.first()
    |> String.trim()
    |> String.to_integer()
  end

  @doc """
  Extracts the inner type from a wrapper type string.

  Handles balanced parentheses for nested types like `"Nullable(DateTime64(9))"`.
  Input is the string after the outer `(`, e.g. `"DateTime64(9))"`.
  """
  @spec extract_inner_type(String.t()) :: String.t()
  def extract_inner_type(rest) do
    extract_inner_type_walk(rest, 0, [])
  end

  @spec extract_inner_type_walk(String.t(), non_neg_integer(), [char()]) :: String.t()
  defp extract_inner_type_walk(<<")"::utf8, _rest::binary>>, 0, acc) do
    acc |> Enum.reverse() |> IO.iodata_to_binary()
  end

  defp extract_inner_type_walk(<<")"::utf8, rest::binary>>, depth, acc) do
    extract_inner_type_walk(rest, depth - 1, [?) | acc])
  end

  defp extract_inner_type_walk(<<"("::utf8, rest::binary>>, depth, acc) do
    extract_inner_type_walk(rest, depth + 1, [?( | acc])
  end

  defp extract_inner_type_walk(<<char::utf8, rest::binary>>, depth, acc) do
    extract_inner_type_walk(rest, depth, [char | acc])
  end

  @spec default_value_for_type(String.t()) :: term()
  defp default_value_for_type("String"), do: ""
  defp default_value_for_type("Bool"), do: false
  defp default_value_for_type("UUID"), do: <<0::128>>
  defp default_value_for_type("Float32"), do: 0.0
  defp default_value_for_type("Float64"), do: 0.0
  defp default_value_for_type(_), do: 0

  @spec build_array_offsets_and_data([list()]) :: {[non_neg_integer()], [term()]}
  defp build_array_offsets_and_data(arrays) do
    {offsets_rev, flat_rev} =
      Enum.reduce(arrays, {[], []}, fn arr, {offsets, flat} ->
        prev =
          case offsets do
            [last | _] -> last
            [] -> 0
          end

        new_offset = prev + length(arr)
        {[new_offset | offsets], Enum.reverse(arr) ++ flat}
      end)

    {Enum.reverse(offsets_rev), Enum.reverse(flat_rev)}
  end
end

defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder do
  @moduledoc """
  Encodes Elixir data into column-oriented data blocks for ClickHouse INSERT operations.

  Produces complete client Data packets (type 2) in the native TCP wire format,
  ready to be sent over the socket.

  Uses `Protocol` primitives for all low-level encoding so no new wire format logic is introduced in this module.
  """

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol

  @type column :: {name :: String.t(), type :: String.t(), values :: [term()]}

  @doc """
  Encodes a data block with columns of data into a complete client Data packet.

  The `negotiated_rev` is the protocol revision negotiated during the handshake,
  used to determine whether to include the custom serialization flag per column.
  """
  @spec encode_data_block([column()], non_neg_integer()) :: iodata()
  def encode_data_block(columns, negotiated_rev) do
    num_columns = length(columns)
    num_rows = columns |> List.first() |> elem(2) |> length()
    include_custom_serialization? = negotiated_rev >= 54_454 and num_rows > 0

    [
      Protocol.encode_varuint(Protocol.client_data()),
      Protocol.encode_string(""),
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
  Encodes an empty block (end-of-data signal) as a complete client Data packet.
  """
  @spec encode_empty_block(non_neg_integer()) :: iodata()
  def encode_empty_block(_negotiated_rev) do
    [
      Protocol.encode_varuint(Protocol.client_data()),
      Protocol.encode_string(""),
      encode_block_info(),
      Protocol.encode_varuint(0),
      Protocol.encode_varuint(0)
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
end

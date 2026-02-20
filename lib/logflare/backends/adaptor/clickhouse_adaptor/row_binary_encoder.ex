defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder do
  @moduledoc """
  RowBinary encoding functions for ClickHouse data types.

  ClickHouse RowBinary format uses little-endian encoding for all multi-byte
  integers and IEEE 754 format for floating-point numbers. Strings and arrays
  use varint-encoded length prefixes.

  Reference: https://clickhouse.com/docs/en/interfaces/formats#rowbinary
  """

  import Bitwise

  import Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodingUtils, only: [sanitize_for_json: 1]
  import Logflare.Utils.Guards

  @epoch_date ~D[1970-01-01]
  @unix_unit_scales %{
    second: 0,
    millisecond: 3,
    microsecond: 6,
    nanosecond: 9
  }
  @precision_scales %{
    0 => 1,
    1 => 10,
    2 => 100,
    3 => 1_000,
    4 => 10_000,
    5 => 100_000,
    6 => 1_000_000,
    7 => 10_000_000,
    8 => 100_000_000,
    9 => 1_000_000_000
  }

  @type precision :: 0..9
  @type enum8_value :: -128..127

  defguardp is_uint8_range(n) when is_integer(n) and n >= 0 and n <= 255
  defguardp is_int8_range(n) when is_integer(n) and n >= -128 and n <= 127
  defguardp is_int16_range(n) when is_integer(n) and n >= -32_768 and n <= 32_767

  # =============================================================================
  # Varint Encoding
  # =============================================================================

  @spec varint(non_neg_integer()) :: binary()
  def varint(n) when is_non_negative_integer(n) and n < 128, do: <<n>>

  def varint(n) when is_non_negative_integer(n),
    do: <<1::1, n::7, varint(n >>> 7)::binary>>

  # =============================================================================
  # UUID
  # =============================================================================

  @spec uuid(Ecto.UUID.t() | String.t()) :: binary()
  def uuid(uuid_string) when is_non_empty_binary(uuid_string) do
    uuid_raw =
      uuid_string
      |> String.replace("-", "")
      |> Base.decode16!(case: :mixed)

    case uuid_raw do
      <<u1::64, u2::64>> ->
        <<u1::64-little, u2::64-little>>

      _other ->
        raise ArgumentError, "invalid UUID: #{inspect(uuid_string)}"
    end
  end

  # =============================================================================
  # String
  # =============================================================================

  @spec string(String.t() | iodata()) :: iodata()
  def string(value) when is_binary(value) do
    [varint(byte_size(value)), value]
  end

  def string(value) when is_list(value) do
    length = IO.iodata_length(value)
    [varint(length), value]
  end

  # =============================================================================
  # FixedString(N) - fixed length, null-padded
  # =============================================================================

  @spec fixed_string(String.t(), pos_integer()) :: binary()
  def fixed_string(value, n) when is_binary(value) and is_pos_integer(n) do
    byte_size = byte_size(value)

    cond do
      byte_size == n -> value
      byte_size < n -> value <> :binary.copy(<<0>>, n - byte_size)
      byte_size > n -> binary_part(value, 0, n)
    end
  end

  # =============================================================================
  # Boolean
  # =============================================================================

  @spec bool(boolean()) :: binary()
  def bool(true), do: <<1>>
  def bool(false), do: <<0>>

  # =============================================================================
  # Unsigned Integers
  # =============================================================================

  @spec uint8(non_neg_integer()) :: binary()
  def uint8(n) when is_uint8_range(n), do: <<n::8>>

  @spec uint16(non_neg_integer()) :: binary()
  def uint16(n) when is_non_negative_integer(n), do: <<n::little-unsigned-16>>

  @spec uint32(non_neg_integer()) :: binary()
  def uint32(n) when is_non_negative_integer(n), do: <<n::little-unsigned-32>>

  @spec uint64(non_neg_integer()) :: binary()
  def uint64(n) when is_non_negative_integer(n), do: <<n::little-unsigned-64>>

  @spec uint128(non_neg_integer()) :: binary()
  def uint128(n) when is_non_negative_integer(n), do: <<n::little-unsigned-128>>

  @spec uint256(non_neg_integer()) :: binary()
  def uint256(n) when is_non_negative_integer(n), do: <<n::little-unsigned-256>>

  # =============================================================================
  # Signed Integers
  # =============================================================================

  @spec int8(integer()) :: binary()
  def int8(n) when is_integer(n), do: <<n::signed-8>>

  @spec int16(integer()) :: binary()
  def int16(n) when is_integer(n), do: <<n::little-signed-16>>

  @spec int32(integer()) :: binary()
  def int32(n) when is_integer(n), do: <<n::little-signed-32>>

  @spec int64(integer()) :: binary()
  def int64(n) when is_integer(n), do: <<n::little-signed-64>>

  @spec int128(integer()) :: binary()
  def int128(n) when is_integer(n), do: <<n::little-signed-128>>

  @spec int256(integer()) :: binary()
  def int256(n) when is_integer(n), do: <<n::little-signed-256>>

  # =============================================================================
  # Floating Point
  # =============================================================================

  @spec float32(number()) :: binary()
  def float32(n) when is_float(n), do: <<n::little-float-32>>
  def float32(n) when is_integer(n), do: <<n * 1.0::little-float-32>>

  @spec float64(number()) :: binary()
  def float64(n) when is_float(n), do: <<n::little-float-64>>
  def float64(n) when is_integer(n), do: <<n * 1.0::little-float-64>>

  # =============================================================================
  # Enum8 / Enum16
  # =============================================================================

  @spec enum8(enum8_value()) :: binary()
  def enum8(n) when is_int8_range(n), do: <<n::signed-8>>

  @spec enum16(integer()) :: binary()
  def enum16(n) when is_int16_range(n), do: <<n::little-signed-16>>

  # =============================================================================
  # IPv4 (4 bytes, big-endian network order)
  # =============================================================================

  @spec ipv4(:inet.ip4_address() | String.t()) :: binary()
  def ipv4({a, b, c, d})
      when is_integer(a) and is_integer(b) and is_integer(c) and is_integer(d) do
    <<a, b, c, d>>
  end

  def ipv4(address) when is_binary(address) do
    {:ok, ip_tuple} = :inet.parse_ipv4_address(String.to_charlist(address))
    ipv4(ip_tuple)
  end

  # =============================================================================
  # IPv6 (16 bytes, big-endian network order)
  # =============================================================================

  @spec ipv6(:inet.ip6_address() | String.t()) :: binary()
  def ipv6({a, b, c, d, e, f, g, h})
      when is_integer(a) and is_integer(b) and is_integer(c) and is_integer(d) and
             is_integer(e) and is_integer(f) and is_integer(g) and is_integer(h) do
    <<a::16, b::16, c::16, d::16, e::16, f::16, g::16, h::16>>
  end

  def ipv6(address) when is_binary(address) do
    {:ok, ip_tuple} = :inet.parse_ipv6_address(String.to_charlist(address))
    ipv6(ip_tuple)
  end

  # =============================================================================
  # Date (days since epoch, unsigned 16-bit)
  # =============================================================================

  @spec date(Date.t()) :: binary()
  def date(%Date{} = value) do
    days = Date.diff(value, @epoch_date)
    <<days::little-unsigned-16>>
  end

  # =============================================================================
  # Date32 (days since epoch, signed 32-bit, extended range)
  # =============================================================================

  @spec date32(Date.t()) :: binary()
  def date32(%Date{} = value) do
    days = Date.diff(value, @epoch_date)
    <<days::little-signed-32>>
  end

  # =============================================================================
  # Time (seconds since midnight, signed 32-bit)
  # =============================================================================

  @spec time(Time.t()) :: binary()
  def time(%Time{hour: hour, minute: minute, second: second}) do
    seconds = hour * 3600 + minute * 60 + second
    <<seconds::little-signed-32>>
  end

  # =============================================================================
  # DateTime (seconds precision, unsigned 32-bit)
  # =============================================================================

  @spec datetime(DateTime.t()) :: binary()
  def datetime(%DateTime{} = value) do
    <<DateTime.to_unix(value, :second)::little-unsigned-32>>
  end

  # =============================================================================
  # DateTime64 (variable precision, signed 64-bit)
  # =============================================================================

  @spec datetime64(DateTime.t(), precision()) :: binary()
  def datetime64(value, precision \\ 6)

  def datetime64(%DateTime{microsecond: {microsecond, _}} = value, precision)
      when is_integer(precision) and precision >= 0 and precision <= 9 do
    scale = Map.fetch!(@precision_scales, precision)
    timestamp_seconds = DateTime.to_unix(value, :second)

    sub_second_value =
      cond do
        precision == 6 ->
          microsecond

        precision < 6 ->
          div(microsecond, Map.fetch!(@precision_scales, 6 - precision))

        precision > 6 ->
          microsecond * Map.fetch!(@precision_scales, precision - 6)
      end

    timestamp_scaled = timestamp_seconds * scale + sub_second_value
    <<timestamp_scaled::little-signed-64>>
  end

  @spec datetime64_from_unix(
          integer(),
          :second | :millisecond | :microsecond | :nanosecond,
          precision()
        ) :: binary()
  def datetime64_from_unix(value, unit, precision)
      when is_integer(value) and is_integer(precision) and precision >= 0 and precision <= 9 do
    source_decimals = Map.fetch!(@unix_unit_scales, unit)

    scaled =
      cond do
        source_decimals == precision ->
          value

        source_decimals < precision ->
          value * Map.fetch!(@precision_scales, precision - source_decimals)

        source_decimals > precision ->
          div(value, Map.fetch!(@precision_scales, source_decimals - precision))
      end

    <<scaled::little-signed-64>>
  end

  # =============================================================================
  # JSON (encoded as String)
  # =============================================================================

  @spec json(term()) :: iodata()
  def json(value) do
    string(Jason.encode_to_iodata!(sanitize_for_json(value)))
  end

  # =============================================================================
  # Nullable (1-byte null flag + value if not null)
  # =============================================================================

  @spec nullable(term(), (term() -> binary() | iodata())) :: iodata()
  def nullable(nil, _encoder), do: <<1>>

  def nullable(value, encoder) when is_function(encoder, 1) do
    [<<0>>, encoder.(value)]
  end

  # =============================================================================
  # Arrays
  # =============================================================================

  @spec array(list(), (term() -> binary() | iodata())) :: iodata()
  def array([], _element_encoder), do: [<<0>>]

  def array(items, element_encoder) when is_list(items) and is_function(element_encoder, 1) do
    [varint(length(items)) | Enum.map(items, element_encoder)]
  end

  @spec array_string([String.t()]) :: iodata()
  def array_string(items) when is_list(items), do: array(items, &string/1)

  @spec array_uint64([non_neg_integer()]) :: iodata()
  def array_uint64(items) when is_list(items), do: array(items, &uint64/1)

  @spec array_float64([number()]) :: iodata()
  def array_float64(items) when is_list(items), do: array(items, &float64/1)

  @spec array_json([term()]) :: iodata()
  def array_json(items) when is_list(items), do: array(items, &json/1)

  @spec array_datetime64([DateTime.t()], precision()) :: iodata()
  def array_datetime64(items, precision \\ 6) when is_list(items) do
    array(items, &datetime64(&1, precision))
  end

  # =============================================================================
  # Maps
  # =============================================================================

  @spec map(
          map() | [{term(), term()}],
          key_encoder :: (term() -> binary() | iodata()),
          value_encoder :: (term() -> binary() | iodata())
        ) :: iodata()
  def map(items, _key_encoder, _value_encoder) when items == %{}, do: [<<0>>]
  def map([], _key_encoder, _value_encoder), do: [<<0>>]

  def map(items, key_encoder, value_encoder)
      when is_map(items) and is_function(key_encoder, 1) and is_function(value_encoder, 1) do
    pairs = Map.to_list(items)
    encode_map_pairs(pairs, key_encoder, value_encoder)
  end

  def map(items, key_encoder, value_encoder)
      when is_list(items) and is_function(key_encoder, 1) and is_function(value_encoder, 1) do
    encode_map_pairs(items, key_encoder, value_encoder)
  end

  defp encode_map_pairs(pairs, key_encoder, value_encoder) do
    {keys, values} = Enum.unzip(pairs)
    [varint(length(pairs)), Enum.map(keys, key_encoder), Enum.map(values, value_encoder)]
  end

  @spec map_string_string(map() | [{String.t(), String.t()}]) :: iodata()
  def map_string_string(items), do: map(items, &string/1, &string/1)

  @spec map_string_uint64(map() | [{String.t(), non_neg_integer()}]) :: iodata()
  def map_string_uint64(items), do: map(items, &string/1, &uint64/1)

  @spec map_string_int32(map() | [{String.t(), integer()}]) :: iodata()
  def map_string_int32(items), do: map(items, &string/1, &int32/1)

  @spec map_string_float64(map() | [{String.t(), number()}]) :: iodata()
  def map_string_float64(items), do: map(items, &string/1, &float64/1)

  @spec map_string_bool(map() | [{String.t(), boolean()}]) :: iodata()
  def map_string_bool(items), do: map(items, &string/1, &bool/1)

  @spec map_string_json(map() | [{String.t(), term()}]) :: iodata()
  def map_string_json(items), do: map(items, &string/1, &json/1)
end

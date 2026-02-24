defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol do
  @moduledoc """
  Low-level wire format primitives for the ClickHouse native TCP protocol.

  Provides VarUInt (unsigned LEB128), string, fixed-size integer, float, and
  boolean encoding/decoding — plus packet type and protocol version constants.

  All multi-byte integers use little-endian byte order per the protocol spec.
  """

  import Bitwise
  import Logflare.Utils.Guards

  # Client packet types
  @client_hello 0
  @client_query 1
  @client_data 2
  @client_cancel 3
  @client_ping 4

  # Server packet types
  @server_hello 0
  @server_data 1
  @server_exception 2
  @server_progress 3
  @server_pong 4
  @server_end_of_stream 5
  @server_profile_info 6
  @server_totals 7
  @server_extremes 8
  @server_tables_status_response 9
  @server_log 10
  @server_table_columns 11
  @server_profile_events 14

  # Protocol version constants
  @dbms_tcp_protocol_version 54_483
  @min_revision_with_block_info 51_903
  @min_revision_with_client_info 54_032
  @min_revision_with_server_timezone 54_058
  @min_revision_with_server_display_name 54_372
  @min_revision_with_version_patch 54_401
  @min_revision_with_low_cardinality_type 54_405
  @min_revision_with_settings_serialized_as_strings 54_429
  @min_revision_with_opentelemetry 54_442
  @min_revision_with_interserver_secret 54_441
  @min_revision_with_distributed_depth 54_448
  @min_revision_with_initial_query_start_time 54_449
  @min_revision_with_custom_serialization 54_454
  @min_protocol_version_with_addendum 54_458
  @min_protocol_version_with_parameters 54_459
  @min_protocol_version_with_password_complexity_rules 54_461
  @min_revision_with_interserver_secret_v2 54_462
  @min_revision_with_server_query_time_in_progress 54_463
  @min_protocol_version_with_chunked_packets 54_470
  @min_revision_with_versioned_parallel_replicas_protocol 54_471
  @min_revision_with_server_settings 54_474
  @min_revision_with_query_plan_serialization 54_477
  @min_revision_with_versioned_cluster_function_protocol 54_479

  # Client identity (sent in Hello packet)
  @client_name "logflare-native"
  @client_version_major 24
  @client_version_minor 1
  @client_version_patch 0

  # ---------------------------------------------------------------------------
  # Client packet type accessors
  # ---------------------------------------------------------------------------

  @spec client_hello() :: non_neg_integer()
  def client_hello, do: @client_hello

  @spec client_query() :: non_neg_integer()
  def client_query, do: @client_query

  @spec client_data() :: non_neg_integer()
  def client_data, do: @client_data

  @spec client_cancel() :: non_neg_integer()
  def client_cancel, do: @client_cancel

  @spec client_ping() :: non_neg_integer()
  def client_ping, do: @client_ping

  # ---------------------------------------------------------------------------
  # Server packet type accessors
  # ---------------------------------------------------------------------------

  @spec server_hello() :: non_neg_integer()
  def server_hello, do: @server_hello

  @spec server_data() :: non_neg_integer()
  def server_data, do: @server_data

  @spec server_exception() :: non_neg_integer()
  def server_exception, do: @server_exception

  @spec server_progress() :: non_neg_integer()
  def server_progress, do: @server_progress

  @spec server_pong() :: non_neg_integer()
  def server_pong, do: @server_pong

  @spec server_end_of_stream() :: non_neg_integer()
  def server_end_of_stream, do: @server_end_of_stream

  @spec server_profile_info() :: non_neg_integer()
  def server_profile_info, do: @server_profile_info

  @spec server_totals() :: non_neg_integer()
  def server_totals, do: @server_totals

  @spec server_extremes() :: non_neg_integer()
  def server_extremes, do: @server_extremes

  @spec server_tables_status_response() :: non_neg_integer()
  def server_tables_status_response, do: @server_tables_status_response

  @spec server_log() :: non_neg_integer()
  def server_log, do: @server_log

  @spec server_table_columns() :: non_neg_integer()
  def server_table_columns, do: @server_table_columns

  @spec server_profile_events() :: non_neg_integer()
  def server_profile_events, do: @server_profile_events

  # ---------------------------------------------------------------------------
  # Protocol version constant accessors
  # ---------------------------------------------------------------------------

  @spec dbms_tcp_protocol_version() :: pos_integer()
  def dbms_tcp_protocol_version, do: @dbms_tcp_protocol_version

  @spec min_revision_with_block_info() :: pos_integer()
  def min_revision_with_block_info, do: @min_revision_with_block_info

  @spec min_revision_with_client_info() :: pos_integer()
  def min_revision_with_client_info, do: @min_revision_with_client_info

  @spec min_revision_with_server_timezone() :: pos_integer()
  def min_revision_with_server_timezone, do: @min_revision_with_server_timezone

  @spec min_revision_with_server_display_name() :: pos_integer()
  def min_revision_with_server_display_name, do: @min_revision_with_server_display_name

  @spec min_revision_with_version_patch() :: pos_integer()
  def min_revision_with_version_patch, do: @min_revision_with_version_patch

  @spec min_revision_with_low_cardinality_type() :: pos_integer()
  def min_revision_with_low_cardinality_type, do: @min_revision_with_low_cardinality_type

  @spec min_revision_with_settings_serialized_as_strings() :: pos_integer()
  def min_revision_with_settings_serialized_as_strings,
    do: @min_revision_with_settings_serialized_as_strings

  @spec min_revision_with_opentelemetry() :: pos_integer()
  def min_revision_with_opentelemetry, do: @min_revision_with_opentelemetry

  @spec min_revision_with_interserver_secret() :: pos_integer()
  def min_revision_with_interserver_secret, do: @min_revision_with_interserver_secret

  @spec min_revision_with_distributed_depth() :: pos_integer()
  def min_revision_with_distributed_depth, do: @min_revision_with_distributed_depth

  @spec min_revision_with_initial_query_start_time() :: pos_integer()
  def min_revision_with_initial_query_start_time, do: @min_revision_with_initial_query_start_time

  @spec min_revision_with_custom_serialization() :: pos_integer()
  def min_revision_with_custom_serialization, do: @min_revision_with_custom_serialization

  @spec min_protocol_version_with_addendum() :: pos_integer()
  def min_protocol_version_with_addendum, do: @min_protocol_version_with_addendum

  @spec min_protocol_version_with_parameters() :: pos_integer()
  def min_protocol_version_with_parameters, do: @min_protocol_version_with_parameters

  @spec min_protocol_version_with_password_complexity_rules() :: pos_integer()
  def min_protocol_version_with_password_complexity_rules,
    do: @min_protocol_version_with_password_complexity_rules

  @spec min_revision_with_interserver_secret_v2() :: pos_integer()
  def min_revision_with_interserver_secret_v2, do: @min_revision_with_interserver_secret_v2

  @spec min_revision_with_server_query_time_in_progress() :: pos_integer()
  def min_revision_with_server_query_time_in_progress,
    do: @min_revision_with_server_query_time_in_progress

  @spec min_protocol_version_with_chunked_packets() :: pos_integer()
  def min_protocol_version_with_chunked_packets, do: @min_protocol_version_with_chunked_packets

  @spec min_revision_with_versioned_parallel_replicas_protocol() :: pos_integer()
  def min_revision_with_versioned_parallel_replicas_protocol,
    do: @min_revision_with_versioned_parallel_replicas_protocol

  @spec min_revision_with_server_settings() :: pos_integer()
  def min_revision_with_server_settings, do: @min_revision_with_server_settings

  @spec min_revision_with_query_plan_serialization() :: pos_integer()
  def min_revision_with_query_plan_serialization, do: @min_revision_with_query_plan_serialization

  @spec min_revision_with_versioned_cluster_function_protocol() :: pos_integer()
  def min_revision_with_versioned_cluster_function_protocol,
    do: @min_revision_with_versioned_cluster_function_protocol

  # ---------------------------------------------------------------------------
  # Client identity accessors
  # ---------------------------------------------------------------------------

  @spec client_name() :: String.t()
  def client_name, do: @client_name

  @spec client_version_major() :: non_neg_integer()
  def client_version_major, do: @client_version_major

  @spec client_version_minor() :: non_neg_integer()
  def client_version_minor, do: @client_version_minor

  @spec client_version_patch() :: non_neg_integer()
  def client_version_patch, do: @client_version_patch

  # ---------------------------------------------------------------------------
  # VarUInt — Unsigned LEB128
  # ---------------------------------------------------------------------------

  @spec encode_varuint(non_neg_integer()) :: binary()
  def encode_varuint(value) when is_non_negative_integer(value) and value < 128 do
    <<value::8>>
  end

  def encode_varuint(value) when is_non_negative_integer(value) do
    <<1::1, value::7, encode_varuint(value >>> 7)::binary>>
  end

  @spec decode_varuint(binary()) :: {non_neg_integer(), binary()}
  def decode_varuint(binary) when is_binary(binary) do
    decode_varuint(binary, 0, 0)
  end

  defp decode_varuint(<<0::1, byte::7, rest::binary>>, shift, acc) do
    {acc + (byte <<< shift), rest}
  end

  defp decode_varuint(<<1::1, byte::7, rest::binary>>, shift, acc) do
    decode_varuint(rest, shift + 7, acc + (byte <<< shift))
  end

  # ---------------------------------------------------------------------------
  # Strings — VarUInt(byte_length) + UTF-8 bytes
  # ---------------------------------------------------------------------------

  @spec encode_string(binary()) :: binary()
  def encode_string(str) when is_binary(str) do
    encode_varuint(byte_size(str)) <> str
  end

  @spec decode_string(binary()) :: {String.t(), binary()}
  def decode_string(binary) when is_binary(binary) do
    {length, rest} = decode_varuint(binary)
    <<str::binary-size(length), rest2::binary>> = rest
    {str, rest2}
  end

  # ---------------------------------------------------------------------------
  # Boolean — single byte (UInt8 alias)
  # ---------------------------------------------------------------------------

  @spec encode_bool(boolean()) :: binary()
  def encode_bool(true), do: <<1::8>>
  def encode_bool(false), do: <<0::8>>

  @spec decode_bool(binary()) :: {boolean(), binary()}
  def decode_bool(<<1::8, rest::binary>>), do: {true, rest}
  def decode_bool(<<0::8, rest::binary>>), do: {false, rest}

  # ---------------------------------------------------------------------------
  # Fixed-size integers — all little-endian
  # ---------------------------------------------------------------------------

  @spec encode_uint8(non_neg_integer()) :: binary()
  def encode_uint8(value) when is_integer(value), do: <<value::little-unsigned-8>>

  @spec decode_uint8(binary()) :: {non_neg_integer(), binary()}
  def decode_uint8(<<value::little-unsigned-8, rest::binary>>), do: {value, rest}

  @spec encode_int8(integer()) :: binary()
  def encode_int8(value) when is_integer(value), do: <<value::little-signed-8>>

  @spec decode_int8(binary()) :: {integer(), binary()}
  def decode_int8(<<value::little-signed-8, rest::binary>>), do: {value, rest}

  @spec encode_uint16(non_neg_integer()) :: binary()
  def encode_uint16(value) when is_integer(value), do: <<value::little-unsigned-16>>

  @spec decode_uint16(binary()) :: {non_neg_integer(), binary()}
  def decode_uint16(<<value::little-unsigned-16, rest::binary>>), do: {value, rest}

  @spec encode_int16(integer()) :: binary()
  def encode_int16(value) when is_integer(value), do: <<value::little-signed-16>>

  @spec decode_int16(binary()) :: {integer(), binary()}
  def decode_int16(<<value::little-signed-16, rest::binary>>), do: {value, rest}

  @spec encode_uint32(non_neg_integer()) :: binary()
  def encode_uint32(value) when is_integer(value), do: <<value::little-unsigned-32>>

  @spec decode_uint32(binary()) :: {non_neg_integer(), binary()}
  def decode_uint32(<<value::little-unsigned-32, rest::binary>>), do: {value, rest}

  @spec encode_int32(integer()) :: binary()
  def encode_int32(value) when is_integer(value), do: <<value::little-signed-32>>

  @spec decode_int32(binary()) :: {integer(), binary()}
  def decode_int32(<<value::little-signed-32, rest::binary>>), do: {value, rest}

  @spec encode_uint64(non_neg_integer()) :: binary()
  def encode_uint64(value) when is_integer(value), do: <<value::little-unsigned-64>>

  @spec decode_uint64(binary()) :: {non_neg_integer(), binary()}
  def decode_uint64(<<value::little-unsigned-64, rest::binary>>), do: {value, rest}

  @spec encode_int64(integer()) :: binary()
  def encode_int64(value) when is_integer(value), do: <<value::little-signed-64>>

  @spec decode_int64(binary()) :: {integer(), binary()}
  def decode_int64(<<value::little-signed-64, rest::binary>>), do: {value, rest}

  @spec encode_uint128(non_neg_integer()) :: binary()
  def encode_uint128(value) when is_integer(value), do: <<value::little-unsigned-128>>

  @spec decode_uint128(binary()) :: {non_neg_integer(), binary()}
  def decode_uint128(<<value::little-unsigned-128, rest::binary>>), do: {value, rest}

  @spec encode_int128(integer()) :: binary()
  def encode_int128(value) when is_integer(value), do: <<value::little-signed-128>>

  @spec decode_int128(binary()) :: {integer(), binary()}
  def decode_int128(<<value::little-signed-128, rest::binary>>), do: {value, rest}

  # ---------------------------------------------------------------------------
  # Floating point — little-endian IEEE 754
  # ---------------------------------------------------------------------------

  @spec encode_float32(float()) :: binary()
  def encode_float32(value) when is_float(value), do: <<value::little-float-32>>

  @spec decode_float32(binary()) :: {float(), binary()}
  def decode_float32(<<value::little-float-32, rest::binary>>), do: {value, rest}

  @spec encode_float64(float()) :: binary()
  def encode_float64(value) when is_float(value), do: <<value::little-float-64>>
  def encode_float64(value) when is_integer(value), do: <<value * 1.0::little-float-64>>

  @spec decode_float64(binary()) :: {float(), binary()}
  def decode_float64(<<value::little-float-64, rest::binary>>), do: {value, rest}

  # ---------------------------------------------------------------------------
  # Enum8 — signed Int8 (alias)
  # ---------------------------------------------------------------------------

  @spec encode_enum8(integer()) :: binary()
  def encode_enum8(value) when is_integer(value), do: <<value::little-signed-8>>

  # ---------------------------------------------------------------------------
  # UUID — 16 bytes, each 8-byte half in little-endian
  # ---------------------------------------------------------------------------

  @spec encode_uuid(binary()) :: binary()
  def encode_uuid(<<u1::64, u2::64>>) do
    <<u1::little-unsigned-64, u2::little-unsigned-64>>
  end

  def encode_uuid(uuid_string) when is_binary(uuid_string) and byte_size(uuid_string) == 36 do
    {:ok, uuid_raw} = parse_uuid_string(uuid_string)
    encode_uuid(uuid_raw)
  end

  @spec decode_uuid(binary()) :: {binary(), binary()}
  def decode_uuid(<<u1::little-unsigned-64, u2::little-unsigned-64, rest::binary>>) do
    {<<u1::64, u2::64>>, rest}
  end

  # ---------------------------------------------------------------------------
  # DateTime / DateTime64
  # ---------------------------------------------------------------------------

  @spec encode_datetime(non_neg_integer()) :: binary()
  def encode_datetime(unix_seconds) when is_integer(unix_seconds) do
    <<unix_seconds::little-unsigned-32>>
  end

  @spec decode_datetime(binary()) :: {non_neg_integer(), binary()}
  def decode_datetime(<<value::little-unsigned-32, rest::binary>>), do: {value, rest}

  @spec encode_datetime64(integer(), non_neg_integer()) :: binary()
  def encode_datetime64(scaled_value, _precision) when is_integer(scaled_value) do
    <<scaled_value::little-signed-64>>
  end

  @spec decode_datetime64(binary()) :: {integer(), binary()}
  def decode_datetime64(<<value::little-signed-64, rest::binary>>), do: {value, rest}

  # ---------------------------------------------------------------------------
  # FixedString(N) — N bytes, null-padded
  # ---------------------------------------------------------------------------

  @spec encode_fixed_string(binary(), pos_integer()) :: binary()
  def encode_fixed_string(str, n) when is_binary(str) and is_pos_integer(n) do
    len = byte_size(str)

    cond do
      len == n -> str
      len < n -> str <> :binary.copy(<<0>>, n - len)
      true -> binary_part(str, 0, n)
    end
  end

  @spec decode_fixed_string(binary(), pos_integer()) :: {binary(), binary()}
  def decode_fixed_string(data, n) when is_binary(data) and is_pos_integer(n) do
    <<str::binary-size(n), rest::binary>> = data
    {str, rest}
  end

  @spec parse_uuid_string(String.t()) :: {:ok, binary()} | :error
  defp parse_uuid_string(
         <<a::binary-size(8), ?-, b::binary-size(4), ?-, c::binary-size(4), ?-, d::binary-size(4),
           ?-, e::binary-size(12)>>
       ) do
    case Base.decode16(a <> b <> c <> d <> e, case: :mixed) do
      {:ok, raw} -> {:ok, raw}
      :error -> :error
    end
  end

  defp parse_uuid_string(_), do: :error
end

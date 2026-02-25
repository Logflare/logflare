defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection do
  @moduledoc """
  Manages a single persistent TCP connection to ClickHouse using the native protocol.

  Handles the Hello handshake, socket I/O with buffered reads, ping/pong
  health checks, and clean shutdown. Each connection tracks its negotiated
  protocol revision, which gates conditional fields in subsequent packets.
  """

  use TypedStruct

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Compression
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol

  @default_port 9_000
  @default_connect_timeout 5_000
  @default_recv_timeout 30_000
  @compressed_header_size 9

  @type server_info :: %{
          name: String.t(),
          major: non_neg_integer(),
          minor: non_neg_integer(),
          revision: non_neg_integer(),
          timezone: String.t() | nil,
          display_name: String.t() | nil,
          patch: non_neg_integer() | nil
        }

  @type connect_opts :: [
          host: String.t(),
          port: non_neg_integer(),
          database: String.t(),
          username: String.t(),
          password: String.t(),
          transport: :gen_tcp | :ssl,
          connect_timeout: non_neg_integer(),
          compression: :none | :lz4
        ]

  @type column_info :: [{name :: String.t(), type :: String.t()}]

  typedstruct do
    field :socket, :gen_tcp.socket() | :ssl.sslsocket()
    field :transport, :gen_tcp | :ssl
    field :host, String.t()
    field :port, non_neg_integer()
    field :database, String.t()
    field :username, String.t()
    field :server_info, server_info()
    field :negotiated_rev, non_neg_integer()
    field :compression, :none | :lz4 | :zstd, default: :none
    field :buffer, binary(), default: <<>>
    field :recv_timeout, non_neg_integer(), default: 30_000
    field :connected_at, integer()
    field :last_used_at, integer()
  end

  @doc """
  Opens a TCP connection to ClickHouse and performs the native protocol handshake.

  ## Required Options

    * `:host` - ClickHouse server hostname (e.g. `"localhost"`)
    * `:database` - database name to connect to
    * `:username` - authentication username
    * `:password` - authentication password

  ## Optional Options

    * `:port` - TCP port (default: `9000`)
    * `:transport` - `:gen_tcp` for plaintext or `:ssl` for TLS (default: `:gen_tcp`)
    * `:connect_timeout` - timeout in milliseconds for the TCP connect (default: `5000`)
    * `:compression` - `:none` or `:lz4` for LZ4-compressed data blocks (default: `:none`)
  """
  @spec connect(connect_opts()) :: {:ok, t()} | {:error, term()}
  def connect(opts) when is_list(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.get(opts, :port, @default_port)
    database = Keyword.fetch!(opts, :database)
    username = Keyword.fetch!(opts, :username)
    password = Keyword.fetch!(opts, :password)
    transport = Keyword.get(opts, :transport, :gen_tcp)
    connect_timeout = Keyword.get(opts, :connect_timeout, @default_connect_timeout)
    compression = Keyword.get(opts, :compression, :none)

    unless transport in [:gen_tcp, :ssl] do
      raise ArgumentError,
            "expected :transport to be :gen_tcp or :ssl, got: #{inspect(transport)}"
    end

    unless is_pos_integer(connect_timeout) do
      raise ArgumentError,
            "expected :connect_timeout to be a positive integer, got: #{inspect(connect_timeout)}"
    end

    unless compression in [:none, :lz4] do
      raise ArgumentError,
            "expected :compression to be :none or :lz4, got: #{inspect(compression)}"
    end

    conn = %__MODULE__{
      host: host,
      port: port,
      database: database,
      username: username,
      transport: transport,
      compression: compression
    }

    tcp_opts = [:binary, {:packet, :raw}, {:active, false}, {:nodelay, true}]

    connect_opts =
      if transport == :ssl do
        tcp_opts ++ ssl_opts(host)
      else
        tcp_opts
      end

    host_charlist = String.to_charlist(host)

    with {:ok, socket} <- transport.connect(host_charlist, port, connect_opts, connect_timeout),
         conn = %{conn | socket: socket},
         {:ok, conn} <- send_client_hello(conn, password),
         {:ok, conn} <- read_server_hello(conn),
         {:ok, conn} <- maybe_send_addendum(conn) do
      {:ok, %{conn | connected_at: System.monotonic_time(:millisecond)}}
    end
  end

  @spec ssl_opts(String.t()) :: [:ssl.tls_client_option()]
  defp ssl_opts(host) do
    cacerts =
      CAStore.file_path()
      |> File.read!()
      |> :public_key.pem_decode()
      |> Enum.map(fn {_, der, _} -> der end)

    [
      verify: :verify_peer,
      cacerts: cacerts,
      server_name_indication: String.to_charlist(host),
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
    ]
  end

  @doc """
  Tests connectivity by performing a connect → ping → close cycle.
  """
  @spec test_connection(connect_opts()) :: :ok | {:error, term()}
  def test_connection(opts) do
    case connect(opts) do
      {:ok, conn} ->
        result =
          case ping(conn) do
            {:ok, conn} ->
              close(conn)
              :ok

            {:error, reason} ->
              close(conn)
              {:error, {:ping_failed, reason}}
          end

        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec close(t()) :: :ok
  def close(%__MODULE__{socket: nil}), do: :ok

  def close(%__MODULE__{socket: socket, transport: transport}) do
    transport.close(socket)
  end

  @spec ping(t()) :: {:ok, t()} | {:error, term()}
  def ping(%__MODULE__{} = conn) do
    packet = Protocol.encode_varuint(Protocol.client_ping())

    with :ok <- send_data(conn, packet),
         {:ok, conn} <- read_pong(conn) do
      {:ok, conn}
    end
  end

  @spec alive?(t()) :: boolean()
  def alive?(%__MODULE__{socket: nil}), do: false

  def alive?(%__MODULE__{} = conn) do
    case ping(conn) do
      {:ok, _conn} -> true
      {:error, _reason} -> false
    end
  end

  @doc """
  Sends an INSERT query and reads the server's response (column schema).

  Returns `{:ok, column_info, conn}` where `column_info` is a list of
  `{name, type}` tuples describing the expected columns. After this returns
  successfully, the connection expects Data blocks to be sent.

  The `settings` parameter is a keyword list of ClickHouse settings to include
  with the query (e.g. `[async_insert: 1, wait_for_async_insert: 1]`). These
  are merged after the two base settings (`low_cardinality_allow_in_native_format=0`
  and `input_format_native_allow_types_conversion=1`). Each setting is sent
  with flags=0 (default priority).

  ## Options

    * `:query_id` - optional query identifier (default: auto-generated UUID)
  """
  @spec send_query(t(), String.t(), keyword(), keyword()) ::
          {:ok, column_info(), t()} | {:error, term()}
  def send_query(%__MODULE__{} = conn, sql, settings \\ [], opts \\ [])
      when is_list(settings) and is_list(opts) do
    query_id = Keyword.get(opts, :query_id, Ecto.UUID.generate())
    query_packet = encode_query_packet(conn, sql, query_id, settings)

    with :ok <- send_data(conn, query_packet),
         :ok <- send_data_block(conn, BlockEncoder.encode_empty_block_body()),
         {:ok, column_info, conn} <- read_query_response(conn) do
      {:ok, column_info, conn}
    end
  end

  @doc """
  Sends a block body over the socket, handling compression if enabled.

  When compression is `:none`, sends the body as a plain Data packet.
  When compression is `:lz4`, the block body is compressed and wrapped
  in a compressed envelope while the packet prefix stays uncompressed.

  Accepts iodata — typically the output of `BlockEncoder.encode_block_body/2`
  or `BlockEncoder.encode_empty_block_body/0`.
  """
  @spec send_data_block(t(), iodata()) :: :ok | {:error, term()}
  def send_data_block(%__MODULE__{compression: :none} = conn, block_body) do
    send_data(conn, [BlockEncoder.data_packet_prefix(), block_body])
  end

  def send_data_block(%__MODULE__{compression: :lz4} = conn, block_body) do
    compressed = Compression.compress(block_body)
    send_data(conn, [BlockEncoder.data_packet_prefix(), compressed])
  end

  @doc """
  Reads the server's response after sending data blocks for an INSERT.

  Expects `EndOfStream` (success) or `Exception` (error). Handles unsolicited
  `Progress`, `Log`, and `ProfileEvents` packets that may arrive during or
  after the insert. Drains trailing `ProfileEvents` packets (sent by CH 25.12+
  after `EndOfStream`) so the connection is clean for reuse.
  """
  @spec read_insert_response(t()) :: {:ok, t()} | {:error, term()}
  def read_insert_response(%__MODULE__{} = conn) do
    read_insert_response_loop(conn)
  end

  # ---------------------------------------------------------------------------
  # Client Hello
  # ---------------------------------------------------------------------------

  @spec send_client_hello(t(), String.t()) :: {:ok, t()} | {:error, term()}
  defp send_client_hello(%__MODULE__{} = conn, password) do
    packet =
      [
        Protocol.encode_varuint(Protocol.client_hello()),
        Protocol.encode_string(Protocol.client_name()),
        Protocol.encode_varuint(Protocol.client_version_major()),
        Protocol.encode_varuint(Protocol.client_version_minor()),
        Protocol.encode_varuint(Protocol.dbms_tcp_protocol_version()),
        Protocol.encode_string(conn.database),
        Protocol.encode_string(conn.username),
        Protocol.encode_string(password)
      ]
      |> IO.iodata_to_binary()

    case send_data(conn, packet) do
      :ok -> {:ok, conn}
      {:error, reason} -> {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Server Hello
  # ---------------------------------------------------------------------------

  @spec read_server_hello(t()) :: {:ok, t()} | {:error, term()}
  defp read_server_hello(%__MODULE__{} = conn) do
    with {:ok, packet_type, conn} <- read_varuint(conn) do
      case packet_type do
        0 -> parse_server_hello(conn)
        2 -> read_exception(conn)
        other -> {:error, {:unexpected_packet_type, other}}
      end
    end
  end

  @spec parse_server_hello(t()) :: {:ok, t()} | {:error, term()}
  defp parse_server_hello(%__MODULE__{} = conn) do
    with {:ok, server_name, conn} <- read_string(conn),
         {:ok, server_major, conn} <- read_varuint(conn),
         {:ok, server_minor, conn} <- read_varuint(conn),
         {:ok, server_revision, conn} <- read_varuint(conn) do
      negotiated_rev = min(Protocol.dbms_tcp_protocol_version(), server_revision)

      info = %{
        name: server_name,
        major: server_major,
        minor: server_minor,
        revision: server_revision,
        timezone: nil,
        display_name: nil,
        patch: nil
      }

      conn = %{conn | server_info: info, negotiated_rev: negotiated_rev}

      with {:ok, conn} <- maybe_skip_parallel_replicas_version(conn),
           {:ok, conn} <- maybe_read_timezone(conn),
           {:ok, conn} <- maybe_read_display_name(conn),
           {:ok, conn} <- maybe_read_patch(conn),
           {:ok, conn} <- maybe_skip_chunked_caps(conn),
           {:ok, conn} <- maybe_skip_password_rules(conn),
           {:ok, conn} <- maybe_skip_nonce(conn),
           {:ok, conn} <- maybe_skip_server_settings(conn),
           {:ok, conn} <- maybe_skip_query_plan_version(conn),
           {:ok, conn} <- maybe_skip_cluster_version(conn) do
        {:ok, conn}
      end
    end
  end

  @spec maybe_read_timezone(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_read_timezone(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_058 do
    with {:ok, timezone, conn} <- read_string(conn) do
      {:ok, put_in(conn.server_info.timezone, timezone)}
    end
  end

  defp maybe_read_timezone(conn), do: {:ok, conn}

  @spec maybe_read_display_name(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_read_display_name(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_372 do
    with {:ok, display_name, conn} <- read_string(conn) do
      {:ok, put_in(conn.server_info.display_name, display_name)}
    end
  end

  defp maybe_read_display_name(conn), do: {:ok, conn}

  @spec maybe_read_patch(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_read_patch(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_401 do
    with {:ok, patch, conn} <- read_varuint(conn) do
      {:ok, put_in(conn.server_info.patch, patch)}
    end
  end

  defp maybe_read_patch(conn), do: {:ok, conn}

  # ---------------------------------------------------------------------------
  # Server Hello: additional fields (read and discard)
  # ---------------------------------------------------------------------------

  @spec maybe_skip_parallel_replicas_version(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_parallel_replicas_version(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_471 do
    with {:ok, _version, conn} <- read_varuint(conn), do: {:ok, conn}
  end

  defp maybe_skip_parallel_replicas_version(conn), do: {:ok, conn}

  @spec maybe_skip_chunked_caps(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_chunked_caps(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_470 do
    with {:ok, _send_cap, conn} <- read_string(conn),
         {:ok, _recv_cap, conn} <- read_string(conn) do
      {:ok, conn}
    end
  end

  defp maybe_skip_chunked_caps(conn), do: {:ok, conn}

  @spec maybe_skip_password_rules(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_password_rules(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_461 do
    with {:ok, count, conn} <- read_varuint(conn) do
      skip_password_rule_pairs(conn, count)
    end
  end

  defp maybe_skip_password_rules(conn), do: {:ok, conn}

  @spec skip_password_rule_pairs(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  defp skip_password_rule_pairs(conn, 0), do: {:ok, conn}

  defp skip_password_rule_pairs(conn, remaining) do
    with {:ok, _pattern, conn} <- read_string(conn),
         {:ok, _exception, conn} <- read_string(conn) do
      skip_password_rule_pairs(conn, remaining - 1)
    end
  end

  @spec maybe_skip_nonce(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_nonce(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_462 do
    with {:ok, _nonce_bytes, conn} <- read_bytes(conn, 8), do: {:ok, conn}
  end

  defp maybe_skip_nonce(conn), do: {:ok, conn}

  @spec maybe_skip_server_settings(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_server_settings(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_474 do
    skip_settings_loop(conn)
  end

  defp maybe_skip_server_settings(conn), do: {:ok, conn}

  @spec skip_settings_loop(t()) :: {:ok, t()} | {:error, term()}
  defp skip_settings_loop(conn) do
    with {:ok, name, conn} <- read_string(conn) do
      if name == "" do
        {:ok, conn}
      else
        with {:ok, _flags, conn} <- read_varuint(conn),
             {:ok, _value, conn} <- read_string(conn) do
          skip_settings_loop(conn)
        end
      end
    end
  end

  @spec maybe_skip_query_plan_version(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_query_plan_version(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_477 do
    with {:ok, _version, conn} <- read_varuint(conn), do: {:ok, conn}
  end

  defp maybe_skip_query_plan_version(conn), do: {:ok, conn}

  @spec maybe_skip_cluster_version(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_cluster_version(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_479 do
    with {:ok, _version, conn} <- read_varuint(conn), do: {:ok, conn}
  end

  defp maybe_skip_cluster_version(conn), do: {:ok, conn}

  # ---------------------------------------------------------------------------
  # Hello Addendum (protocol >= 54458)
  # ---------------------------------------------------------------------------

  @spec maybe_send_addendum(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_send_addendum(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_458 do
    parts = [Protocol.encode_string("")]

    parts =
      if rev >= 54_470 do
        parts ++
          [
            Protocol.encode_string("notchunked"),
            Protocol.encode_string("notchunked")
          ]
      else
        parts
      end

    parts =
      if rev >= 54_471 do
        parts ++ [Protocol.encode_varuint(5)]
      else
        parts
      end

    packet = IO.iodata_to_binary(parts)

    case send_data(conn, packet) do
      :ok -> {:ok, conn}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_send_addendum(conn), do: {:ok, conn}

  # ---------------------------------------------------------------------------
  # Ping / Pong
  # ---------------------------------------------------------------------------

  @spec read_pong(t()) :: {:ok, t()} | {:error, term()}
  defp read_pong(%__MODULE__{} = conn) do
    with {:ok, packet_type, conn} <- read_varuint(conn) do
      case packet_type do
        4 -> {:ok, conn}
        2 -> read_exception(conn)
        other -> {:error, {:unexpected_packet_type, other}}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Query packet encoding
  # ---------------------------------------------------------------------------

  @spec encode_query_packet(t(), String.t(), String.t(), keyword()) :: binary()
  defp encode_query_packet(%__MODULE__{negotiated_rev: rev} = conn, sql, query_id, extra_settings) do
    compression_flag = if conn.compression == :none, do: 0, else: 1

    [
      Protocol.encode_varuint(Protocol.client_query()),
      Protocol.encode_string(query_id),
      encode_client_info(conn, rev),
      encode_settings(rev, extra_settings),
      encode_extra_roles(rev),
      encode_interserver_secret(rev),
      Protocol.encode_varuint(2),
      Protocol.encode_varuint(compression_flag),
      Protocol.encode_string(sql),
      encode_parameters(rev)
    ]
    |> IO.iodata_to_binary()
  end

  @spec encode_client_info(t(), non_neg_integer()) :: binary()
  defp encode_client_info(%__MODULE__{} = _conn, rev) when rev < 54_032, do: <<>>

  defp encode_client_info(%__MODULE__{} = _conn, rev) do
    hostname = get_hostname()

    parts = [
      Protocol.encode_uint8(1),
      Protocol.encode_string(""),
      Protocol.encode_string(""),
      Protocol.encode_string("[::ffff:127.0.0.1]:0")
    ]

    parts =
      if rev >= 54_449 do
        parts ++ [Protocol.encode_int64(0)]
      else
        parts
      end

    parts =
      parts ++
        [
          Protocol.encode_uint8(1),
          Protocol.encode_string(""),
          Protocol.encode_string(hostname),
          Protocol.encode_string(Protocol.client_name()),
          Protocol.encode_varuint(Protocol.client_version_major()),
          Protocol.encode_varuint(Protocol.client_version_minor()),
          Protocol.encode_varuint(Protocol.dbms_tcp_protocol_version())
        ]

    # quota_key (empty)
    parts =
      if rev >= 54_060 do
        parts ++ [Protocol.encode_string("")]
      else
        parts
      end

    # distributed_depth
    parts =
      if rev >= 54_448 do
        parts ++ [Protocol.encode_varuint(0)]
      else
        parts
      end

    # client_version_patch
    parts =
      if rev >= 54_401 do
        parts ++ [Protocol.encode_varuint(0)]
      else
        parts
      end

    # OpenTelemetry trace flag (no trace)
    parts =
      if rev >= 54_442 do
        parts ++ [Protocol.encode_uint8(0)]
      else
        parts
      end

    # parallel replicas: collaborate_with_initiator, count, number_of_current
    parts =
      if rev >= 54_453 do
        parts ++
          [
            Protocol.encode_varuint(0),
            Protocol.encode_varuint(0),
            Protocol.encode_varuint(0)
          ]
      else
        parts
      end

    # script query/line numbers
    parts =
      if rev >= 54_475 do
        parts ++ [Protocol.encode_varuint(0), Protocol.encode_varuint(0)]
      else
        parts
      end

    # JWT flag (no JWT)
    parts =
      if rev >= 54_476 do
        parts ++ [Protocol.encode_uint8(0)]
      else
        parts
      end

    IO.iodata_to_binary(parts)
  end

  @spec encode_settings(non_neg_integer(), keyword()) :: binary()
  defp encode_settings(rev, _extra_settings) when rev < 54_429, do: <<>>

  defp encode_settings(_rev, extra_settings) do
    base = [
      {"low_cardinality_allow_in_native_format", 1, "0"},
      {"input_format_native_allow_types_conversion", 1, "1"}
    ]

    extra =
      Enum.map(extra_settings, fn {name, value} ->
        {to_string(name), 0, to_string(value)}
      end)

    all_settings = base ++ extra

    encoded =
      Enum.flat_map(all_settings, fn {name, flags, value} ->
        [
          Protocol.encode_string(name),
          Protocol.encode_varuint(flags),
          Protocol.encode_string(value)
        ]
      end)

    IO.iodata_to_binary(encoded ++ [Protocol.encode_string("")])
  end

  @spec encode_extra_roles(non_neg_integer()) :: binary()
  defp encode_extra_roles(rev) when rev >= 54_472, do: Protocol.encode_string("")
  defp encode_extra_roles(_rev), do: <<>>

  @spec encode_interserver_secret(non_neg_integer()) :: binary()
  defp encode_interserver_secret(rev) when rev >= 54_441, do: Protocol.encode_string("")
  defp encode_interserver_secret(_rev), do: <<>>

  @spec encode_parameters(non_neg_integer()) :: binary()
  defp encode_parameters(rev) when rev >= 54_459, do: Protocol.encode_string("")
  defp encode_parameters(_rev), do: <<>>

  @spec get_hostname() :: String.t()
  defp get_hostname do
    {:ok, hostname} = :inet.gethostname()
    List.to_string(hostname)
  end

  # ---------------------------------------------------------------------------
  # Query response parsing
  # ---------------------------------------------------------------------------

  @spec read_query_response(t()) :: {:ok, column_info(), t()} | {:error, term()}
  defp read_query_response(%__MODULE__{} = conn) do
    read_query_response_loop(conn, nil)
  end

  @spec read_query_response_loop(t(), column_info() | nil) ::
          {:ok, column_info(), t()} | {:error, term()}
  defp read_query_response_loop(%__MODULE__{} = conn, column_info) do
    with {:ok, packet_type, conn} <- read_varuint(conn),
         {:ok, conn} <- maybe_decompress_server_payload(conn, packet_type) do
      case packet_type do
        1 ->
          with {:ok, new_column_info, conn} <- read_data_block(conn) do
            if new_column_info != [] do
              {:ok, new_column_info, conn}
            else
              read_query_response_loop(conn, column_info)
            end
          end

        2 ->
          read_exception(conn)

        3 ->
          with {:ok, conn} <- skip_progress(conn) do
            read_query_response_loop(conn, column_info)
          end

        5 ->
          if column_info do
            {:ok, column_info, conn}
          else
            {:error, :no_column_info}
          end

        10 ->
          with {:ok, conn} <- skip_data_block(conn) do
            read_query_response_loop(conn, column_info)
          end

        11 ->
          with {:ok, conn} <- skip_table_columns(conn) do
            read_query_response_loop(conn, column_info)
          end

        14 ->
          with {:ok, conn} <- skip_data_block(conn) do
            read_query_response_loop(conn, column_info)
          end

        other ->
          {:error, {:unexpected_packet_type, other}}
      end
    end
  end

  @spec read_data_block(t()) :: {:ok, column_info(), t()} | {:error, term()}
  defp read_data_block(%__MODULE__{} = conn) do
    with {:ok, _temp_table, conn} <- read_string(conn),
         {:ok, conn} <- maybe_decompress_block_body(conn),
         {:ok, conn} <- read_block_info(conn),
         {:ok, num_columns, conn} <- read_varuint(conn),
         {:ok, num_rows, conn} <- read_varuint(conn),
         {:ok, column_info, conn} <- read_columns(conn, num_columns, num_rows, []) do
      {:ok, column_info, conn}
    end
  end

  @spec maybe_decompress_block_body(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_decompress_block_body(%__MODULE__{compression: :none} = conn), do: {:ok, conn}

  defp maybe_decompress_block_body(%__MODULE__{compression: :lz4} = conn) do
    read_and_decompress_envelope(conn)
  end

  # Server packet types whose entire payloads (after packet type) are compressed.
  # TableColumns (11) has its full payload compressed.
  # Data (1), Log (10), ProfileEvents (14) have temp_table_name uncompressed,
  # then the block body compressed — handled separately in read_data_block.
  @fully_compressed_server_types [11]

  @spec maybe_decompress_server_payload(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  defp maybe_decompress_server_payload(%__MODULE__{compression: :none} = conn, _packet_type),
    do: {:ok, conn}

  defp maybe_decompress_server_payload(%__MODULE__{compression: :lz4} = conn, packet_type)
       when packet_type in @fully_compressed_server_types do
    read_and_decompress_envelope(conn)
  end

  defp maybe_decompress_server_payload(conn, _packet_type), do: {:ok, conn}

  @spec read_and_decompress_envelope(t()) :: {:ok, t()} | {:error, term()}
  defp read_and_decompress_envelope(%__MODULE__{} = conn) do
    with {:ok, checksum, conn} <- read_bytes(conn, 16),
         {:ok, <<method::8>>, conn} <- read_bytes(conn, 1),
         {:ok, <<compressed_size::little-unsigned-32>>, conn} <- read_bytes(conn, 4),
         {:ok, <<uncompressed_size::little-unsigned-32>>, conn} <- read_bytes(conn, 4) do
      data_size = compressed_size - @compressed_header_size

      with {:ok, compressed_data, conn} <- read_bytes(conn, data_size) do
        envelope =
          <<checksum::binary, method::8, compressed_size::little-unsigned-32,
            uncompressed_size::little-unsigned-32, compressed_data::binary>>

        case Compression.decompress(envelope) do
          {:ok, decompressed} ->
            {:ok, %{conn | buffer: decompressed <> conn.buffer}}

          {:error, reason} ->
            {:error, {:decompression_error, reason}}
        end
      end
    end
  end

  @spec skip_data_block(t()) :: {:ok, t()} | {:error, term()}
  defp skip_data_block(%__MODULE__{} = conn) do
    with {:ok, _column_info, conn} <- read_data_block(conn), do: {:ok, conn}
  end

  @spec maybe_skip_column_custom_serialization(t()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_column_custom_serialization(%__MODULE__{negotiated_rev: rev} = conn)
       when rev >= 54_454 do
    with {:ok, has_custom, conn} <- read_uint8(conn) do
      if has_custom == 1 do
        skip_serialization_kind_stack(conn)
      else
        {:ok, conn}
      end
    end
  end

  defp maybe_skip_column_custom_serialization(conn), do: {:ok, conn}

  @spec skip_serialization_kind_stack(t()) :: {:ok, t()} | {:error, term()}
  defp skip_serialization_kind_stack(conn) do
    with {:ok, kind, conn} <- read_uint8(conn) do
      case kind do
        0 -> {:ok, conn}
        _ -> skip_serialization_kind_stack(conn)
      end
    end
  end

  @spec read_block_info(t()) :: {:ok, t()} | {:error, term()}
  defp read_block_info(%__MODULE__{negotiated_rev: rev} = conn) when rev >= 51_903 do
    read_block_info_loop(conn)
  end

  defp read_block_info(conn), do: {:ok, conn}

  @spec read_block_info_loop(t()) :: {:ok, t()} | {:error, term()}
  defp read_block_info_loop(conn) do
    with {:ok, field_num, conn} <- read_varuint(conn) do
      case field_num do
        0 ->
          {:ok, conn}

        1 ->
          with {:ok, _is_overflows, conn} <- read_uint8(conn), do: read_block_info_loop(conn)

        2 ->
          with {:ok, _bucket_num, conn} <- read_int32(conn), do: read_block_info_loop(conn)

        3 ->
          with {:ok, conn} <- skip_int32_vector(conn), do: read_block_info_loop(conn)

        _other ->
          read_block_info_loop(conn)
      end
    end
  end

  @spec skip_int32_vector(t()) :: {:ok, t()} | {:error, term()}
  defp skip_int32_vector(conn) do
    with {:ok, count, conn} <- read_varuint(conn) do
      if count == 0, do: {:ok, conn}, else: read_bytes(conn, count * 4) |> skip_data_result()
    end
  end

  @spec skip_data_result({:ok, binary(), t()} | {:error, term()}) :: {:ok, t()} | {:error, term()}
  defp skip_data_result({:ok, _data, conn}), do: {:ok, conn}
  defp skip_data_result({:error, _} = error), do: error

  @spec read_columns(t(), non_neg_integer(), non_neg_integer(), column_info()) ::
          {:ok, column_info(), t()} | {:error, term()}
  defp read_columns(conn, 0, _num_rows, acc), do: {:ok, Enum.reverse(acc), conn}

  defp read_columns(conn, remaining, num_rows, acc) do
    with {:ok, name, conn} <- read_string(conn),
         {:ok, type, conn} <- read_string(conn),
         {:ok, conn} <- maybe_skip_column_custom_serialization(conn),
         {:ok, conn} <- skip_column_data(conn, type, num_rows) do
      read_columns(conn, remaining - 1, num_rows, [{name, type} | acc])
    end
  end

  @spec skip_column_data(t(), String.t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  defp skip_column_data(conn, _type, 0), do: {:ok, conn}

  defp skip_column_data(conn, type, num_rows) do
    case fixed_type_byte_size(type) do
      {:ok, size} ->
        with {:ok, _data, conn} <- read_bytes(conn, size * num_rows), do: {:ok, conn}

      :variable ->
        skip_string_values(conn, num_rows)

      :unsupported ->
        {:error, {:unsupported_column_type, type}}
    end
  end

  @spec fixed_type_byte_size(String.t()) :: {:ok, pos_integer()} | :variable | :unsupported
  defp fixed_type_byte_size("UInt8"), do: {:ok, 1}
  defp fixed_type_byte_size("Int8"), do: {:ok, 1}
  defp fixed_type_byte_size("UInt16"), do: {:ok, 2}
  defp fixed_type_byte_size("Int16"), do: {:ok, 2}
  defp fixed_type_byte_size("Date"), do: {:ok, 2}
  defp fixed_type_byte_size("UInt32"), do: {:ok, 4}
  defp fixed_type_byte_size("Int32"), do: {:ok, 4}
  defp fixed_type_byte_size("Float32"), do: {:ok, 4}
  defp fixed_type_byte_size("DateTime"), do: {:ok, 4}
  defp fixed_type_byte_size("UInt64"), do: {:ok, 8}
  defp fixed_type_byte_size("Int64"), do: {:ok, 8}
  defp fixed_type_byte_size("Float64"), do: {:ok, 8}
  defp fixed_type_byte_size("DateTime64" <> _), do: {:ok, 8}
  defp fixed_type_byte_size("UUID"), do: {:ok, 16}
  defp fixed_type_byte_size("Bool"), do: {:ok, 1}
  defp fixed_type_byte_size("Enum8" <> _), do: {:ok, 1}
  defp fixed_type_byte_size("Enum16" <> _), do: {:ok, 2}
  defp fixed_type_byte_size("String"), do: :variable
  defp fixed_type_byte_size(_), do: :unsupported

  @spec skip_string_values(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  defp skip_string_values(conn, 0), do: {:ok, conn}

  defp skip_string_values(conn, remaining) do
    with {:ok, _value, conn} <- read_string(conn) do
      skip_string_values(conn, remaining - 1)
    end
  end

  @spec skip_progress(t()) :: {:ok, t()} | {:error, term()}
  defp skip_progress(%__MODULE__{negotiated_rev: rev} = conn) do
    with {:ok, _rows, conn} <- read_varuint(conn),
         {:ok, _bytes, conn} <- read_varuint(conn),
         {:ok, _total_rows, conn} <- read_varuint(conn),
         {:ok, conn} <- maybe_skip_write_progress(conn, rev),
         {:ok, conn} <- maybe_skip_elapsed_ns(conn, rev),
         {:ok, conn} <- maybe_skip_total_bytes(conn, rev) do
      {:ok, conn}
    end
  end

  @spec maybe_skip_write_progress(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_write_progress(conn, rev) when rev >= 54_420 do
    with {:ok, _written_rows, conn} <- read_varuint(conn),
         {:ok, _written_bytes, conn} <- read_varuint(conn) do
      {:ok, conn}
    end
  end

  defp maybe_skip_write_progress(conn, _rev), do: {:ok, conn}

  @spec maybe_skip_elapsed_ns(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_elapsed_ns(conn, rev) when rev >= 54_460 do
    with {:ok, _elapsed_ns, conn} <- read_varuint(conn), do: {:ok, conn}
  end

  defp maybe_skip_elapsed_ns(conn, _rev), do: {:ok, conn}

  @spec maybe_skip_total_bytes(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  defp maybe_skip_total_bytes(conn, rev) when rev >= 54_463 do
    with {:ok, _total_bytes, conn} <- read_varuint(conn), do: {:ok, conn}
  end

  defp maybe_skip_total_bytes(conn, _rev), do: {:ok, conn}

  @spec skip_table_columns(t()) :: {:ok, t()} | {:error, term()}
  defp skip_table_columns(conn) do
    with {:ok, _external_table_name, conn} <- read_string(conn),
         {:ok, _columns_description, conn} <- read_string(conn) do
      {:ok, conn}
    end
  end

  # ---------------------------------------------------------------------------
  # INSERT response parsing
  # ---------------------------------------------------------------------------

  @spec read_insert_response_loop(t()) :: {:ok, t()} | {:error, term()}
  defp read_insert_response_loop(%__MODULE__{} = conn) do
    with {:ok, packet_type, conn} <- read_varuint(conn),
         {:ok, conn} <- maybe_decompress_server_payload(conn, packet_type) do
      case packet_type do
        5 ->
          drain_trailing_packets(conn)

        2 ->
          read_exception(conn)

        3 ->
          with {:ok, conn} <- skip_progress(conn), do: read_insert_response_loop(conn)

        10 ->
          with {:ok, conn} <- skip_data_block(conn), do: read_insert_response_loop(conn)

        14 ->
          with {:ok, conn} <- skip_data_block(conn), do: read_insert_response_loop(conn)

        other ->
          {:error, {:unexpected_packet_type, other}}
      end
    end
  end

  @spec drain_trailing_packets(t()) :: {:ok, t()}
  defp drain_trailing_packets(%__MODULE__{buffer: buffer} = conn)
       when byte_size(buffer) > 0 do
    with {:ok, packet_type, conn} <- read_varuint(conn),
         {:ok, conn} <- maybe_decompress_server_payload(conn, packet_type) do
      case packet_type do
        14 ->
          with {:ok, conn} <- skip_data_block(conn), do: drain_trailing_packets(conn)

        3 ->
          with {:ok, conn} <- skip_progress(conn), do: drain_trailing_packets(conn)

        10 ->
          with {:ok, conn} <- skip_data_block(conn), do: drain_trailing_packets(conn)

        _ ->
          {:ok, conn}
      end
    else
      _ -> {:ok, conn}
    end
  end

  defp drain_trailing_packets(%__MODULE__{} = conn) do
    zero_timeout_conn = %{conn | recv_timeout: 0}

    case recv(zero_timeout_conn) do
      {:ok, data} ->
        updated = %{zero_timeout_conn | buffer: zero_timeout_conn.buffer <> data}
        {:ok, drained} = drain_trailing_packets(updated)
        {:ok, %{drained | recv_timeout: @default_recv_timeout}}

      {:error, :timeout} ->
        {:ok, %{conn | recv_timeout: @default_recv_timeout}}

      {:error, _} ->
        {:ok, %{conn | recv_timeout: @default_recv_timeout}}
    end
  end

  # ---------------------------------------------------------------------------
  # Exception parsing
  # ---------------------------------------------------------------------------

  @spec read_exception(t()) :: {:error, term()}
  defp read_exception(%__MODULE__{} = conn) do
    with {:ok, code, conn} <- read_int32(conn),
         {:ok, name, conn} <- read_string(conn),
         {:ok, message, conn} <- read_string(conn),
         {:ok, stack_trace, conn} <- read_string(conn),
         {:ok, has_nested, _conn} <- read_uint8(conn) do
      error_message =
        "ClickHouse exception (#{code}): #{name}: #{message}" <>
          if(stack_trace != "", do: "\n#{stack_trace}", else: "")

      if has_nested == 1 do
        {:error, {:exception, code, error_message <> " (has nested exception)"}}
      else
        {:error, {:exception, code, error_message}}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Buffered socket I/O
  # ---------------------------------------------------------------------------

  @spec send_data(t(), iodata()) :: :ok | {:error, term()}
  defp send_data(%__MODULE__{socket: socket, transport: transport}, data) do
    transport.send(socket, data)
  end

  @spec recv(t()) :: {:ok, binary()} | {:error, term()}
  defp recv(%__MODULE__{socket: socket, transport: transport, recv_timeout: timeout}) do
    transport.recv(socket, 0, timeout)
  end

  @spec read_bytes(t(), non_neg_integer()) :: {:ok, binary(), t()} | {:error, term()}
  defp read_bytes(%__MODULE__{buffer: buffer} = conn, n) when byte_size(buffer) >= n do
    <<data::binary-size(n), rest::binary>> = buffer
    {:ok, data, %{conn | buffer: rest}}
  end

  defp read_bytes(%__MODULE__{} = conn, n) do
    case recv(conn) do
      {:ok, data} ->
        read_bytes(%{conn | buffer: conn.buffer <> data}, n)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec read_varuint(t()) :: {:ok, non_neg_integer(), t()} | {:error, term()}
  defp read_varuint(%__MODULE__{} = conn) do
    read_varuint(conn, 0, 0)
  end

  @spec read_varuint(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer(), t()} | {:error, term()}
  defp read_varuint(conn, shift, acc) do
    case read_bytes(conn, 1) do
      {:ok, <<0::1, byte::7>>, conn} ->
        {:ok, Bitwise.bor(acc, Bitwise.bsl(byte, shift)), conn}

      {:ok, <<1::1, byte::7>>, conn} ->
        read_varuint(conn, shift + 7, Bitwise.bor(acc, Bitwise.bsl(byte, shift)))

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec read_string(t()) :: {:ok, binary(), t()} | {:error, term()}
  defp read_string(%__MODULE__{} = conn) do
    with {:ok, length, conn} <- read_varuint(conn),
         {:ok, data, conn} <- read_bytes(conn, length) do
      {:ok, data, conn}
    end
  end

  @spec read_int32(t()) :: {:ok, integer(), t()} | {:error, term()}
  defp read_int32(%__MODULE__{} = conn) do
    with {:ok, <<value::little-signed-32>>, conn} <- read_bytes(conn, 4) do
      {:ok, value, conn}
    end
  end

  @spec read_uint8(t()) :: {:ok, non_neg_integer(), t()} | {:error, term()}
  defp read_uint8(%__MODULE__{} = conn) do
    with {:ok, <<value::unsigned-8>>, conn} <- read_bytes(conn, 1) do
      {:ok, value, conn}
    end
  end
end

defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection do
  @moduledoc """
  Manages a single persistent TCP connection to ClickHouse using the native protocol.

  Handles the Hello handshake, socket I/O with buffered reads, ping/pong
  health checks, and clean shutdown. Each connection tracks its negotiated
  protocol revision, which gates conditional fields in subsequent packets.
  """

  use TypedStruct

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol

  @default_port 9_000
  @default_connect_timeout 5_000
  @recv_timeout 30_000

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
          connect_timeout: non_neg_integer()
        ]

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

    unless transport in [:gen_tcp, :ssl] do
      raise ArgumentError,
            "expected :transport to be :gen_tcp or :ssl, got: #{inspect(transport)}"
    end

    unless is_pos_integer(connect_timeout) do
      raise ArgumentError,
            "expected :connect_timeout to be a positive integer, got: #{inspect(connect_timeout)}"
    end

    conn = %__MODULE__{
      host: host,
      port: port,
      database: database,
      username: username,
      transport: transport
    }

    tcp_opts = [:binary, {:packet, :raw}, {:active, false}, {:nodelay, true}]
    host_charlist = String.to_charlist(host)

    with {:ok, socket} <- transport.connect(host_charlist, port, tcp_opts, connect_timeout),
         conn = %{conn | socket: socket},
         {:ok, conn} <- send_client_hello(conn, password),
         {:ok, conn} <- read_server_hello(conn),
         {:ok, conn} <- maybe_send_addendum(conn) do
      {:ok, conn}
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
  defp recv(%__MODULE__{socket: socket, transport: transport}) do
    transport.recv(socket, 0, @recv_timeout)
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

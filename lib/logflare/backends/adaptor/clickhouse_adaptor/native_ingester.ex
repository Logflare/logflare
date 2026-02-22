defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester do
  @moduledoc """
  Orchestrates INSERT operations over the ClickHouse native TCP protocol.

  Composes `Connection` (wire-level socket I/O) and `BlockEncoder` (column-oriented
  data encoding) to perform end-to-end inserts. Handles SQL construction, column
  normalization, and sub-block splitting for large batches.

  Column types are obtained from the server during the INSERT handshake.

  Batches larger than 10k rows are automatically split into sub-blocks to
  reduce peak memory usage.
  """

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent

  @low_cardinality_prefix "LowCardinality("
  @sub_block_size 10_000

  @doc """
  Inserts data into ClickHouse via the native TCP protocol.

  When called with a `Connection` and pre-built columns, performs a low-level
  INSERT: sends the query, validates columns against the server's schema,
  encodes and sends data blocks, then reads the server's confirmation.

  When called with a `Backend` and `LogEvent` structs, checks out a pooled
  connection, obtains column types from the server during the INSERT handshake,
  builds column data from the events' bodies, and sends the data.
  """
  @spec insert(Connection.t(), String.t(), [BlockEncoder.column()], keyword()) ::
          {:ok, Connection.t()} | {:error, term()}
  def insert(%Connection{} = conn, table, columns, settings)
      when is_list(columns) and is_list(settings) do
    column_names = Enum.map(columns, &elem(&1, 0))
    sql = build_insert_sql(conn.database, table, column_names)

    normalized_columns = normalize_columns(columns)

    with {:ok, server_columns, conn} <- Connection.send_query(conn, sql, settings),
         :ok <- validate_columns(normalized_columns, server_columns),
         {:ok, conn} <- send_data_blocks(conn, normalized_columns),
         {:ok, conn} <- Connection.read_insert_response(conn) do
      {:ok, conn}
    end
  end

  @spec insert(Backend.t(), String.t(), [LogEvent.t()], atom()) ::
          :ok | {:error, term()}
  def insert(%Backend{config: config} = backend, table, [%LogEvent{} | _] = events, event_type) do
    column_names = QueryTemplates.columns_for_type(event_type)

    settings =
      if Map.get(config, :async_insert, false),
        do: [async_insert: 1, wait_for_async_insert: 1],
        else: []

    Pool.checkout(backend, fn conn ->
      sql = build_insert_sql(conn.database, table, column_names)

      with {:ok, server_columns, conn} <- Connection.send_query(conn, sql, settings) do
        columns = build_columns_from_schema(events, server_columns)
        normalized = normalize_columns(columns)

        case send_data_and_confirm(conn, normalized) do
          {:ok, updated_conn} ->
            {:ok, updated_conn}

          {:error, {:exception, _, _}} = error ->
            {error, conn}

          {:error, {:column_mismatch, _}} = error ->
            {error, conn}

          {:error, _} = error ->
            {error, :remove}
        end
      else
        {:error, _} = error ->
          {error, :remove}
      end
    end)
  end

  @spec build_insert_sql(String.t(), String.t(), [String.t()]) :: String.t()
  defp build_insert_sql(database, table, column_names) do
    cols = Enum.join(column_names, ", ")
    "INSERT INTO #{database}.#{table} (#{cols}) VALUES"
  end

  @spec validate_columns([BlockEncoder.column()], Connection.column_info()) ::
          :ok | {:error, term()}
  defp validate_columns(columns, server_columns) do
    client_columns = Enum.map(columns, fn {name, type, _values} -> {name, type} end)

    normalized_server =
      Enum.map(server_columns, fn {name, type} -> {name, normalize_type(type)} end)

    if client_columns == normalized_server do
      :ok
    else
      {:error, {:column_mismatch, expected: normalized_server, got: client_columns}}
    end
  end

  @spec build_columns_from_schema([LogEvent.t()], Connection.column_info()) ::
          [BlockEncoder.column()]
  defp build_columns_from_schema(events, server_columns) do
    Enum.map(server_columns, fn {name, type} ->
      default = default_for_type(type)

      values =
        Enum.map(events, fn event ->
          extract_value(event, name) || default
        end)

      {name, type, values}
    end)
  end

  @spec extract_value(LogEvent.t(), String.t()) :: term()
  defp extract_value(%LogEvent{id: id}, "id"), do: uuid_to_raw(id)

  defp extract_value(%LogEvent{source_uuid: su}, "source_uuid"),
    do: Atom.to_string(su)

  defp extract_value(%LogEvent{source_name: sn}, "source_name"), do: sn || ""

  defp extract_value(%LogEvent{body: body}, "mapping_config_id"),
    do: uuid_to_raw(body["mapping_config_id"])

  defp extract_value(%LogEvent{body: body}, name), do: body[name]

  @spec default_for_type(String.t()) :: term()
  defp default_for_type("String"), do: ""
  defp default_for_type("UUID"), do: <<0::128>>
  defp default_for_type("Bool"), do: false
  defp default_for_type("UInt8"), do: 0
  defp default_for_type("UInt32"), do: 0
  defp default_for_type("UInt64"), do: 0
  defp default_for_type("Int32"), do: 0
  defp default_for_type("Int64"), do: 0
  defp default_for_type("Float32"), do: 0.0
  defp default_for_type("Float64"), do: 0.0
  defp default_for_type("DateTime64" <> _), do: 0
  defp default_for_type("Enum8" <> _), do: 1
  defp default_for_type("Nullable(" <> _), do: nil
  defp default_for_type("Array(" <> _), do: []

  defp default_for_type(@low_cardinality_prefix <> rest),
    do: default_for_type(BlockEncoder.extract_inner_type(rest))

  defp default_for_type("JSON" <> _), do: %{}
  defp default_for_type(_), do: ""

  @spec normalize_columns([BlockEncoder.column()]) :: [BlockEncoder.column()]
  defp normalize_columns(columns) do
    Enum.map(columns, &normalize_column/1)
  end

  @spec normalize_column(BlockEncoder.column()) :: BlockEncoder.column()
  defp normalize_column({name, type, values}) do
    {normalized_type, normalized_values} = normalize_type_and_values(type, values)
    {name, normalized_type, normalized_values}
  end

  @spec normalize_type_and_values(String.t(), [term()]) :: {String.t(), [term()]}
  defp normalize_type_and_values(@low_cardinality_prefix <> rest, values) do
    normalize_type_and_values(BlockEncoder.extract_inner_type(rest), values)
  end

  defp normalize_type_and_values("JSON" <> _, values) do
    {"String", Enum.map(values, &Jason.encode!/1)}
  end

  defp normalize_type_and_values("Array(" <> rest, values) do
    inner = BlockEncoder.extract_inner_type(rest)

    case inner do
      "JSON" <> _ ->
        converted = Enum.map(values, fn arr -> Enum.map(arr || [], &Jason.encode!/1) end)
        {"Array(String)", converted}

      "LowCardinality(" <> lc_rest ->
        actual_inner = BlockEncoder.extract_inner_type(lc_rest)
        {"Array(#{actual_inner})", values}

      _ ->
        {"Array(#{inner})", values}
    end
  end

  defp normalize_type_and_values("Nullable(" <> rest, values) do
    inner = BlockEncoder.extract_inner_type(rest)
    {"Nullable(#{inner})", values}
  end

  defp normalize_type_and_values(type, values), do: {type, values}

  @spec normalize_type(String.t()) :: String.t()
  defp normalize_type(@low_cardinality_prefix <> rest) do
    normalize_type(BlockEncoder.extract_inner_type(rest))
  end

  defp normalize_type("Array(" <> rest) do
    inner = BlockEncoder.extract_inner_type(rest)
    "Array(#{normalize_type(inner)})"
  end

  defp normalize_type("Nullable(" <> rest) do
    inner = BlockEncoder.extract_inner_type(rest)
    "Nullable(#{normalize_type(inner)})"
  end

  defp normalize_type("JSON" <> _), do: "String"
  defp normalize_type(type), do: type

  @spec send_data_and_confirm(Connection.t(), [BlockEncoder.column()]) ::
          {:ok, Connection.t()} | {:error, term()}
  defp send_data_and_confirm(conn, columns) do
    with {:ok, conn} <- send_data_blocks(conn, columns),
         {:ok, conn} <- Connection.read_insert_response(conn) do
      {:ok, conn}
    end
  end

  @spec send_data_blocks(Connection.t(), [BlockEncoder.column()]) ::
          {:ok, Connection.t()} | {:error, term()}
  defp send_data_blocks(%Connection{} = conn, columns) do
    num_rows = columns |> List.first() |> elem(2) |> length()

    result =
      if num_rows <= @sub_block_size do
        body = BlockEncoder.encode_block_body(columns, conn.negotiated_rev)
        Connection.send_data_block(conn, body)
      else
        send_sub_blocks(conn, columns)
      end

    case result do
      :ok ->
        empty = BlockEncoder.encode_empty_block_body()

        case Connection.send_data_block(conn, empty) do
          :ok -> {:ok, conn}
          error -> error
        end

      error ->
        error
    end
  end

  @spec send_sub_blocks(Connection.t(), [BlockEncoder.column()]) :: :ok | {:error, term()}
  defp send_sub_blocks(%Connection{} = conn, columns) do
    chunked_values =
      Enum.map(columns, fn {_name, _type, values} ->
        Enum.chunk_every(values, @sub_block_size)
      end)

    column_meta = Enum.map(columns, fn {name, type, _values} -> {name, type} end)

    chunked_values
    |> Enum.zip()
    |> Enum.reduce_while(:ok, fn chunk_tuple, :ok ->
      chunks = Tuple.to_list(chunk_tuple)

      sub_columns =
        Enum.zip_with(column_meta, chunks, fn {name, type}, values ->
          {name, type, values}
        end)

      body = BlockEncoder.encode_block_body(sub_columns, conn.negotiated_rev)

      case Connection.send_data_block(conn, body) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @spec uuid_to_raw(term()) :: binary()
  defp uuid_to_raw(nil), do: <<0::128>>
  defp uuid_to_raw(<<_::128>> = raw), do: raw

  defp uuid_to_raw(uuid_string) when is_binary(uuid_string) do
    case Ecto.UUID.dump(uuid_string) do
      {:ok, raw} -> raw
      :error -> <<0::128>>
    end
  end

  defp uuid_to_raw(_), do: <<0::128>>
end

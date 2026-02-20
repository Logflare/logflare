defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester do
  @moduledoc """
  Orchestrates INSERT operations over the ClickHouse native TCP protocol.

  Composes `Connection` (wire-level socket I/O) and `BlockEncoder` (column-oriented
  data encoding) to perform end-to-end inserts. Handles SQL construction, column
  validation against the server's schema, and sub-block splitting for large batches.

  Batches larger than 10k rows are automatically split into sub-blocks to
  reduce peak memory usage.
  """

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection

  @sub_block_size 10_000

  @doc """
  Performs a complete INSERT operation over the native TCP protocol.

  Takes a connection, table name, and column-oriented data, then:

  1. Sends the INSERT query and reads the server's column schema
  2. Validates the server's column schema matches the provided data
  3. Encodes and sends data blocks (splitting into sub-blocks for large batches)
  4. Sends the end-of-data empty block
  5. Reads the server's confirmation (EndOfStream) or error (Exception)

  ## Options

    * `:query_id` - optional query identifier (default: auto-generated UUID)
  """
  @spec insert(Connection.t(), String.t(), [BlockEncoder.column()], keyword()) ::
          {:ok, Connection.t()} | {:error, term()}
  def insert(%Connection{} = conn, table, columns, opts \\ []) when is_list(columns) do
    column_names = Enum.map(columns, &elem(&1, 0))
    sql = build_insert_sql(conn.database, table, column_names)

    with {:ok, server_columns, conn} <- Connection.send_query(conn, sql, opts),
         :ok <- validate_columns(columns, server_columns),
         {:ok, conn} <- send_data_blocks(conn, columns),
         {:ok, conn} <- Connection.read_insert_response(conn) do
      {:ok, conn}
    end
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

    if client_columns == server_columns do
      :ok
    else
      {:error, {:column_mismatch, expected: server_columns, got: client_columns}}
    end
  end

  @spec send_data_blocks(Connection.t(), [BlockEncoder.column()]) ::
          {:ok, Connection.t()} | {:error, term()}
  defp send_data_blocks(%Connection{} = conn, columns) do
    num_rows = columns |> List.first() |> elem(2) |> length()

    result =
      if num_rows <= @sub_block_size do
        block = BlockEncoder.encode_data_block(columns, conn.negotiated_rev)
        Connection.send_data_block(conn, block)
      else
        send_sub_blocks(conn, columns)
      end

    case result do
      :ok ->
        empty = BlockEncoder.encode_empty_block(conn.negotiated_rev)

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

      block = BlockEncoder.encode_data_block(sub_columns, conn.negotiated_rev)

      case Connection.send_data_block(conn, block) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end
end

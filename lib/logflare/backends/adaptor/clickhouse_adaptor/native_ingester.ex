defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester do
  @moduledoc """
  Orchestrates INSERT operations over the ClickHouse native TCP protocol.

  Uses cached column schemas from `SchemaCache` to pre-encode data blocks
  before sending the INSERT query, minimizing work inside ClickHouse's
  server-side query measurement window.
  """

  require Logger

  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.SchemaCache
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent

  @sub_block_size 10_000
  @max_retries 1
  @retry_delay 500
  @retryable_exception_codes [32, 159, 164, 202, 241, 252]

  @doc """
  Inserts log events into ClickHouse via the native TCP protocol.

  Checks out a pooled connection, obtains column types from the server during
  the INSERT handshake, builds column data from the events' bodies, and sends
  the data. Retries on transient errors with a fresh connection.
  """
  @spec insert(Backend.t(), String.t(), [LogEvent.t()], LogEvent.TypeDetection.event_type()) ::
          :ok | {:error, term()}
  def insert(%Backend{config: config} = backend, table, [%LogEvent{} | _] = events, event_type)
      when is_event_type(event_type) do
    column_names = QueryTemplates.columns_for_type(event_type)

    settings =
      if Map.get(config, :async_insert, false),
        do: [async_insert: 1, wait_for_async_insert: 1],
        else: []

    do_insert_with_retry(backend, table, events, column_names, settings, @max_retries)
  end

  @spec build_insert_sql(String.t(), String.t(), [String.t()]) :: String.t()
  defp build_insert_sql(database, table, column_names) do
    cols = Enum.join(column_names, ", ")
    "INSERT INTO #{database}.#{table} (#{cols}) VALUES"
  end

  @spec do_insert_with_retry(
          Backend.t(),
          table :: String.t(),
          [LogEvent.t()],
          column_names :: [String.t()],
          settings :: keyword(),
          retries_left :: non_neg_integer()
        ) ::
          :ok | {:error, term()}
  defp do_insert_with_retry(backend, table, events, column_names, settings, retries_left) do
    case do_pooled_insert(backend, table, events, column_names, settings) do
      :ok ->
        :ok

      {:error, reason} = error ->
        if retries_left > 0 and retryable?(reason) do
          Logger.warning(
            "ClickHouse NativeIngester: retryable error #{inspect(reason)}, " <>
              "retrying in #{@retry_delay}ms (#{retries_left} retries left)"
          )

          Process.sleep(@retry_delay)
          do_insert_with_retry(backend, table, events, column_names, settings, retries_left - 1)
        else
          error
        end
    end
  end

  @spec do_pooled_insert(Backend.t(), String.t(), [LogEvent.t()], [String.t()], keyword()) ::
          :ok | {:error, term()}
  defp do_pooled_insert(backend, table, events, column_names, settings) do
    cache_key = "#{backend.config.database}.#{table}"
    cached_schema = SchemaCache.get(backend.id, cache_key)

    Pool.checkout(backend, fn conn ->
      sql = build_insert_sql(conn.database, table, column_names)

      result =
        if cached_schema do
          do_cached_insert(conn, sql, events, cached_schema, backend.id, cache_key, settings)
        else
          do_uncached_insert(conn, sql, events, backend.id, cache_key, settings)
        end

      case result do
        {:ok, updated_conn} ->
          {:ok, updated_conn}

        {:error, {:exception, code, message}} = error ->
          Logger.error(
            "ClickHouse NativeIngester: exception during insert into #{table}, " <>
              "code=#{code} message=#{inspect(message)}, removing connection"
          )

          {error, :remove}

        {:error, {:column_mismatch, _} = detail} = error ->
          Logger.error(
            "ClickHouse NativeIngester: column mismatch during insert into #{table}, " <>
              "detail=#{inspect(detail)}, removing connection"
          )

          {error, :remove}

        {:error, reason} = error ->
          Logger.error(
            "ClickHouse NativeIngester: error during insert into #{table}, " <>
              "reason=#{inspect(reason)}, removing connection"
          )

          {error, :remove}
      end
    end)
  catch
    :exit, {:timeout, _} ->
      Logger.error("ClickHouse NativeIngester: pool checkout timeout for insert into #{table}")
      {:error, :checkout_timeout}
  end

  @spec do_cached_insert(
          Connection.t(),
          String.t(),
          [LogEvent.t()],
          Connection.column_info(),
          term(),
          String.t(),
          keyword()
        ) :: {:ok, Connection.t()} | {:error, term()}
  defp do_cached_insert(conn, sql, events, cached_schema, backend_id, cache_key, settings) do
    columns = build_columns_from_schema(events, cached_schema)
    normalized = normalize_columns(columns)
    encoded_block = BlockEncoder.encode_block_body(normalized, conn.negotiated_rev)

    with {:ok, server_columns, conn} <- Connection.send_query(conn, sql, settings) do
      if server_columns == cached_schema do
        send_pre_encoded_and_confirm(conn, encoded_block)
      else
        Logger.info(
          "ClickHouse NativeIngester: schema cache mismatch for #{cache_key}, re-encoding"
        )

        SchemaCache.put(backend_id, cache_key, server_columns)
        columns = build_columns_from_schema(events, server_columns)
        normalized = normalize_columns(columns)
        send_data_and_confirm(conn, normalized)
      end
    end
  end

  @spec do_uncached_insert(
          Connection.t(),
          String.t(),
          [LogEvent.t()],
          term(),
          String.t(),
          keyword()
        ) :: {:ok, Connection.t()} | {:error, term()}
  defp do_uncached_insert(conn, sql, events, backend_id, cache_key, settings) do
    with {:ok, server_columns, conn} <- Connection.send_query(conn, sql, settings) do
      columns = build_columns_from_schema(events, server_columns)
      normalized = normalize_columns(columns)

      case send_data_and_confirm(conn, normalized) do
        {:ok, conn} ->
          SchemaCache.put(backend_id, cache_key, server_columns)
          {:ok, conn}

        error ->
          error
      end
    end
  end

  @spec retryable?(term()) :: boolean()
  defp retryable?(:closed), do: true
  defp retryable?(:timeout), do: true
  defp retryable?(:econnrefused), do: true
  defp retryable?(:econnreset), do: true
  defp retryable?(:checkout_timeout), do: true
  defp retryable?({:exception, code, _}) when code in @retryable_exception_codes, do: true
  defp retryable?(_), do: false

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

  defp default_for_type("LowCardinality(" <> rest),
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
  defp normalize_type_and_values("LowCardinality(" <> rest, values) do
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

  @spec send_data_and_confirm(Connection.t(), [BlockEncoder.column()]) ::
          {:ok, Connection.t()} | {:error, term()}
  defp send_data_and_confirm(conn, columns) do
    with {:ok, conn} <- send_data_blocks(conn, columns),
         {:ok, conn} <- Connection.read_insert_response(conn) do
      {:ok, conn}
    end
  end

  @spec send_pre_encoded_and_confirm(Connection.t(), iodata()) ::
          {:ok, Connection.t()} | {:error, term()}
  defp send_pre_encoded_and_confirm(conn, encoded_block) do
    with :ok <- Connection.send_data_block(conn, encoded_block),
         :ok <- Connection.send_data_block(conn, BlockEncoder.encode_empty_block_body()),
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

  defp uuid_to_raw(uuid_string) when is_non_empty_binary(uuid_string) do
    case Ecto.UUID.dump(uuid_string) do
      {:ok, raw} -> raw
      :error -> <<0::128>>
    end
  end

  defp uuid_to_raw(_), do: <<0::128>>
end

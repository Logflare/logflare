defmodule GRPC.Client.Adapters.Finch do
  @moduledoc """
  A client adapter using Finch.
  """

  alias Grpc.Client.Adapters.Finch.BidirectionalStream
  alias GRPC.Client.Adapters.Finch.StreamState
  alias Grpc.Client.Adapters.Finch.StreamRequestProcess
  alias Grpc.Client.Adapters.Finch.CustomStream

  @behaviour GRPC.Client.Adapter

  @impl true
  def connect(channel, opts \\ []) do
    instance_name = Keyword.fetch!(opts, :instance_name)

    {:ok, %{channel | adapter_payload: %{instance_name: instance_name}}}
  end

  @impl true
  def disconnect(channel) do
    # Function is not safe for Finch, we just forget about the payload
    {:ok, %{channel | adapter_payload: nil}}
  end

  @impl true
  def send_request(%{channel: %{adapter_payload: nil}}, _message, _opts),
    do: raise(ArgumentError, "Can't perform a request without a connection process")

  def send_request(stream, message, opts) do
    headers = GRPC.Transport.HTTP2.client_headers_without_reserved(stream, opts)
    {:ok, data, _} = GRPC.Message.to_data(message, opts)
    path = get_full_path(stream)

    {:ok, stream_request_pid} = StreamRequestProcess.start_link(path, headers, data)

    GRPC.Client.Stream.put_payload(stream, :stream_request_pid, stream_request_pid)
  end

  @impl true
  def receive_data(
        %{
          channel: %{adapter_payload: %{instance_name: _instance_name}}
        } = stream,
        opts
      ) do
    do_receive_data(stream, stream.grpc_type, opts)
  end

  @impl true
  def send_headers(%{channel: %{adapter_payload: nil}}, _opts),
    do: raise("Can't start a client stream without a connection process")

  def send_headers(%{grpc_type: :bidirectional_stream} = stream, opts) do
    {:ok, stream_state_pid} = StreamState.start_link()

    stream
    |> GRPC.Client.Stream.put_payload(:stream_state_pid, stream_state_pid)
    |> GRPC.Client.Stream.put_payload(:stream_state_opts, opts)
  end

  def send_headers(stream, opts) do
    headers = GRPC.Transport.HTTP2.client_headers_without_reserved(stream, opts)
    {:ok, {body_stream, stream_state_pid}} = CustomStream.start()

    path = get_full_path(stream)

    {:ok, stream_request_pid} =
      StreamRequestProcess.start_link(path, headers, {:stream, body_stream}, opts)

    stream
    |> GRPC.Client.Stream.put_payload(:stream_request_pid, stream_request_pid)
    |> GRPC.Client.Stream.put_payload(:stream_state_pid, stream_state_pid)
  end

  @impl true
  def send_data(
        %{
          channel: %{adapter_payload: %{instance_name: _instance_name}},
          payload: %{stream_state_pid: stream_state_pid}
        } = stream,
        message,
        opts
      ) do
    {:ok, data, _} = GRPC.Message.to_data(message, opts)

    CustomStream.add_item(stream_state_pid, data)

    if opts[:send_end_stream] do
      # This synchronously sends the final data and closes the stream. Correct.

      CustomStream.close(stream_state_pid)
    end

    stream
  end

  @impl true
  def end_stream(
        %{
          channel: %{adapter_payload: %{instance_name: _instance_name}},
          payload: %{stream_state_pid: stream_state_pid}
        } = stream
      ) do
    CustomStream.close(stream_state_pid)
    stream
  end

  @impl true
  def cancel(stream) do
    %{
      channel: %{adapter_payload: %{instance_name: _instance_name}},
      payload: payload
    } = stream

    if payload[:stream_state_pid] do
      CustomStream.close(payload[:stream_state_pid])
      GRPC.Client.Stream.put_payload(stream, :stream_state_pid, nil)
    end

    if payload[:stream_request_pid] do
      StreamRequestProcess.close(payload[:stream_request_pid])
    end

    :ok
  end

  defp do_receive_data(
         stream,
         :bidirectional_stream,
         opts
       ) do
    # Finch does not allow chunked request and responses, you can stream but you are forced to close the stream
    # in order to process the messages, in a bidirectional_stream the stream could or not be closed
    # in this case is required to send each request individually

    response = response_data_bidirectional_stream(stream, opts)

    with {:headers, headers} <- Enum.at(response, 0, :empty) do
      if opts[:return_headers] do
        {:ok, response, %{headers: headers}}
      else
        {:ok, response}
      end
    else
      :empty -> {:ok, []}
      e -> e
    end
  end

  defp do_receive_data(
         %{payload: %{stream_request_pid: stream_request_pid}} = stream,
         :server_stream,
         opts
       ) do
    response = response_data_stream(stream, stream_request_pid, opts)

    with {:headers, headers} <- Enum.at(response, 0) do
      if opts[:return_headers] do
        {:ok, response, %{headers: headers}}
      else
        {:ok, response}
      end
    end
  end

  defp do_receive_data(
         %{payload: %{stream_request_pid: stream_request_pid}} = stream,
         request_type,
         opts
       )
       when request_type in [:client_stream, :unary] do
    response = response_data_stream(stream, stream_request_pid, opts)

    with {:headers, headers} <- Enum.at(response, 0),
         response <- Enum.to_list(response),
         :ok <- check_for_error(response) do
      data = Keyword.fetch!(response, :ok)

      if opts[:return_headers] do
        {:ok, data, %{headers: headers, trailers: Keyword.get(response, :trailers)}}
      else
        {:ok, data}
      end
    end
  end

  defp response_data_bidirectional_stream(grpc_stream, opts) do
    path = get_full_path(grpc_stream)

    {:ok, pid} =
      BidirectionalStream.start_link(
        grpc_stream,
        path,
        opts,
        grpc_stream.payload.stream_state_opts[:timeout] || :infinity
      )

    Stream.unfold(
      pid,
      fn pid ->
        case BidirectionalStream.next_item(pid) do
          nil -> nil
          response -> {response, pid}
        end
      end
    )
  end

  defp response_data_stream(grpc_stream, stream_request_pid, opts) do
    state = %{
      grpc_stream: grpc_stream,
      stream_request_pid: stream_request_pid,
      buffer: <<>>,
      opts: opts
    }

    Stream.unfold(state, fn state ->
      next_response(state)
    end)
  end

  defp next_response(state) do
    state.stream_request_pid
    |> StreamRequestProcess.next_response()
    |> read_stream(state)
  end

  defp read_stream({header_or_trailer, headers}, state)
       when header_or_trailer in [:headers, :trailers] do
    state = %{state | grpc_stream: check_compression(headers, state.grpc_stream)}

    if header_or_trailer == :headers || state.opts[:return_headers] do
      case parse_headers(headers) do
        {:ok, headers} -> {{header_or_trailer, headers}, state}
        error -> {error, state}
      end
    else
      next_response(state)
    end
  end

  defp read_stream({:data, data}, state) do
    case GRPC.Message.get_message(state.buffer <> data, state.grpc_stream.compressor) do
      {{_, message}, rest} ->
        reply = state.grpc_stream.codec.decode(message, state.grpc_stream.response_mod)
        new_state = Map.put(state, :buffer, rest)
        {{:ok, reply}, new_state}

      _ ->
        new_state = Map.put(state, :buffer, state.buffer <> data)
        next_response(new_state)
    end
  end

  defp read_stream({:error, :timeout}, state) do
    {{:error,
      GRPC.RPCError.exception(
        GRPC.Status.deadline_exceeded(),
        "timeout when waiting for server"
      )}, state}
  end

  defp read_stream({:error, _} = error, state) do
    {error, state}
  end

  defp read_stream(:done, _state) do
    nil
  end

  defp check_for_error(responses) do
    error = Keyword.get(responses, :error)

    if error, do: {:error, error}, else: :ok
  end

  defp get_full_path(%{channel: %{host: host, port: port, scheme: scheme}, path: path}) do
    "#{scheme}://#{host}:#{port}#{path}"
  end

  defp check_compression(headers, stream) do
    headers_map = Map.new(headers)
    encoding = headers_map["grpc-encoding"]

    if encoding do
      encoding = Enum.find(stream.accepted_compressors, nil, fn c -> c.name() == encoding end)
      Map.put(stream, :compressor, encoding)
    else
      stream
    end
  end

  defp parse_headers(headers) do
    headers = GRPC.Transport.HTTP2.decode_headers(headers)

    if headers["grpc-status"] do
      grpc_status = String.to_integer(headers["grpc-status"])

      if grpc_status == GRPC.Status.ok() do
        {:ok, headers}
      else
        {:error, %GRPC.RPCError{status: grpc_status, message: headers["grpc-message"]}}
      end
    else
      {:ok, headers}
    end
  end
end

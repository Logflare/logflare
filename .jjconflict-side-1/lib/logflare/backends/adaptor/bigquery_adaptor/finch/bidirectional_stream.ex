defmodule Grpc.Client.Adapters.Finch.BidirectionalStream do
  alias GRPC.Client.Adapters.Finch.StreamState
  alias Grpc.Client.Adapters.Finch.RequestUtils
  use GenServer

  def start_link(stream, path, opts, timeout) do
    GenServer.start_link(__MODULE__, [stream, path, opts, timeout])
  end

  def next_item(pid) do
    GenServer.call(pid, :next_item)
  end

  @impl true
  def init([stream, path, opts, timeout]) do
    initial_state = %{
      grpc_stream: stream,
      path: path,
      opts: opts,
      timeout: timeout,
      responses: :queue.new(),
      initial_headers_sent: false
    }

    {:ok, initial_state}
  end

  @impl true
  def handle_call(:next_item, _from, %{initial_headers_sent: false} = state) do
    with {:ok, request} <- get_next_request(state.grpc_stream),
         {:ok, {data, headers, trailers}} <- send_request(request, state) do
      responses = :queue.in(data, state.responses)
      responses = maybe_trailers(trailers, state.opts, responses)
      new_state = %{state | initial_headers_sent: true, responses: responses}
      {:reply, headers, new_state}
    else
      nil -> {:stop, :normal, nil, state}
      {:error, _error} = error -> {:stop, :normal, error, state}
      e -> e
    end
  end

  @impl true
  def handle_call(:next_item, _from, %{initial_headers_sent: true} = state) do
    if :queue.is_empty(state.responses) do
      with {:ok, request} <- get_next_request(state.grpc_stream),
           {:ok, {data, _headers, trailers}} <- send_request(request, state) do
        responses = maybe_trailers(trailers, state.opts, state.responses)
        {:reply, data, %{state | responses: responses}}
      else
        nil -> {:stop, :normal, nil, state}
        {:error, _error} = error -> {:stop, :normal, error, state}
        e -> e
      end
    else
      {{:value, response}, responses} = :queue.out(state.responses)
      {:reply, response, %{state | responses: responses}}
    end
  end

  defp maybe_trailers(trailers, opts, responses) do
    if opts[:return_headers] do
      :queue.in(trailers, responses)
    else
      responses
    end
  end

  def get_next_request(grpc_stream) do
    case StreamState.next_item(grpc_stream.payload[:stream_state_pid]) do
      :close -> nil
      item -> {:ok, item}
    end
  end

  defp send_request(request, %{grpc_stream: stream} = state) do
    client_headers =
      GRPC.Transport.HTTP2.client_headers_without_reserved(
        stream,
        stream.payload.stream_state_opts
      )

    req = Finch.build(:post, state.path, client_headers, request)

    with {:ok, %{status: 200} = response} <-
           Finch.request(req, stream.channel.adapter_payload.instance_name),
         state = %{
           state
           | grpc_stream: RequestUtils.check_compression(response.headers, state.grpc_stream)
         },
         {:ok, headers} <- RequestUtils.parse_headers(response.headers),
         {{_, message}, _rest} <-
           GRPC.Message.get_message(response.body, state.grpc_stream.compressor),
         {:ok, trailers} <- RequestUtils.parse_headers(response.trailers) do
      reply = state.grpc_stream.codec.decode(message, state.grpc_stream.response_mod)

      {:ok, {{:ok, reply}, {:headers, headers}, {:trailers, trailers}}}
    end
  end
end

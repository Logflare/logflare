defmodule Grpc.Client.Adapters.Finch.RequestProcess do
  use GenServer

  alias Grpc.Client.Adapters.Finch.StreamRequestProcess

  def start_link(
        stream_request_pid,
        finch_instance_name,
        path,
        client_headers,
        data \\ nil,
        opts \\ []
      ) do
    GenServer.start_link(__MODULE__, [
      stream_request_pid,
      finch_instance_name,
      path,
      client_headers,
      data,
      opts
    ])
  end

  @impl true
  def init([stream_request_pid, finch_instance_name, path, client_headers, data, opts]) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    req = Finch.build(:post, path, client_headers, data)

    stream_ref =
      Finch.async_request(req, finch_instance_name, receive_timeout: timeout)

    {:ok,
     %{
       stream_ref: stream_ref,
       stream_request_pid: stream_request_pid,
       recieved_headers: false,
       timeout: timeout
     }, timeout}
  end

  @impl true
  def handle_info({ref, {:status, 200}}, %{stream_ref: ref} = state) do
    {:noreply, state, state.timeout}
  end

  @impl true
  def handle_info({ref, {:headers, headers}}, %{stream_ref: ref} = state) do
    msg =
      if state.recieved_headers do
        {:trailers, headers}
      else
        {:headers, headers}
      end

    StreamRequestProcess.consume(state.stream_request_pid, msg)
    {:noreply, %{state | recieved_headers: true}, state.timeout}
  end

  @impl true
  def handle_info({ref, {:data, data}}, %{stream_ref: ref} = state) do
    StreamRequestProcess.consume(state.stream_request_pid, {:data, data})
    {:noreply, state, state.timeout}
  end

  @impl true
  def handle_info({ref, {:error, exception}}, %{stream_ref: ref} = state) do
    StreamRequestProcess.consume(state.stream_request_pid, {:error, exception})
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({ref, :done}, %{stream_ref: ref} = state) do
    StreamRequestProcess.consume(state.stream_request_pid, :done)
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(:timeout, state) do
    StreamRequestProcess.consume(state.stream_request_pid, {:error, :timeout})
    {:stop, :normal, state}
  end
end

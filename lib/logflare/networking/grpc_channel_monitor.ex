defmodule Logflare.Networking.GrpcChannelMonitor do
  @moduledoc false

  use GenServer

  require Logger

  @min_backoff 1_000
  @max_backoff 30_000

  defstruct [:idx, :url, :registry, :backoff, :channel, :conn_time]

  @spec start_link({non_neg_integer(), String.t(), atom()}) :: GenServer.on_start()
  def start_link({idx, url, registry}) do
    GenServer.start_link(__MODULE__, {idx, url, registry})
  end

  @impl GenServer
  def init({idx, url, registry}) do
    # Idle connections are disconnected after 4 minutes, start with delay to prevent reconnection storm
    connect_after(idx * 250)
    {:ok, %__MODULE__{idx: idx, url: url, registry: registry, backoff: @min_backoff}}
  end

  @impl GenServer
  def handle_info(:connect, %{channel: channel} = state) when channel != nil,
    do: {:noreply, state}

  def handle_info(:connect, %{idx: idx, url: url, registry: registry} = state) do
    case GRPC.Stub.connect(url,
           adapter: GRPC.Client.Adapters.Mint,
           interceptors: [Logflare.Networking.GrpcAuthInterceptor],
           compressor: GRPC.Compressor.Gzip,
           # Reset to Mint defaults, avoiding grpc bug https://github.com/elixir-grpc/grpc/issues/507
           cred: GRPC.Credential.new([])
         ) do
      {:ok, channel} ->
        Registry.register(registry, idx, channel)

        {:noreply,
         %{state | backoff: @min_backoff, channel: channel, conn_time: System.os_time(:second)}}

      {:error, reason} ->
        Logger.warning(
          "GrpcChannelMonitor[#{idx}]: connect failed (#{inspect(reason)}), retry in #{state.backoff}ms"
        )

        connect_after(state.backoff)
        {:noreply, %{state | backoff: min(state.backoff * 2, @max_backoff)}}
    end
  end

  def handle_info({:elixir_grpc, :connection_down, _pid}, state) do
    Logger.warning(
      "GrpcChannelMonitor[#{state.idx}]: connection down after #{System.os_time(:second) - state.conn_time}s, reconnecting"
    )

    {:noreply, handle_disconnection(state)}
  end

  # GRPC.Client.Adapters.Mint calls Process.flag(:trap_exit, true) on the caller;
  # handle exit after :connection_down
  def handle_info({:EXIT, _pid, _reason}, %{channel: nil} = state) do
    {:noreply, state}
  end

  # handle crashes of the ConnectionProcess that don't emit :connection_down
  def handle_info({:EXIT, _pid, _reason}, state) do
    Logger.warning("GrpcChannelMonitor[#{state.idx}]: connection process exited, reconnecting")
    {:noreply, handle_disconnection(state)}
  end

  defp handle_disconnection(%{idx: idx, registry: registry, channel: channel} = state) do
    Registry.unregister(registry, idx)

    if channel do
      try do
        GRPC.Stub.disconnect(channel)
      catch
        :exit, {:noproc, _call} -> :ok
      end
    end

    connect_after(0)
    %{state | backoff: @min_backoff, channel: nil}
  end

  defp connect_after(timeout) do
    send_after =
      Application.get_env(:logflare, __MODULE__, [])
      |> Keyword.get(:send_after, &Process.send_after/3)

    send_after.(self(), :connect, timeout)
  end
end

defmodule Logflare.Networking.GrpcChannelMonitor do
  @moduledoc false

  use GenServer

  require Logger

  @min_backoff 1_000
  @max_backoff 30_000

  defstruct [:idx, :url, :registry, :backoff, connected?: false]

  @spec start_link({non_neg_integer(), String.t(), atom()}) :: GenServer.on_start()
  def start_link({idx, url, registry}) do
    GenServer.start_link(__MODULE__, {idx, url, registry})
  end

  @impl GenServer
  def init({idx, url, registry}) do
    send(self(), :connect)
    {:ok, %__MODULE__{idx: idx, url: url, registry: registry, backoff: @min_backoff}}
  end

  @impl GenServer
  def handle_info(:connect, %{connected?: true} = state), do: {:noreply, state}

  def handle_info(:connect, %{idx: idx, url: url, registry: registry} = state) do
    case GRPC.Stub.connect(url,
           adapter: GRPC.Client.Adapters.Mint,
           interceptors: [Logflare.Networking.GrpcAuthInterceptor]
         ) do
      {:ok, channel} ->
        Registry.register(registry, idx, channel)
        {:noreply, %{state | backoff: @min_backoff, connected?: true}}

      {:error, reason} ->
        Logger.warning(
          "GrpcChannelMonitor[#{idx}]: connect failed (#{inspect(reason)}), retry in #{state.backoff}ms"
        )

        Process.send_after(self(), :connect, state.backoff)
        {:noreply, %{state | backoff: min(state.backoff * 2, @max_backoff)}}
    end
  end

  def handle_info({:elixir_grpc, :connection_down, _pid}, state) do
    Logger.warning("GrpcChannelMonitor[#{state.idx}]: connection down, reconnecting")
    {:noreply, handle_disconnection(state)}
  end

  # GRPC.Client.Adapters.Mint calls Process.flag(:trap_exit, true) on the caller;
  # handle crashes of the ConnectionProcess that don't emit :connection_down
  def handle_info({:EXIT, _pid, _reason}, state) do
    Logger.warning("GrpcChannelMonitor[#{state.idx}]: connection process exited, reconnecting")
    {:noreply, handle_disconnection(state)}
  end

  defp handle_disconnection(%{idx: idx, registry: registry} = state) do
    Registry.unregister(registry, idx)
    send(self(), :connect)
    %{state | backoff: @min_backoff, connected?: false}
  end
end

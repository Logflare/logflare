defmodule LogflareGrpc.Shutdown do
  @moduledoc """
  Drains Cowboy connections concurrently before Ranch's serial connection-supervisor teardown.
  Start this child before the ingestion drainer and after the gRPC server. Leave the server
  running while all connections get one shared opportunity to finish sending responses.
  Cowboy handles a system terminate request by starting GOAWAY, not by exiting immediately.
  The zero call timeout sends the request without waiting for each connection to reply.
  Ranch drops connections left over after the shared wait instead of draining them again.
  """

  use GenServer, shutdown: 7_000

  require Logger

  alias Logflare.IngressDrain
  alias Logflare.Utils.Tasks

  @goaway_ms 5_000
  @wire_timeout_ms 6_000

  @spec protocol_options() :: keyword()
  def protocol_options do
    [goaway_initial_timeout: 0, goaway_complete_timeout: @goaway_ms, shutdown_timeout: @goaway_ms]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    {:ok, opts}
  end

  @impl GenServer
  def terminate(:shutdown, opts) do
    result = Tasks.run_within(fn -> drain(opts) end, @wire_timeout_ms)

    case result do
      {:ok, :ok} ->
        :ok

      {:ok, {:error, reason}} ->
        Logger.warning("shutdown: gRPC wire drain failed: #{inspect(reason)}")

      :timeout ->
        Logger.warning("shutdown: gRPC wire drain timed out")

      :error ->
        Logger.warning("shutdown: gRPC wire drain failed")
    end
  end

  def terminate(_reason, _opts), do: :ok

  defp drain(opts) do
    case Keyword.get_lazy(opts, :listener, &IngressDrain.grpc_listener/0) do
      nil -> :ok
      ref -> drain_listener(ref, Keyword.get(opts, :wait?, true))
    end
  end

  defp drain_listener(ref, wait?) do
    transport = :ranch.get_transport_options(ref)

    with :ok <- :ranch.set_transport_options(ref, Map.put(transport, :shutdown, :brutal_kill)),
         :ok <- :ranch.suspend_listener(ref) do
      if wait?, do: drain_connections(ref), else: :ok
    end
  end

  defp drain_connections(ref) do
    count =
      for pid <- :ranch.procs(ref, :connections), is_pid(pid), reduce: 0 do
        count ->
          Process.monitor(pid)
          initiate_goaway(pid)
          count + 1
      end

    await_connections(count)
  end

  defp await_connections(0), do: :ok

  defp await_connections(count) do
    receive do
      {:DOWN, _monitor, :process, _pid, _reason} -> await_connections(count - 1)
    end
  end

  defp initiate_goaway(pid) do
    :sys.terminate(pid, :shutdown, 0)
  catch
    :exit, {_reason, {:sys, :terminate, _args}} -> :ok
  end
end

defmodule Logflare.Test.ShutdownProbeApplication do
  use Application

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.DurableBuffer.Backends.Cloud
  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolBuffer
  alias Logflare.Backends.Spool.Health
  alias Logflare.Backends.Spool.ObjectDeleter
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.IngressGate
  alias Logflare.Readiness
  alias Logflare.Shutdown
  alias Logflare.SigtermHandler
  alias Logflare.Test.ShutdownProbePlug
  alias Logflare.Test.ShutdownProbeStorage

  @impl true
  def start(_type, {probe_pid, spool_path}) do
    Readiness.initialize()
    IngressGate.initialize()
    Health.initialize()

    listener =
      Supervisor.child_spec(
        {Bandit,
         plug: {ShutdownProbePlug, {probe_pid, spool_path}},
         ip: {127, 0, 0, 1},
         port: 0,
         startup_log: false},
        id: {LogflareWeb.Endpoint, :http}
      )

    spool =
      {DurableBuffer,
       name: SpoolBuffer.name(),
       partitions: 1,
       flush_delay_ms: 60_000,
       backend:
         {Cloud,
          bucket: spool_path, storage_mod: ShutdownProbeStorage, queue_mod: nil, compress: false}}

    endpoint = %{
      id: LogflareWeb.Endpoint,
      start:
        {Supervisor, :start_link,
         [[listener], [strategy: :one_for_one, name: LogflareWeb.Endpoint]]},
      type: :supervisor
    }

    children = [
      {PartitionSupervisor,
       child_spec: Task.Supervisor, name: Logflare.TaskSupervisors, partitions: 1},
      IngestEventQueue,
      SpoolAck,
      ObjectDeleter,
      spool,
      endpoint,
      Shutdown
    ]

    {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

    :ok =
      :gen_event.swap_sup_handler(
        :erl_signal_server,
        {:erl_signal_handler, []},
        {SigtermHandler, []}
      )

    Readiness.mark_ready()
    {:ok, supervisor}
  end

  @impl true
  def prep_stop(state), do: Logflare.Application.prep_stop(state)
end

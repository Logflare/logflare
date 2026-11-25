alias Logflare.Backends.Spool.SpoolAck
alias Logflare.IngressGate
alias Logflare.Readiness
alias Logflare.Test.ShutdownProbeApplication

[mode, spool_path] = System.argv()
Application.put_env(:logflare, :sigterm_shutdown_grace_period_ms, 200)

Application.put_env(
  :logflare,
  :shutdown_timeout_ms,
  if(mode == "blocked_flush", do: 1_500, else: 4_000)
)

Application.put_env(:logflare, :shutdown_request_timeout_ms, 2_000)
Application.put_env(:logflare, :shutdown_drain_timeout_ms, 500)
Application.put_env(:logflare, :spool, mode: :producer)

if mode in ["blocked_flush", "late_request"] do
  {:ok, _apps} = Application.ensure_all_started(:ex_unit)
  {:ok, _apps} = Application.ensure_all_started(:mimic)
  Mimic.copy(SpoolAck)
  Mimic.set_mimic_global()
end

if mode == "late_request" do
  Mimic.stub(SpoolAck, :flush, fn _timeout ->
    :persistent_term.put({SpoolAck, :probe_drainer}, self())
    IO.binwrite(:stdio, "SHUTDOWN_PROBE_SYNC_FINISHED\n")

    receive do
      :finish_cleanup -> :ok
    end
  end)
end

if mode == "blocked_flush" do
  Mimic.stub(SpoolAck, :pending_count, fn ->
    IO.binwrite(:stdio, "SHUTDOWN_PROBE_FLUSH_BLOCKED\n")
    Process.sleep(:infinity)
  end)
end

:ok =
  :application.load(
    {:application, :shutdown_probe,
     [
       mod: {ShutdownProbeApplication, {self(), spool_path}},
       applications: [:kernel, :stdlib, :elixir, :bandit, :cowboy, :durable_buffer]
     ]}
  )

{:ok, _apps} = Application.ensure_all_started(:shutdown_probe)
{:ok, server} = Bandit.PhoenixAdapter.bandit_pid(LogflareWeb.Endpoint)
{:ok, {_address, port}} = ThousandIsland.listener_info(server)

parent = self()

read_commands = fn read_commands ->
  case IO.gets("") do
    "release\n" ->
      send(parent, :release)
      read_commands.(read_commands)

    "finish_cleanup\n" ->
      send(:persistent_term.get({SpoolAck, :probe_drainer}), :finish_cleanup)

    _ ->
      System.stop()
  end
end

spawn(fn -> read_commands.(read_commands) end)

IO.binwrite(:stdio, "SHUTDOWN_PROBE_READY #{System.pid()} #{port}\n")

loop = fn loop, held ->
  receive do
    {:held_http, pid} ->
      IO.binwrite(:stdio, "SHUTDOWN_PROBE_HELD\n")
      send(self(), :check_gate)
      loop.(loop, pid)

    :check_gate ->
      if IngressGate.open?() do
        Process.send_after(self(), :check_gate, 10)
      else
        IO.binwrite(:stdio, "SHUTDOWN_PROBE_CLOSED ready=#{Readiness.ready?()}\n")
      end

      loop.(loop, held)

    :release ->
      send(held, :release)
      loop.(loop, held)
  end
end

loop.(loop, nil)

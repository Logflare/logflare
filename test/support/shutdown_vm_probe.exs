Application.ensure_all_started(:bandit)
Application.ensure_all_started(:cowboy)
Application.put_env(:logflare, :sigterm_shutdown_grace_period_ms, 200)
Application.put_env(:logflare, :shutdown_timeout_ms, 3_000)
Application.put_env(:logflare, :shutdown_request_timeout_ms, 2_000)
Application.delete_env(:logflare, :shutdown_drain_timeout_ms)

Logflare.Readiness.initialize()
Logflare.Readiness.mark_ready()
Logflare.IngressGate.initialize()

spec =
  Supervisor.child_spec(
    {Bandit,
     plug: {Logflare.Test.ShutdownProbePlug, self()},
     ip: {127, 0, 0, 1},
     port: 0,
     startup_log: false},
    id: {LogflareWeb.Endpoint, :http}
  )

{:ok, _supervisor} =
  Supervisor.start_link([spec], name: LogflareWeb.Endpoint, strategy: :one_for_one)

{:ok, server} = Bandit.PhoenixAdapter.bandit_pid(LogflareWeb.Endpoint)
{:ok, {_address, port}} = ThousandIsland.listener_info(server)

:ok =
  :gen_event.swap_sup_handler(
    :erl_signal_server,
    {:erl_signal_handler, []},
    {Logflare.SigtermHandler, []}
  )

parent = self()

spawn(fn ->
  case IO.gets("") do
    "release\n" -> send(parent, :release)
    _ -> send(parent, :stop)
  end
end)

IO.binwrite(:stdio, "SHUTDOWN_PROBE_READY #{System.pid()} #{port}\n")

loop = fn loop, held ->
  receive do
    {:held_http, pid} ->
      IO.binwrite(:stdio, "SHUTDOWN_PROBE_HELD\n")
      send(self(), :check_gate)
      loop.(loop, pid)

    :check_gate ->
      if Logflare.IngressGate.open?() do
        Process.send_after(self(), :check_gate, 10)
      else
        IO.binwrite(:stdio, "SHUTDOWN_PROBE_CLOSED\n")
      end

      loop.(loop, held)

    :release ->
      send(held, :release)
      loop.(loop, held)

    :stop ->
      System.stop()
      loop.(loop, held)
  end
end

loop.(loop, nil)

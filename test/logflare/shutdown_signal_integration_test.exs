defmodule Logflare.ShutdownSignalIntegrationTest do
  use ExUnit.Case, async: false

  @tag timeout: 30_000
  test "real SIGTERM preserves an HTTP/2 response held beyond the withdrawal window" do
    {port, os_pid, conn} = start_probe()
    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/hold", [], nil)
    await_line(port, "SHUTDOWN_PROBE_HELD", 5_000)

    assert {_output, 0} = System.cmd("kill", ["-TERM", os_pid])
    await_line(port, "withdrawing from traffic", 5_000)
    assert {_output, 0} = System.cmd("kill", ["-TERM", os_pid])
    await_line(port, "ignoring repeated sigterm", 5_000)
    await_line(port, "SHUTDOWN_PROBE_CLOSED", 5_000)

    Port.command(port, "release\n")
    {conn, responses} = response(conn, ref)
    assert {:status, ref, 200} in responses
    assert {:data, ref, "ok"} in responses
    assert_receive {^port, {:exit_status, 0}}, 10_000
    Mint.HTTP.close(conn)
  end

  @tag timeout: 30_000
  test "real SIGTERM times out on a stuck admitted request rather than hanging indefinitely" do
    {port, os_pid, conn} = start_probe()
    {:ok, conn, _ref} = Mint.HTTP.request(conn, "GET", "/hold", [], nil)
    await_line(port, "SHUTDOWN_PROBE_HELD", 5_000)
    started = System.monotonic_time(:millisecond)
    assert {_output, 0} = System.cmd("kill", ["-TERM", os_pid])
    await_line(port, "ingress result=timeout", 5_000)
    assert_receive {^port, {:exit_status, 0}}, 5_000
    assert System.monotonic_time(:millisecond) - started < 8_000
    Mint.HTTP.close(conn)
  end

  defp start_probe do
    paths = Enum.flat_map(:code.get_path(), &["-pa", List.to_string(&1)])
    script = Path.expand("../support/shutdown_vm_probe.exs", __DIR__)
    executable = System.find_executable("elixir")

    port =
      Port.open({:spawn_executable, executable}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:line, 4096},
        args: paths ++ [script]
      ])

    on_exit(fn -> if Port.info(port), do: Port.close(port) end)

    ready = await_line(port, "SHUTDOWN_PROBE_READY", 10_000)
    ["SHUTDOWN_PROBE_READY", os_pid, listen_port] = String.split(ready)

    {:ok, conn} =
      Mint.HTTP.connect(:http, "127.0.0.1", String.to_integer(listen_port),
        protocols: [:http2],
        mode: :passive
      )

    {port, os_pid, conn}
  end

  defp response(conn, ref, responses \\ []) do
    if {:done, ref} in responses do
      {conn, responses}
    else
      {:ok, conn, next} = Mint.HTTP.recv(conn, 0, 5_000)
      response(conn, ref, responses ++ next)
    end
  end

  defp await_line(port, text, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    receive_line(port, text, deadline, [])
  end

  defp receive_line(port, text, deadline, seen) do
    receive do
      {^port, {:data, {_kind, line}}} ->
        if String.contains?(line, text),
          do: line,
          else: receive_line(port, text, deadline, [line | seen])

      {^port, {:exit_status, status}} ->
        flunk("probe exited #{status}: #{inspect(Enum.reverse(seen))}")
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        flunk("probe did not emit #{inspect(text)}: #{inspect(Enum.reverse(seen))}")
    end
  end
end

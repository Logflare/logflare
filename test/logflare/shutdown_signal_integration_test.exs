defmodule Logflare.ShutdownSignalIntegrationTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.Encoder

  @moduletag timeout: 30_000
  @moduletag :tmp_dir

  test "SIGTERM preserves an admitted HTTP/2 response and flushes its event before stopping", %{
    tmp_dir: tmp_dir
  } do
    spool_path = Path.join(tmp_dir, "accepted-event")
    {port, os_pid, conn} = start_probe("normal", spool_path)
    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/hold", [], nil)
    await_line(port, "SHUTDOWN_PROBE_HELD", 5_000)

    assert {_output, 0} = System.cmd("kill", ["-TERM", os_pid])
    await_line(port, "withdrawing from traffic", 5_000)
    assert {_output, 0} = System.cmd("kill", ["-TERM", os_pid])
    await_line(port, "ignoring repeated sigterm", 5_000)
    await_line(port, "SHUTDOWN_PROBE_CLOSED ready=false", 5_000)
    refute File.exists?(spool_path)

    Port.command(port, "release\n")
    {conn, responses} = response(conn, ref)
    assert {:status, ref, 200} in responses
    assert {:data, ref, "ok"} in responses
    await_line(port, "shutdown: supervised drain finished result=drained", 5_000)
    assert_receive {^port, {:exit_status, 0}}, 5_000

    assert {[segment], _valid_bytes, <<>>} =
             spool_path |> File.read!() |> Encoder.decode_segments()

    assert [%{body: %{"message" => "accepted-event"}}] = :erlang.binary_to_term(segment, [:safe])
    Mint.HTTP.close(conn)
  end

  test "SIGTERM times out on a stuck admitted request rather than hanging indefinitely", %{
    tmp_dir: tmp_dir
  } do
    {port, os_pid, conn} = start_probe("normal", Path.join(tmp_dir, "stuck-request"))
    {:ok, conn, _ref} = Mint.HTTP.request(conn, "GET", "/hold", [], nil)
    await_line(port, "SHUTDOWN_PROBE_HELD", 5_000)
    started = System.monotonic_time(:millisecond)
    assert {_output, 0} = System.cmd("kill", ["-TERM", os_pid])
    await_line(port, "ingress result=timeout", 5_000)
    await_line(port, "shutdown: supervised drain finished result=timeout", 5_000)
    assert_receive {^port, {:exit_status, 0}}, 5_000
    assert System.monotonic_time(:millisecond) - started < 8_000
    Mint.HTTP.close(conn)
  end

  test "an admitted write that misses the final spool sync receives a retryable response", %{
    tmp_dir: tmp_dir
  } do
    spool_path = Path.join(tmp_dir, "late-event")
    {port, os_pid, conn} = start_probe("late_request", spool_path)
    {:ok, conn, ref} = Mint.HTTP.request(conn, "GET", "/hold", [], nil)
    await_line(port, "SHUTDOWN_PROBE_HELD", 5_000)

    assert {_output, 0} = System.cmd("kill", ["-TERM", os_pid])
    await_line(port, "ingress result=timeout", 5_000)
    await_line(port, "SHUTDOWN_PROBE_SYNC_FINISHED", 5_000)
    Port.command(port, "release\n")

    {conn, responses} = response(conn, ref)
    assert {:status, ref, 503} in responses

    Port.command(port, "finish_cleanup\n")
    await_line(port, "shutdown: supervised drain finished result=timeout", 5_000)
    assert_receive {^port, {:exit_status, 0}}, 5_000
    refute File.exists?(spool_path)
    Mint.HTTP.close(conn)
  end

  test "the supervisor kills a drain blocked in the final flush after SIGQUIT", %{
    tmp_dir: tmp_dir
  } do
    {port, os_pid, conn} = start_probe("blocked_flush", Path.join(tmp_dir, "blocked-flush"))
    started = System.monotonic_time(:millisecond)

    assert {_output, 0} = System.cmd("kill", ["-QUIT", os_pid])
    await_line(port, "SHUTDOWN_PROBE_FLUSH_BLOCKED", 5_000)
    assert_receive {^port, {:exit_status, 0}}, 5_000
    assert System.monotonic_time(:millisecond) - started < 6_000
    Mint.HTTP.close(conn)
  end

  defp start_probe(mode, spool_path) do
    paths = Enum.flat_map(:code.get_path(), &["-pa", List.to_string(&1)])
    script = Path.expand("../support/shutdown_vm_probe.exs", __DIR__)
    executable = System.find_executable("elixir")

    port =
      Port.open({:spawn_executable, executable}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        {:line, 4096},
        args: paths ++ [script, mode, spool_path]
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

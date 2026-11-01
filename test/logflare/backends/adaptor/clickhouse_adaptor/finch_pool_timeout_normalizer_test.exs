defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.FinchPoolTimeoutNormalizerTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.FinchPoolTimeoutNormalizer

  @pool __MODULE__.Pool

  describe "call/3 against a saturated HTTP/1 pool" do
    setup do
      start_supervised!(
        {Finch, name: @pool, pools: %{default: [protocols: [:http1], size: 1, count: 1]}}
      )

      {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listen_socket)
      test_pid = self()
      spawn_link(fn -> accept_loop(listen_socket, test_pid) end)

      url = "http://127.0.0.1:#{port}/"

      # Occupies the pool's only connection until the test ends, so the request under
      # test has to wait for a connection that never frees up.
      # A bare process rather than a Task: on_exit runs outside the owner, and Task
      # shutdown is only legal from the owning process.
      holder =
        spawn(fn ->
          Finch.build(:get, url) |> Finch.request(@pool, receive_timeout: :infinity)
        end)

      on_exit(fn -> Process.exit(holder, :kill) end)

      # The server only sees a request once the holder has checked the connection out.
      assert_receive :request_received, 5_000

      [url: url]
    end

    test "returns a tagged error instead of raising", %{url: url} do
      {client, _retry_calls} = client_with_retry_counter(0)

      assert {:error, :pool_timeout} = Tesla.get(client, url)
    end

    test "surfaces the timeout to the retry middleware", %{url: url} do
      {client, retry_calls} = client_with_retry_counter(1)

      assert {:error, :pool_timeout} = Tesla.get(client, url)

      # The point of normalizing at all: a raise never reaches should_retry, so before
      # this middleware existed the pool timeout was unretriable by construction.
      assert :counters.get(retry_calls, 1) >= 1
    end
  end

  describe "call/3 with an unrelated failure" do
    test "re-raises a RuntimeError it does not recognize" do
      client = Tesla.client([FinchPoolTimeoutNormalizer], fn _env -> raise "unrelated boom" end)

      assert_raise RuntimeError, "unrelated boom", fn -> Tesla.get(client, "/") end
    end

    test "leaves an ordinary error response untouched" do
      client = Tesla.client([FinchPoolTimeoutNormalizer], fn _env -> {:error, :econnrefused} end)

      assert {:error, :econnrefused} = Tesla.get(client, "/")
    end
  end

  defp client_with_retry_counter(max_retries) do
    retry_calls = :counters.new(1, [])

    middleware = [
      {Tesla.Middleware.Retry,
       delay: 10,
       max_retries: max_retries,
       should_retry: fn result ->
         :counters.add(retry_calls, 1, 1)
         match?({:error, _reason}, result)
       end},
      FinchPoolTimeoutNormalizer
    ]

    adapter = {Tesla.Adapter.Finch, name: @pool, pool_timeout: 100, receive_timeout: 5_000}

    {Tesla.client(middleware, adapter), retry_calls}
  end

  # Never responds: each connection stays busy until the client closes it.
  defp accept_loop(listen_socket, test_pid) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)

    spawn(fn ->
      {:ok, _request} = :gen_tcp.recv(socket, 0)
      send(test_pid, :request_received)
      :gen_tcp.recv(socket, 0)
    end)

    accept_loop(listen_socket, test_pid)
  end
end

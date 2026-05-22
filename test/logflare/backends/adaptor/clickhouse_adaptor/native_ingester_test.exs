defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngesterTest do
  @moduledoc false

  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool

  doctest NativeIngester, import: true

  setup :set_mimic_global

  setup do
    insert(:plan, name: "Free")
    {source, backend} = setup_clickhouse_test()

    [source: source, backend: backend]
  end

  describe "retry on transient connection errors" do
    test "retries on :closed and succeeds on second attempt", %{
      source: source,
      backend: backend
    } do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, :closed}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on :timeout", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, :timeout}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on :econnrefused", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, :econnrefused}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on :econnreset", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, :econnreset}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on :checkout_timeout", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        exit({:timeout, {NimblePool, :checkout, []}})
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end
  end

  describe "retry on transient ClickHouse exception codes" do
    test "retries on TOO_MANY_PARTS (252)", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 252, "Too many parts"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on TIMEOUT_EXCEEDED (159)", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 159, "Timeout exceeded"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on TOO_MANY_SIMULTANEOUS_QUERIES (202)", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 202, "Too many simultaneous queries"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on MEMORY_LIMIT_EXCEEDED (241)", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 241, "Memory limit exceeded"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end

    test "retries on READONLY (164)", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 164, "Table is in readonly mode"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end
  end

  describe "retry_strategy/1" do
    test "classifies connection errors as immediate retries" do
      for reason <- [:closed, :timeout, :econnrefused, :econnreset, :checkout_timeout] do
        assert NativeIngester.retry_strategy(reason) == :immediate
      end
    end

    test "classifies transient ClickHouse exception codes as delayed retries" do
      for code <- [32, 159, 164, 202, 241, 252] do
        assert {:delay, delay} = NativeIngester.retry_strategy({:exception, code, "boom"})
        assert delay > 0
      end
    end

    test "classifies non-transient errors as no retry" do
      assert NativeIngester.retry_strategy({:exception, 60, "Unknown table"}) == :no_retry
      assert NativeIngester.retry_strategy({:column_mismatch, expected: [], got: []}) == :no_retry
      assert NativeIngester.retry_strategy({:unexpected_packet_type, 99}) == :no_retry
      assert NativeIngester.retry_strategy(:some_other_error) == :no_retry
    end
  end

  describe "retry timing" do
    test "retries connection errors immediately without backoff", %{
      source: source,
      backend: backend
    } do
      Mimic.expect(Pool, :checkout, fn _backend, _fun -> {:error, :closed} end)
      Mimic.expect(Pool, :checkout, fn _backend, _fun -> :ok end)

      events = [build(:log_event, source: source)]

      {elapsed_us, result} =
        :timer.tc(fn -> NativeIngester.insert(backend, "some_table", events, :log) end)

      assert result == :ok
      assert System.convert_time_unit(elapsed_us, :microsecond, :millisecond) < 200
    end
  end

  describe "no retry on non-transient errors" do
    test "does not retry on :column_mismatch", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:column_mismatch, expected: [], got: []}}
      end)

      events = [build(:log_event, source: source)]

      assert {:error, {:column_mismatch, _}} =
               NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      refute_received :attempt
    end

    test "does not retry on :unexpected_packet_type", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:unexpected_packet_type, 99}}
      end)

      events = [build(:log_event, source: source)]

      assert {:error, {:unexpected_packet_type, 99}} =
               NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      refute_received :attempt
    end

    test "does not retry on non-transient exception codes", %{source: source, backend: backend} do
      test_pid = self()

      # Code 60 = UNKNOWN_TABLE
      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 60, "Table does not exist"}}
      end)

      events = [build(:log_event, source: source)]

      assert {:error, {:exception, 60, "Table does not exist"}} =
               NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      refute_received :attempt
    end
  end

  describe "retry exhaustion" do
    test "returns error after max retries exhausted", %{source: source, backend: backend} do
      test_pid = self()

      # First attempt
      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, :closed}
      end)

      # Retry attempt (max_retries = 1, so this is the last try)
      Mimic.expect(Pool, :checkout, fn _backend, _index, _fun ->
        send(test_pid, :attempt)
        {:error, :closed}
      end)

      events = [build(:log_event, source: source)]
      assert {:error, :closed} = NativeIngester.insert(backend, "some_table", events, :log)

      # Should have been called exactly twice (initial + 1 retry)
      assert_received :attempt
      assert_received :attempt
      refute_received :attempt
    end
  end

  describe "opts passthrough" do
    test "passes the provided opts through to the connection query", %{
      source: source,
      backend: backend
    } do
      test_pid = self()

      Mimic.stub(Connection, :send_query, fn _conn, _sql, opts ->
        send(test_pid, {:captured_opts, opts})
        {:error, :stop}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, fun ->
        fun.(%{database: backend.config.database})
        {:error, :stop}
      end)

      events = [build(:log_event, source: source)]

      opts = [
        async_insert: 1,
        wait_for_async_insert: 1,
        async_insert_busy_timeout_max_ms: 3_000
      ]

      NativeIngester.insert(backend, "some_table", events, :log, opts)

      assert_received {:captured_opts, ^opts}
    end

    test "passes empty opts by default", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.stub(Connection, :send_query, fn _conn, _sql, opts ->
        send(test_pid, {:captured_opts, opts})
        {:error, :stop}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, fun ->
        fun.(%{database: backend.config.database})
        {:error, :stop}
      end)

      events = [build(:log_event, source: source)]

      NativeIngester.insert(backend, "some_table", events, :log)

      assert_received {:captured_opts, []}
    end
  end
end

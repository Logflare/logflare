defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngesterTest do
  @moduledoc false

  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool

  setup :set_mimic_global

  setup do
    insert(:plan, name: "Free")
    {source, backend, cleanup_fn} = setup_clickhouse_test()

    on_exit(fn -> cleanup_fn.() end)

    [source: source, backend: backend]
  end

  describe "retry on transient connection errors" do
    test "retries on :closed and succeeds on second attempt", %{
      source: source,
      backend: backend
    } do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, :closed}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, :timeout}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, :econnrefused}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, :econnreset}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        exit({:timeout, {NimblePool, :checkout, []}})
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 252, "Too many parts"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 159, "Timeout exceeded"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 202, "Too many simultaneous queries"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 241, "Memory limit exceeded"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, {:exception, 164, "Table is in readonly mode"}}
      end)

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        :ok
      end)

      events = [build(:log_event, source: source)]
      assert :ok = NativeIngester.insert(backend, "some_table", events, :log)

      assert_received :attempt
      assert_received :attempt
    end
  end

  describe "no retry on non-transient errors" do
    test "does not retry on :column_mismatch", %{source: source, backend: backend} do
      test_pid = self()

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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

      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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
      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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
      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
        send(test_pid, :attempt)
        {:error, :closed}
      end)

      # Retry attempt (max_retries = 1, so this is the last try)
      Mimic.expect(Pool, :checkout, fn _backend, _fun ->
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
end

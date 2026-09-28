defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.CircuitBreakerTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.CircuitBreaker

  setup do
    insert(:plan, name: "Free")

    backend = insert(:backend, type: :clickhouse)

    Application.put_env(:logflare, CircuitBreaker,
      max_failures: 3,
      window_ms: 60_000,
      block_ms: 60_000
    )

    on_exit(fn -> Application.delete_env(:logflare, CircuitBreaker) end)

    [backend: backend]
  end

  describe "check/1" do
    test "returns :ok when no breaker is running (fail-safe)", %{backend: backend} do
      assert :ok = CircuitBreaker.check(backend)
    end

    test "returns :ok for a running breaker with no failures", %{backend: backend} do
      start_supervised!({CircuitBreaker, backend})

      assert :ok = CircuitBreaker.check(backend)
    end
  end

  describe "record_failure/1 and tripping" do
    test "stays closed below the failure threshold", %{backend: backend} do
      start_supervised!({CircuitBreaker, backend})

      record_failures(backend, 2)
      CircuitBreaker.get_state(backend)

      assert :ok = CircuitBreaker.check(backend)
    end

    test "opens once the failure threshold is reached", %{backend: backend} do
      start_supervised!({CircuitBreaker, backend})

      record_failures(backend, 3)
      CircuitBreaker.get_state(backend)

      assert {:error, :circuit_open, blocked_until} = CircuitBreaker.check(backend)
      assert is_integer(blocked_until)
    end

    test "auto-closes after the block window expires", %{backend: backend} do
      Application.put_env(:logflare, CircuitBreaker,
        max_failures: 3,
        window_ms: 60_000,
        block_ms: 50
      )

      start_supervised!({CircuitBreaker, backend})

      record_failures(backend, 3)
      CircuitBreaker.get_state(backend)

      assert {:error, :circuit_open, _} = CircuitBreaker.check(backend)

      Process.sleep(80)

      assert :ok = CircuitBreaker.check(backend)
    end
  end

  describe "trip/1" do
    test "opens immediately regardless of failure count", %{backend: backend} do
      start_supervised!({CircuitBreaker, backend})

      assert :ok = CircuitBreaker.check(backend)

      assert :ok = CircuitBreaker.trip(backend)

      assert {:error, :circuit_open, blocked_until} = CircuitBreaker.check(backend)
      assert is_integer(blocked_until)
    end

    test "is a no-op when no breaker is running (fail-safe)", %{backend: backend} do
      assert :ok = CircuitBreaker.trip(backend)
      assert :ok = CircuitBreaker.check(backend)
    end
  end

  describe "open telemetry" do
    setup do
      TestUtils.attach_forwarder([:logflare, :clickhouse, :circuit_breaker, :open])

      :ok
    end

    test "emits the failure count and :threshold reason when tripped by the threshold", %{
      backend: backend
    } do
      start_supervised!({CircuitBreaker, backend})

      record_failures(backend, 3)
      CircuitBreaker.get_state(backend)

      assert_receive {:telemetry_event, [:logflare, :clickhouse, :circuit_breaker, :open],
                      %{failures: 3}, metadata}

      assert metadata.backend_id == backend.id
      assert metadata.reason == :threshold
    end

    test "emits a :forced reason when opened by trip/1", %{backend: backend} do
      start_supervised!({CircuitBreaker, backend})

      assert :ok = CircuitBreaker.trip(backend)

      assert_receive {:telemetry_event, [:logflare, :clickhouse, :circuit_breaker, :open],
                      %{failures: 0}, metadata}

      assert metadata.backend_id == backend.id
      assert metadata.reason == :forced
    end

    test "does not emit while the breaker stays closed", %{backend: backend} do
      start_supervised!({CircuitBreaker, backend})

      record_failures(backend, 2)
      CircuitBreaker.get_state(backend)

      refute_received {:telemetry_event, [:logflare, :clickhouse, :circuit_breaker, :open], _, _}
    end
  end

  describe "fail-safe on process death" do
    test "reads as closed after the breaker process crashes", %{backend: backend} do
      pid = start_supervised!({CircuitBreaker, backend})

      record_failures(backend, 3)
      CircuitBreaker.get_state(backend)

      assert {:error, :circuit_open, _} = CircuitBreaker.check(backend)

      Process.unlink(pid)
      Process.exit(pid, :kill)
      wait_until_deregistered(backend)

      assert :ok = CircuitBreaker.check(backend)
    end
  end

  defp record_failures(backend, count) do
    Enum.each(1..count, fn _ -> CircuitBreaker.record_failure(backend) end)
  end

  defp wait_until_deregistered(backend, attempts \\ 50) do
    via = Backends.via_backend(backend, CircuitBreaker)

    cond do
      attempts == 0 -> :ok
      is_nil(GenServer.whereis(via)) -> :ok
      true -> Process.sleep(10) && wait_until_deregistered(backend, attempts - 1)
    end
  end
end

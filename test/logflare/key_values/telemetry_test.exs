defmodule Logflare.KeyValues.TelemetryTest do
  use ExUnit.Case, async: false

  alias Logflare.KeyValues.Telemetry

  @state_key {Telemetry, :state}
  @operations [:lookup, :extraction, :database_fetch]
  @events (for operation <- @operations, suffix <- [[], [:stop]] do
             [:logflare, :key_values, operation] ++ suffix
           end) ++ [[:logflare, :key_values, :fill]]

  setup do
    previous_state = :persistent_term.get(@state_key, nil)
    previous_config = Application.get_env(:logflare, Telemetry)
    :persistent_term.erase(@state_key)

    Application.put_env(:logflare, Telemetry,
      sample_denominators: [lookup: 1, extraction: 1, database_fetch: 1]
    )

    test_pid = self()
    handler = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler,
        @events,
        fn event, measurements, metadata, pid ->
          send(pid, {:metric, event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler)

      case previous_state do
        nil -> :persistent_term.erase(@state_key)
        state -> :persistent_term.put(@state_key, state)
      end

      case previous_config do
        nil -> Application.delete_env(:logflare, Telemetry)
        config -> Application.put_env(:logflare, Telemetry, config)
      end
    end)

    :ok
  end

  test "uninitialized instrumentation preserves calls without creating events" do
    assert Telemetry.observe(:lookup, nil, fn -> false end) == false
    assert :ok = Telemetry.fill("org.id", :warmed)
    assert :ok = Telemetry.poll()
    refute Telemetry.sample?(:lookup)
    refute_receive {:metric, _, _, _}
  end

  test "sampled calls preserve false and nil and separate accessor kinds" do
    :ok = Telemetry.initialize()

    for {accessor, value, kind, result} <- [
          {nil, false, :whole, :value},
          {"org.id", nil, :dot, :nil_value},
          {"$.org.id", "org_123", :jsonpath, :value}
        ] do
      assert Telemetry.observe(:lookup, accessor, fn -> value end) === value

      assert_receive {:metric, [:logflare, :key_values, :lookup, :stop], %{duration: duration},
                      %{
                        implementation: :accessor_cache,
                        accessor_kind: ^kind,
                        result: ^result,
                        sample_denominator: 1
                      }}

      assert is_integer(duration) and duration >= 0
    end

    assert :ok = Telemetry.poll()

    for {kind, result} <- [{:whole, :value}, {:dot, :nil_value}, {:jsonpath, :value}] do
      assert_receive {:metric, [:logflare, :key_values, :lookup], %{count: 1},
                      %{accessor_kind: ^kind, result: ^result}}
    end
  end

  test "disabled timing still exports exact operation and fill counts only on poll" do
    disable_sampling()
    :ok = Telemetry.initialize()

    assert :value = Telemetry.observe(:lookup, "org.id", fn -> :value end)
    assert nil == Telemetry.observe(:extraction, "org.id", fn -> nil end)
    assert nil == Telemetry.observe(:database_fetch, nil, fn -> nil end)
    assert :ok = Telemetry.fill("org.id", :warmed)
    assert :ok = Telemetry.fill("$.org.id", :database)
    refute_receive {:metric, _, _, _}
    assert :ok = Telemetry.poll()

    assert_receive {:metric, [:logflare, :key_values, :lookup], %{count: 1}, %{result: :value}}

    assert_receive {:metric, [:logflare, :key_values, :extraction], %{count: 1},
                    %{result: :nil_value}}

    assert_receive {:metric, [:logflare, :key_values, :database_fetch], %{count: 1},
                    %{result: :nil_value}}

    assert_receive {:metric, [:logflare, :key_values, :fill], %{count: 1}, %{source: :warmed}}
    assert_receive {:metric, [:logflare, :key_values, :fill], %{count: 1}, %{source: :database}}
    refute_receive {:metric, _, _, _}
    assert :ok = Telemetry.poll()
    refute_receive {:metric, _, _, _}
  end

  test "counts survive repeated initialization and poller restarts" do
    disable_sampling()
    :ok = Telemetry.initialize()
    Telemetry.observe(:lookup, nil, fn -> 1 end)
    :ok = Telemetry.initialize()

    Task.async(fn -> Telemetry.poll() end) |> Task.await()
    assert_receive {:metric, [:logflare, :key_values, :lookup], %{count: 1}, _}
    Task.async(fn -> Telemetry.poll() end) |> Task.await()
    refute_receive {:metric, _, _, _}

    Telemetry.observe(:lookup, nil, fn -> 2 end)
    Task.async(fn -> Telemetry.poll() end) |> Task.await()
    assert_receive {:metric, [:logflare, :key_values, :lookup], %{count: 1}, _}
  end

  test "counts keep every operation, accessor, result and fill source separate" do
    disable_sampling()
    :ok = Telemetry.initialize()
    accessors = [{nil, :whole}, {"org.id", :dot}, {"$.org.id", :jsonpath}]

    for operation <- @operations, {accessor, _kind} <- accessors do
      Telemetry.observe(operation, accessor, fn -> false end)
      Telemetry.observe(operation, accessor, fn -> nil end)

      assert_raise RuntimeError, fn ->
        Telemetry.observe(operation, accessor, fn -> raise "unavailable" end)
      end
    end

    for {accessor, _kind} <- accessors, source <- [:warmed, :database] do
      Telemetry.fill(accessor, source)
    end

    :ok = Telemetry.poll()

    for operation <- @operations,
        {_accessor, kind} <- accessors,
        result <- [:value, :nil_value, :error] do
      assert_receive {:metric, [:logflare, :key_values, ^operation], %{count: 1},
                      %{accessor_kind: ^kind, result: ^result}}
    end

    for {_accessor, kind} <- accessors, source <- [:warmed, :database] do
      assert_receive {:metric, [:logflare, :key_values, :fill], %{count: 1},
                      %{accessor_kind: ^kind, source: ^source}}
    end

    refute_receive {:metric, _, _, _}
  end

  test "concurrent increments and polls preserve every count without negative deltas" do
    disable_sampling()
    :ok = Telemetry.initialize()
    parent = self()

    workers =
      for _ <- 1..8 do
        Task.async(fn ->
          for index <- 1..1000 do
            Telemetry.observe(:lookup, "org.id", fn -> index end)
            if rem(index, 100) == 0, do: Telemetry.poll()
          end

          send(parent, :finished)
        end)
      end

    Enum.each(workers, &Task.await/1)
    for _ <- workers, do: assert_receive(:finished)
    assert :ok = Telemetry.poll()
    counts = collect_counts([])
    assert counts != []
    assert Enum.all?(counts, &(&1 > 0))
    assert Enum.sum(counts) == 8000
    assert :ok = Telemetry.poll()
    refute_receive {:metric, _, _, _}
  end

  test "errors, throws and exits retain their reason and emit error observations" do
    :ok = Telemetry.initialize()

    assert_raise ArgumentError, "bad accessor", fn ->
      Telemetry.observe(:extraction, "org.id", fn -> raise ArgumentError, "bad accessor" end)
    end

    assert catch_throw(Telemetry.observe(:lookup, nil, fn -> throw(:stop) end)) == :stop

    assert catch_exit(Telemetry.observe(:database_fetch, nil, fn -> exit(:unavailable) end)) ==
             :unavailable

    for operation <- @operations do
      assert_receive {:metric, [:logflare, :key_values, ^operation, :stop], %{duration: duration},
                      %{result: :error}}

      assert duration >= 0
    end

    assert :ok = Telemetry.poll()

    for operation <- @operations do
      assert_receive {:metric, [:logflare, :key_values, ^operation], %{count: 1},
                      %{result: :error}}
    end
  end

  test "sampling varies between invocations of the same operation" do
    Application.put_env(:logflare, Telemetry, sample_denominators: [lookup: 2])
    :ok = Telemetry.initialize()
    :rand.seed(:exsss, {11, 22, 33})
    samples = for _ <- 1..1000, do: Telemetry.sample?(:lookup)
    selected = Enum.count(samples, & &1)
    assert selected in 400..600
    assert Enum.uniq(samples) |> Enum.sort() == [false, true]
  end

  test "rejects invalid sampling denominators before installing state" do
    for value <- [0, -1, 0.5, "1", nil] do
      Application.put_env(:logflare, Telemetry, sample_denominators: [lookup: value])
      assert_raise ArgumentError, fn -> Telemetry.initialize() end
      assert :persistent_term.get(@state_key, nil) == nil
    end
  end

  defp disable_sampling do
    Application.put_env(:logflare, Telemetry,
      sample_denominators: [lookup: :disabled, extraction: :disabled, database_fetch: :disabled]
    )
  end

  defp collect_counts(counts) do
    receive do
      {:metric, [:logflare, :key_values, :lookup], %{count: count}, _metadata} ->
        collect_counts([count | counts])
    after
      0 -> counts
    end
  end
end

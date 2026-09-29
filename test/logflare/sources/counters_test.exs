defmodule Logflare.Sources.CountersTest do
  use ExUnit.Case, async: true

  alias Logflare.Sources.Counters
  alias Logflare.Sources.Source

  test "tracks every source counter without resetting an existing source" do
    counter = :sources_counters_metrics_test
    on_exit(fn -> Counters.delete(counter) end)

    assert {:ok, ^counter} = Counters.increment(counter, 5)
    assert {:ok, ^counter} = Counters.create(counter)
    assert {:ok, ^counter} = Counters.decrement(counter)
    assert {:ok, ^counter} = Counters.increment_bq_count(counter, 7)
    assert {:ok, ^counter} = Counters.increment_inserts_since_boot_count(counter, 11)
    assert {:ok, ^counter} = Counters.increment_total_cluster_inserts_count(counter, 13)
    assert {:ok, ^counter} = Counters.increment_source_changed_at_unix_ts(counter, 17)

    assert Counters.get_inserts(counter) == {:ok, 5}
    assert Counters.get_bq_inserts(counter) == {:ok, 7}
    assert Counters.log_count(counter) == 4
    assert Counters.log_count(%Source{token: counter}) == 4
    assert Counters.get_inserts_since_boot(counter) == 11
    assert Counters.get_total_cluster_inserts(counter) == 13
    assert Counters.get_source_changed_at_unix_ms(counter) == 17
  end

  test "increments a source counter concurrently without losing updates" do
    counter = :sources_counters_concurrency_test
    workers = 16
    increments_per_worker = 1_000
    on_exit(fn -> Counters.delete(counter) end)

    assert {:ok, ^counter} = Counters.create(counter)

    1..workers
    |> Task.async_stream(
      fn _worker ->
        for _increment <- 1..increments_per_worker do
          assert {:ok, ^counter} = Counters.increment(counter)
        end

        :ok
      end,
      max_concurrency: workers,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    assert Counters.get_inserts(counter) == {:ok, workers * increments_per_worker}
  end

  test "an increment on a detached ref reaches the current source counter" do
    counter = :sources_counters_reset_race_test
    on_exit(fn -> Counters.delete(counter) end)

    assert {:ok, ^counter} = Counters.create(counter)
    [{^counter, old_ref}] = :ets.lookup(:table_counters, counter)
    assert :atomics.info(old_ref).size == 6

    # A reset can happen after increment/2 looks up the ref but before it adds.
    assert {:ok, ^counter} = Counters.delete(counter)
    assert {:ok, ^counter} = Counters.create(counter)
    assert {:ok, ^counter} = Counters.add_to_ref(counter, 1, 7, old_ref)
    assert Counters.get_inserts(counter) == {:ok, 7}

    [{^counter, replaced_ref}] = :ets.lookup(:table_counters, counter)
    assert {:ok, ^counter} = Counters.delete(counter)
    assert {:ok, ^counter} = Counters.add_to_ref(counter, 1, 3, replaced_ref)
    assert Counters.get_inserts(counter) == {:ok, 3}
  end

  test "deleting a source counter restores zero-valued reads and permits recreation" do
    counter = :sources_counters_delete_test
    on_exit(fn -> Counters.delete(counter) end)

    assert {:ok, ^counter} = Counters.increment(counter, 3)
    assert {:ok, 3} = Counters.get_inserts(counter)
    assert {:ok, ^counter} = Counters.delete(counter)

    assert Counters.get_inserts(counter) == {:ok, 0}
    assert Counters.get_bq_inserts(counter) == {:ok, 0}
    assert Counters.log_count(counter) == 0
    assert Counters.get_inserts_since_boot(counter) == 0
    assert Counters.get_total_cluster_inserts(counter) == 0
    assert Counters.get_source_changed_at_unix_ms(counter) == 0

    assert {:ok, ^counter} = Counters.increment(counter)
    assert Counters.get_inserts(counter) == {:ok, 1}
  end
end

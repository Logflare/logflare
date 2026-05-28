defmodule Logflare.UserMetrics.MetricStoreTest do
  use ExUnit.Case, async: false

  alias Telemetry.Metrics
  alias Logflare.UserMetrics.MetricStore

  @name :um_metric_store_test

  setup do
    config = %{
      export_period: 60_000,
      metrics: [],
      name: @name,
      pull_mode: true
    }

    {:ok, config: config}
  end

  describe "recording metrics" do
    setup %{config: config} do
      {:ok, store: start_supervised!({MetricStore, config})}
    end

    test "records counter metrics" do
      metric = Metrics.counter("um.test.counter")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 1, tags)

      assert %{{:counter, "um.test.counter"} => %{^tags => 2}} = MetricStore.get_metrics(@name)
    end

    test "records sum metrics" do
      metric = Metrics.sum("um.test.sum")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      assert %{{:sum, "um.test.sum"} => %{^tags => 3}} = MetricStore.get_metrics(@name)
    end

    test "ignores sum write when value is non-numeric" do
      metric = Metrics.sum("um.test.sum.nan")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, :not_a_number, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      assert %{{:sum, "um.test.sum.nan"} => %{^tags => 3}} = MetricStore.get_metrics(@name)
    end

    test "records last_value metrics, latest write wins" do
      metric = Metrics.last_value("um.test.last_value")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      assert %{{:last_value, "um.test.last_value"} => %{^tags => 2}} =
               MetricStore.get_metrics(@name)
    end

    test "records distribution metrics into correct buckets" do
      metric = Metrics.distribution("um.test.dist", reporter_options: [buckets: [2, 4]])
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 2, tags)
      MetricStore.write_metric(@name, metric, 3, tags)
      MetricStore.write_metric(@name, metric, 5, tags)
      MetricStore.write_metric(@name, metric, 5, tags)

      assert %{
               {:distribution, "um.test.dist"} => %{
                 ^tags => %{0 => {1, 2}, 1 => {1, 3}, 2 => {2, 10}}
               }
             } = MetricStore.get_metrics(@name)
    end

    test "ignores distribution write when value is non-numeric" do
      metric = Metrics.distribution("um.test.dist.nan", reporter_options: [buckets: [2, 4]])
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 2, tags)
      MetricStore.write_metric(@name, metric, :not_a_number, tags)
      MetricStore.write_metric(@name, metric, 3, tags)

      assert %{
               {:distribution, "um.test.dist.nan"} => %{
                 ^tags => %{0 => {1, 2}, 1 => {1, 3}}
               }
             } = MetricStore.get_metrics(@name)
    end

    test "accumulates different tag sets independently" do
      metric = Metrics.sum("um.test.sum.tags")
      tags1 = %{test: "a"}
      tags2 = %{test: "b"}

      MetricStore.write_metric(@name, metric, 1, tags1)
      MetricStore.write_metric(@name, metric, 2, tags2)
      MetricStore.write_metric(@name, metric, 2, tags1)

      assert %{
               {:sum, "um.test.sum.tags"} => %{^tags1 => 3, ^tags2 => 2}
             } = MetricStore.get_metrics(@name)
    end
  end

  describe "memory trimming" do
    # Use max_table_memory: 1 to guarantee the limit is always exceeded,
    # making trim behaviour deterministic regardless of ETS baseline size.
    defp induce_rotation do
      send(@name, :rotate_and_trim)
      Process.sleep(50)
    end

    test "trims oldest generation when memory limit is exceeded" do
      metric = Metrics.sum("um.trim.oldest")

      config = %{
        export_period: 60_000,
        metrics: [metric],
        name: @name,
        pull_mode: true,
        max_table_memory: 1
      }

      start_supervised!({MetricStore, config})

      MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})
      refute MetricStore.get_metrics(@name, 0) == %{}

      # Rotation makes gen 0 an "older" generation; trim clears it
      induce_rotation()

      assert MetricStore.get_metrics(@name, 0) == %{}
    end

    test "current generation is never trimmed even when over the limit" do
      metric = Metrics.sum("um.trim.current")

      config = %{
        export_period: 60_000,
        metrics: [metric],
        name: @name,
        pull_mode: true,
        max_table_memory: 1
      }

      start_supervised!({MetricStore, config})

      MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})

      # No rotation — gen 0 is still the current gen and must not be trimmed
      refute MetricStore.get_metrics(@name, 0) == %{}
    end

    test "successive rotations each trim the generation that aged out" do
      metric = Metrics.sum("um.trim.successive")

      config = %{
        export_period: 60_000,
        metrics: [metric],
        name: @name,
        pull_mode: true,
        max_table_memory: 1
      }

      start_supervised!({MetricStore, config})

      MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})
      induce_rotation()
      assert MetricStore.get_metrics(@name, 0) == %{}

      MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})
      induce_rotation()
      assert MetricStore.get_metrics(@name, 1) == %{}

      # Write to gen 2 (current) — it must survive the trim
      MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})
      refute MetricStore.get_metrics(@name, 2) == %{}
    end
  end

  describe "pull/1 (unbounded)" do
    setup %{config: config} do
      metric = Metrics.sum("pull.test.sum")
      config = %{config | metrics: [metric]}
      {:ok, store: start_supervised!({MetricStore, config}), metric: metric}
    end

    test "returns populated metrics and clears the generation" do
      metric = Metrics.sum("pull.test.sum")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 5, tags)
      MetricStore.write_metric(@name, metric, 3, tags)

      assert {:ok, metrics} = MetricStore.pull(@name)
      assert length(metrics) == 1
      assert MetricStore.get_metrics(@name, 0) == %{}
    end

    test "returns {:ok, []} on empty store" do
      assert {:ok, []} = MetricStore.pull(@name)
    end

    test "second pull returns empty after first drained all metrics" do
      metric = Metrics.sum("pull.test.sum")

      MetricStore.write_metric(@name, metric, 10, %{test: "value"})

      assert {:ok, first} = MetricStore.pull(@name)
      assert length(first) == 1
      assert {:ok, []} = MetricStore.pull(@name)
    end

    test "writes after rotation land in next generation and appear in subsequent pull" do
      metric = Metrics.sum("pull.test.sum")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)

      assert {:ok, first} = MetricStore.pull(@name)
      assert length(first) == 1

      MetricStore.write_metric(@name, metric, 2, tags)

      assert {:ok, second} = MetricStore.pull(@name)
      assert length(second) == 1
    end

    test "metrics written between two pulls are not lost or double-emitted" do
      metric = Metrics.sum("pull.test.sum")

      MetricStore.write_metric(@name, metric, 1, %{test: "a"})

      {:ok, first} = MetricStore.pull(@name)
      assert length(first) == 1

      MetricStore.write_metric(@name, metric, 2, %{test: "b"})

      {:ok, second} = MetricStore.pull(@name)
      assert length(second) == 1

      {:ok, third} = MetricStore.pull(@name)
      assert third == []
    end
  end

  describe "pull/2 with limit" do
    setup %{config: config} do
      metric = Metrics.sum("pull.limit.sum")
      config = %{config | metrics: [metric]}
      {:ok, store: start_supervised!({MetricStore, config}), metric: metric}
    end

    test "limits the number of ETS rows returned per call" do
      metric = Metrics.sum("pull.limit.sum")

      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})
      MetricStore.write_metric(@name, metric, 3, %{id: "c"})

      assert {:ok, first} = MetricStore.pull(@name, 1)
      assert length(first) == 1

      # Two rows remain in the partially-drained generation
      assert {:ok, second} = MetricStore.pull(@name, 2)
      assert length(second) == 1

      assert {:ok, []} = MetricStore.pull(@name, 10)
    end

    test "resumes the same generation on successive bounded pulls" do
      metric = Metrics.sum("pull.limit.sum")

      MetricStore.write_metric(@name, metric, 10, %{id: "x"})
      MetricStore.write_metric(@name, metric, 20, %{id: "y"})

      assert {:ok, _batch1} = MetricStore.pull(@name, 1)
      assert {:ok, _batch2} = MetricStore.pull(@name, 1)

      assert {:ok, []} = MetricStore.pull(@name, 1)
    end

    test "limit larger than available rows drains completely" do
      metric = Metrics.sum("pull.limit.sum")

      MetricStore.write_metric(@name, metric, 5, %{id: "only"})

      assert {:ok, metrics} = MetricStore.pull(@name, 1000)
      assert length(metrics) == 1
      assert {:ok, []} = MetricStore.pull(@name, 1000)
    end

    test "pull(:infinity) drains the whole generation" do
      metric = Metrics.sum("pull.limit.sum")

      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})

      assert {:ok, metrics} = MetricStore.pull(@name, :infinity)
      assert length(metrics) == 1

      assert {:ok, []} = MetricStore.pull(@name, :infinity)
    end
  end

  test "max_table_memory enforced via rotation timer in pull mode", %{config: config} do
    metric = Metrics.sum("um.trim.pull.value")

    config = config |> Map.put(:metrics, [metric]) |> Map.put(:max_table_memory, 1)
    start_supervised!({MetricStore, config})

    MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})
    refute MetricStore.get_metrics(@name, 0) == %{}

    send(@name, :rotate_and_trim)
    Process.sleep(50)
    MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})

    send(@name, :rotate_and_trim)
    Process.sleep(50)

    assert MetricStore.get_metrics(@name, 0) == %{}
    assert MetricStore.get_metrics(@name, 1) == %{}
  end
end

defmodule Logflare.TelemetrySystemExportTest do
  use ExUnit.Case, async: false
  use Mimic

  import Logflare.TelemetryExportTestHelpers

  alias Logflare.SavedSearches.Cache, as: SavedSearchesCache
  alias Logflare.SystemMetrics.Observer
  alias Logflare.Telemetry
  alias Logflare.TestUtils
  alias Logflare.Utils

  setup :set_mimic_private

  test "exports Observer uptime in milliseconds", %{test: name} do
    [metric] =
      Enum.filter(
        Telemetry.metrics(),
        &(&1.name == [:logflare, :system, :observer, :metrics, :uptime])
      )

    start_exporter([metric], name)

    {before_uptime, _} = :erlang.statistics(:wall_clock)
    Observer.dispatch_stats()
    assert [exported] = export_metrics(name)
    {after_uptime, _} = :erlang.statistics(:wall_clock)

    assert exported.name == "logflare.system.observer.metrics.uptime"
    assert exported.unit == "ms"
    assert {:gauge, %{data_points: [%{value: {:as_double, uptime}}]}} = exported.data
    assert uptime >= before_uptime
    assert uptime <= after_uptime
  end

  test "exports Cachex supervisor heap words as megabytes", %{test: name} do
    [metric] =
      Enum.filter(
        Telemetry.metrics(),
        &(&1.name == [:cachex, :saved_searches, :total_heap_size])
      )

    start_exporter([metric], name)

    {:total_heap_size, words} =
      SavedSearchesCache |> Process.whereis() |> Process.info(:total_heap_size)

    expected_megabytes = words * :erlang.system_info(:wordsize) * 1.0e-6
    Telemetry.cachex_metrics()

    assert [exported] = export_metrics(name)
    assert exported.name == "cachex.saved_searches.total_heap_size"
    assert exported.unit == "MBy"
    assert {:gauge, %{data_points: [%{value: {:as_double, heap}}]}} = exported.data
    assert heap == expected_megabytes
  end

  test "exports Cachex purge and stats call counts", %{test: name} do
    metrics =
      Enum.filter(Telemetry.metrics(), fn metric ->
        metric.name in [[:cachex, :saved_searches, :purge], [:cachex, :saved_searches, :stats]]
      end)

    assert length(metrics) == 2
    start_exporter(metrics, name)

    for _ <- 1..3 do
      assert {:ok, _count} = Cachex.purge(SavedSearchesCache)
      assert {:ok, _stats} = Cachex.stats(SavedSearchesCache)
    end

    assert {:ok, baseline} = Cachex.stats(SavedSearchesCache, notify: false)
    assert baseline.calls.purge >= 3
    assert baseline.calls.stats >= 3

    Telemetry.cachex_metrics()
    exported = Map.new(export_metrics(name), &{&1.name, &1})
    assert map_size(exported) == 2

    for stat <- [:purge, :stats] do
      metric = Map.fetch!(exported, "cachex.saved_searches.#{stat}")
      assert {:gauge, %{data_points: [%{value: {:as_double, count}}]}} = metric.data
      assert count >= Map.fetch!(baseline.calls, stat)
    end
  end

  test "exports ETS snapshots in bytes while events remain in words", %{test: name} do
    individual_event = [:logflare, :system, :top_ets_tables, :individual]
    grouped_event = [:logflare, :system, :top_ets_tables, :grouped]
    individual_name = "logflare.system.top_ets_tables.individual.memory"
    grouped_name = "logflare.system.top_ets_tables.grouped.memory"
    grouped_table_name = ":telemetry_export_ets_bytes"
    table_names = [:telemetry_export_ets_bytes1, :telemetry_export_ets_bytes2]

    metrics =
      Enum.filter(Telemetry.metrics(), &(&1.event_name in [individual_event, grouped_event]))

    assert length(metrics) == 2
    start_exporter(metrics, name)
    TestUtils.attach_forwarder(individual_event)
    TestUtils.attach_forwarder(grouped_event)

    tables =
      for table_name <- table_names do
        table = :ets.new(table_name, [:named_table])
        assert :ets.insert(table, {:payload, List.duplicate(0, 100)})
        {table, :ets.info(table, :memory)}
      end

    stub(Utils, :ets_info, fn table ->
      if :ets.info(table, :name) in table_names, do: :ets.info(table), else: :undefined
    end)

    grouped_words = Enum.sum(Enum.map(tables, &elem(&1, 1)))
    wordsize = :erlang.system_info(:wordsize)

    for _ <- 1..2 do
      for _ <- 1..2 do
        Telemetry.ets_table_metrics()

        for {table, words} <- tables do
          assert_receive {:telemetry_event, ^individual_event, %{memory: ^words}, %{name: ^table}}
        end

        assert_receive {:telemetry_event, ^grouped_event, %{memory: ^grouped_words},
                        %{name: ^grouped_table_name}}
      end

      exported = Map.new(export_metrics(name), &{&1.name, &1})
      assert map_size(exported) == 2

      for {metric_name, expected} <- [
            {individual_name,
             Enum.map(tables, fn {table, words} -> {Atom.to_string(table), words} end)},
            {grouped_name, [{grouped_table_name, grouped_words}]}
          ] do
        assert %{unit: "By", data: {:gauge, %{data_points: points}}} = exported[metric_name]

        for {table_name, words} <- expected do
          assert %{value: {:as_double, memory}} =
                   Enum.find(points, fn point ->
                     point_attributes(point) == %{"name" => table_name}
                   end)

          assert memory == words * wordsize
        end
      end
    end
  end
end

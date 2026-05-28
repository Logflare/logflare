defmodule Logflare.UserMetrics.TelemetryHandlersTest do
  use ExUnit.Case, async: false

  alias Telemetry.Metrics
  alias Logflare.UserMetrics.MetricStore
  alias Logflare.UserMetrics.TelemetryHandlers
  alias Logflare.Backends.UserMonitoring

  @store_name :um_th_test_store

  # Strip all tags by default so routing tests only verify values, not tags.
  defp no_tags(_metric, _meta), do: %{}

  defp start_store(metrics) do
    start_supervised!({MetricStore, %{
      metrics: metrics,
      name: @store_name,
      export_period: 60_000,
      pull_mode: true
    }})
  end

  defp start_handlers(metrics, opts \\ []) do
    extract_tags = Keyword.get(opts, :extract_tags, &no_tags/2)

    start_supervised!({TelemetryHandlers, %{
      metrics: metrics,
      store_name: @store_name,
      extract_tags: extract_tags
    }})
  end

  describe "routing telemetry events to MetricStore" do
    test "accumulates sum metric across multiple events" do
      metric = Metrics.sum("um.th.sum",
        event_name: [:um, :th, :sum],
        measurement: :value,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      :telemetry.execute([:um, :th, :sum], %{value: 10}, %{})
      :telemetry.execute([:um, :th, :sum], %{value: 5}, %{})

      assert %{{:sum, "um.th.sum"} => %{%{} => 15}} = MetricStore.get_metrics(@store_name)
    end

    test "increments counter across multiple events" do
      metric = Metrics.counter("um.th.counter",
        event_name: [:um, :th, :counter],
        measurement: :n,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      :telemetry.execute([:um, :th, :counter], %{n: 1}, %{})
      :telemetry.execute([:um, :th, :counter], %{n: 1}, %{})
      :telemetry.execute([:um, :th, :counter], %{n: 1}, %{})

      assert %{{:counter, "um.th.counter"} => %{%{} => 3}} = MetricStore.get_metrics(@store_name)
    end

    test "applies 1-arity measurement function" do
      metric = Metrics.sum("um.th.measure1",
        event_name: [:um, :th, :measure1],
        measurement: fn m -> m.raw * 2 end,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      :telemetry.execute([:um, :th, :measure1], %{raw: 21}, %{})

      assert %{{:sum, "um.th.measure1"} => %{%{} => 42}} = MetricStore.get_metrics(@store_name)
    end

    test "applies 2-arity measurement function receiving both measurements and metadata" do
      metric = Metrics.sum("um.th.measure2",
        event_name: [:um, :th, :measure2],
        measurement: fn m, meta -> m.value * meta.multiplier end,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      :telemetry.execute([:um, :th, :measure2], %{value: 7}, %{multiplier: 6})

      assert %{{:sum, "um.th.measure2"} => %{%{} => 42}} = MetricStore.get_metrics(@store_name)
    end

    test "handler stays attached after a non-numeric sum value (does not crash/detach)" do
      metric = Metrics.sum("um.th.badval",
        event_name: [:um, :th, :badval],
        measurement: :value,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      :telemetry.execute([:um, :th, :badval], %{value: :not_a_number}, %{})

      assert [_] = :telemetry.list_handlers([:um, :th, :badval])
    end
  end

  describe "extract_tags" do
    test "custom extract_tags function determines which tags are stored in ETS" do
      metric = Metrics.sum("um.th.custom.tags",
        event_name: [:um, :th, :custom, :tags],
        measurement: :value,
        tags: []
      )

      extract_tags = fn _metric, meta -> %{region: meta.region} end

      start_store([metric])
      start_handlers([metric], extract_tags: extract_tags)

      :telemetry.execute([:um, :th, :custom, :tags], %{value: 5}, %{region: "us-east"})
      :telemetry.execute([:um, :th, :custom, :tags], %{value: 3}, %{region: "eu-west"})
      :telemetry.execute([:um, :th, :custom, :tags], %{value: 2}, %{region: "us-east"})

      assert %{
        {:sum, "um.th.custom.tags"} => %{
          %{region: "us-east"} => 7,
          %{region: "eu-west"} => 3
        }
      } = MetricStore.get_metrics(@store_name)
    end

    test "separate tag sets accumulate independently" do
      metric = Metrics.sum("um.th.multikey",
        event_name: [:um, :th, :multikey],
        measurement: :value,
        tags: []
      )

      extract_tags = fn _metric, meta -> %{id: meta.id} end

      start_store([metric])
      start_handlers([metric], extract_tags: extract_tags)

      :telemetry.execute([:um, :th, :multikey], %{value: 10}, %{id: "a"})
      :telemetry.execute([:um, :th, :multikey], %{value: 20}, %{id: "b"})
      :telemetry.execute([:um, :th, :multikey], %{value: 5}, %{id: "a"})

      assert %{
        {:sum, "um.th.multikey"} => %{
          %{id: "a"} => 15,
          %{id: "b"} => 20
        }
      } = MetricStore.get_metrics(@store_name)
    end
  end

  describe "UserMonitoring.extract_tags/2" do
    test "includes string-keyed scalar values" do
      result = UserMonitoring.extract_tags(:ignored, %{
        "user_id" => 42,
        "source_uuid" => "abc-123"
      })

      assert result == %{"user_id" => 42, "source_uuid" => "abc-123"}
    end

    test "excludes atom keys, nested maps, lists, and nil values" do
      result = UserMonitoring.extract_tags(:ignored, %{
        "kept" => "yes",
        :atom_key => "excluded",
        "nested" => %{"a" => 1},
        "list_val" => [1, 2, 3],
        "nil_val" => nil
      })

      assert result == %{"kept" => "yes"}
    end

    test "returns empty map for empty metadata" do
      assert UserMonitoring.extract_tags(:ignored, %{}) == %{}
    end
  end

  describe "keep filtering" do
    test "does not write when keep/1 returns false" do
      metric = Metrics.counter("um.th.keep",
        event_name: [:um, :th, :keep],
        measurement: :n,
        keep: fn meta -> meta.keep == true end,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      :telemetry.execute([:um, :th, :keep], %{n: 1}, %{keep: true})
      :telemetry.execute([:um, :th, :keep], %{n: 1}, %{keep: false})

      assert %{{:counter, "um.th.keep"} => %{%{} => 1}} = MetricStore.get_metrics(@store_name)
    end

    test "nothing is written when all events are filtered" do
      metric = Metrics.counter("um.th.drop.all",
        event_name: [:um, :th, :drop, :all],
        measurement: :n,
        keep: fn _meta -> false end,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      :telemetry.execute([:um, :th, :drop, :all], %{n: 1}, %{})
      :telemetry.execute([:um, :th, :drop, :all], %{n: 1}, %{})

      assert MetricStore.get_metrics(@store_name) == %{}
    end
  end

  describe "shutdown" do
    test "detaches telemetry handlers when GenServer stops" do
      metric = Metrics.sum("um.th.detach",
        event_name: [:um, :th, :detach],
        measurement: :value,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      assert [_] = :telemetry.list_handlers([:um, :th, :detach])

      stop_supervised!(TelemetryHandlers)

      assert [] = :telemetry.list_handlers([:um, :th, :detach])
    end

    test "no writes to store after handler is detached" do
      metric = Metrics.sum("um.th.post.stop",
        event_name: [:um, :th, :post, :stop],
        measurement: :value,
        tags: []
      )
      start_store([metric])
      start_handlers([metric])

      stop_supervised!(TelemetryHandlers)

      :telemetry.execute([:um, :th, :post, :stop], %{value: 99}, %{})

      assert MetricStore.get_metrics(@store_name) == %{}
    end
  end
end

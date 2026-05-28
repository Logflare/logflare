defmodule Logflare.UserMetrics.PullProducerTest do
  use ExUnit.Case, async: false

  alias Telemetry.Metrics
  alias Logflare.UserMetrics.MetricStore
  alias Logflare.UserMetrics.PullProducer

  @store_name :um_pull_producer_test

  defmodule TestConsumer do
    use GenStage

    def start_link({producer, test_pid}) do
      GenStage.start_link(__MODULE__, {producer, test_pid})
    end

    @impl true
    def init({producer, test_pid}) do
      {:consumer, %{test_pid: test_pid}, subscribe_to: [{producer, max_demand: 10, min_demand: 0}]}
    end

    @impl true
    def handle_events(events, _from, state) do
      send(state.test_pid, {:events, events})
      {:noreply, [], state}
    end
  end

  setup do
    metric = Metrics.sum("pp.test.sum")

    store =
      start_supervised!({MetricStore, %{
        metrics: [metric],
        name: @store_name,
        export_period: 60_000,
        pull_mode: true
      }})

    {:ok, metric: metric, store: store}
  end

  defp start_producer(pull_interval \\ 100) do
    start_supervised!({PullProducer, metric_store_name: @store_name, pull_interval: pull_interval})
  end

  test "emits events when store has metrics", %{metric: metric} do
    MetricStore.write_metric(@store_name, metric, 10, %{id: "a"})
    MetricStore.write_metric(@store_name, metric, 20, %{id: "b"})

    producer = start_producer()
    start_supervised!({TestConsumer, {producer, self()}})

    assert_receive {:events, events}, 1_000
    assert length(events) >= 1
  end

  test "receives events on next tick when store is initially empty", %{metric: metric} do
    producer = start_producer(100)
    start_supervised!({TestConsumer, {producer, self()}})

    refute_receive {:events, _}, 50

    MetricStore.write_metric(@store_name, metric, 5, %{id: "x"})

    assert_receive {:events, events}, 1_000
    assert length(events) >= 1
  end

  test "initial producer state has zero pending demand and no tick scheduled" do
    producer = start_producer()

    %{state: inner} = :sys.get_state(producer)

    assert inner.pending_demand == 0
    assert inner.tick_ref == nil
  end

  test "schedules tick and emits once store is populated after demand was pending", %{
    metric: metric
  } do
    producer = start_producer(50)
    start_supervised!({TestConsumer, {producer, self()}})

    refute_receive {:events, _}, 30

    MetricStore.write_metric(@store_name, metric, 42, %{id: "z"})

    assert_receive {:events, events}, 1_000
    assert length(events) == 1
    assert {:ok, []} = MetricStore.pull(@store_name)
  end
end

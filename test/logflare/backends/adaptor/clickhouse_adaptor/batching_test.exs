defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.BatchingTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline

  @backend_id 42
  @pipeline_key {:consolidated, @backend_id, self()}
  @startup_key {:consolidated, @backend_id, nil}
  @log_key {:fresh, :log, 20_594}
  @metric_key {:fresh, :metric, 20_594}
  @other_day_key {:fresh, :log, 20_593}
  @stale_key {:stale, :log, 20_594}

  defp state(pipeline_count, max_pipelines \\ 8) do
    %{
      pipeline_count: pipeline_count,
      max_pipelines: max_pipelines,
      last_count_decrease: nil
    }
  end

  defp resolve(state, lens, startup_batch_key_counts \\ %{}) do
    ClickHouseAdaptor.resolve_pipeline_count(state, lens, startup_batch_key_counts)
  end

  test "keeps a single pipeline when only one batch-sized queue is pending" do
    lens = [{@pipeline_key, Pipeline.max_batch_size()}]

    assert resolve(state(1), lens) == 1
  end

  test "does not scale at the old per-queue threshold" do
    lens = [{@pipeline_key, 30_000}]

    assert resolve(state(1), lens) == 1
  end

  test "scales when startup contains a complete batch for one exact batch key" do
    batch_size = Pipeline.max_batch_size()
    lens = [{@pipeline_key, batch_size}, {@startup_key, batch_size}]

    assert resolve(state(1), lens, %{@log_key => batch_size}) == 2
  end

  test "does not scale when one batch worth of startup rows is split across batch keys" do
    batch_size = Pipeline.max_batch_size()
    half_batch = div(batch_size, 2)
    lens = [{@pipeline_key, batch_size}, {@startup_key, batch_size}]

    assert resolve(state(1), lens, %{@log_key => half_batch, @metric_key => half_batch}) == 1
  end

  test "does not combine freshness or day buckets when deciding a batch is complete" do
    batch_size = Pipeline.max_batch_size()
    partial = div(batch_size, 3)
    lens = [{@startup_key, partial * 3}]

    counts = %{@log_key => partial, @other_day_key => partial, @stale_key => partial}
    assert resolve(state(1), lens, counts) == 1
  end

  test "does not create a cold pipeline for a small startup burst" do
    count = Pipeline.max_batch_size() - 1
    lens = [{@startup_key, count}]

    assert resolve(state(1), lens, %{@log_key => count}) == 1
  end

  test "at 100k pending events it keeps the existing pipeline and in-flight budget" do
    lens = [{@pipeline_key, Pipeline.max_batch_size()}, {@startup_key, 40_000}]

    assert resolve(state(1), lens, %{@log_key => 40_000}) == 1
    assert Pipeline.max_in_flight() == 2_160_000
  end

  test "two complete startup batches fit in one new pipeline's in-flight budget" do
    batch_size = Pipeline.max_batch_size()
    lens = [{@startup_key, batch_size * 2}]

    counts = %{@log_key => batch_size, @metric_key => batch_size}
    assert resolve(state(1), lens, counts) == 2
  end

  test "respects the dynamic pipeline maximum when startup has multiple complete batches" do
    pending = Pipeline.max_in_flight() * 2
    lens = [{@startup_key, pending}]

    assert resolve(state(1, 2), lens, %{@log_key => pending}) == 2
  end
end

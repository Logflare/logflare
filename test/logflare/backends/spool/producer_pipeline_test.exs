defmodule Logflare.Backends.Spool.ProducerPipelineTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.ProducerPipeline

  @max_spool_file_size 32 * 1024 * 1024
  @early_flush_file_size 12 * 1024 * 1024

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    :ok
  end

  defp message(size), do: %{data: {Ecto.UUID.generate(), :fake_tid, size}}

  defp not_throttled! do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 1.0,
      spool_max_ets_percent: 1.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)
  end

  defp throttled! do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)
  end

  describe "spool_batch_size_splitter/0 when not throttled" do
    test "continues accumulating under the normal 32MB budget" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, {_count, remaining}} = reducer.(message(10 * 1024 * 1024), initial)
      assert remaining == @max_spool_file_size - 10 * 1024 * 1024
    end

    test "emits once accumulated size would exceed the normal 32MB budget" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, acc} = reducer.(message(20 * 1024 * 1024), initial)
      assert {:emit, {_count, :pending}} = reducer.(message(15 * 1024 * 1024), acc)
    end
  end

  describe "spool_batch_size_splitter/0 when throttled" do
    test "emits at the smaller early-flush budget instead of the normal 32MB one" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      # 15MB alone wouldn't trip the normal 32MB budget, but does trip the 12MB one.
      assert {:emit, {_count, :pending}} = reducer.(message(15 * 1024 * 1024), initial)
    end

    test "continuing under the early-flush budget still tracks the correct remaining bytes" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, {_count, remaining}} = reducer.(message(5 * 1024 * 1024), initial)
      assert remaining == @early_flush_file_size - 5 * 1024 * 1024
    end

    test "the budget decided for a batch stays locked in even if throttling clears mid-batch" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      # First message decides the budget for this whole batch: 12MB (throttled).
      assert {:cont, acc} = reducer.(message(5 * 1024 * 1024), initial)

      # Flip to "not throttled" and wait for MemoryMonitor to genuinely refresh —
      # if the reducer re-checked per message, this would flip its behavior.
      Application.put_env(:logflare, :spool,
        spool_memory_limit_percent: 1.0,
        spool_max_ets_percent: 1.0
      )

      Process.sleep(1_100)
      assert MemoryMonitor.throttled?() == false

      # 5MB + 8MB = 13MB, over the locked-in 12MB budget (would NOT emit under
      # a freshly-rechecked 32MB budget) — proves the decision wasn't re-evaluated.
      assert {:emit, {_count, :pending}} = reducer.(message(8 * 1024 * 1024), acc)
    end
  end
end

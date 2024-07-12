defmodule Logflare.Backends.IngestEventQueueTest do
  use Logflare.DataCase

  alias Logflare.PubSubRates
  alias Logflare.Backends.IngestEventQueue.BroadcastWorker
  alias Logflare.Backends.IngestEventQueue.DemandWorker
  alias Logflare.Backends.IngestEventQueue.QueueJanitor
  alias Logflare.Backends.IngestEventQueue.MapperJanitor
  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue

  test "get_table_size/1 returns nil for non-existing tables" do
    assert nil == IngestEventQueue.get_table_size({1, 2})
  end

  test "upsert_tid/1 will recreate a new ets table if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    assert {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
    :ets.delete(tid)
    assert {:ok, new_tid} = IngestEventQueue.upsert_tid({source, backend})
    assert new_tid != tid
  end

  test "get_tid/1 will return nil if tid is stale and deleted" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    assert {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
    assert ^tid = IngestEventQueue.get_tid({source, backend})
    :ets.delete(tid)
    assert nil == IngestEventQueue.get_tid({source, backend})
  end

  describe "with a queue" do
    setup do
      user = insert(:user)
      sb = {insert(:source, user: user), insert(:backend, user: user)}
      IngestEventQueue.upsert_tid(sb)
      [source_backend: sb]
    end

    test "object lifecycle", %{source_backend: sb} do
      le = build(:log_event)
      # insert to table
      assert :ok = IngestEventQueue.add_to_table(sb, [le])
      assert IngestEventQueue.get_table_size(sb) == 1
      # can take pending items
      assert {:ok, [_]} = IngestEventQueue.take_pending(sb, 5)
      assert IngestEventQueue.count_pending(sb) == 1
      # set to ingested
      assert {:ok, 1} = IngestEventQueue.mark_ingested(sb, [le])
      assert IngestEventQueue.count_pending(sb) == 0
      # truncate to n items
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 1)
      assert IngestEventQueue.get_table_size(sb) == 1
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 0)
      assert IngestEventQueue.count_pending(sb) == 0
    end

    test "drop n items from a queue", %{source_backend: sb} do
      batch = for _ <- 1..500, do: build(:log_event)
      assert :ok = IngestEventQueue.add_to_table(sb, batch)
      assert :ok = IngestEventQueue.drop(sb, :all, 2)
      assert IngestEventQueue.get_table_size(sb) == 498
      assert :ok = IngestEventQueue.drop(sb, :ingested, 2)
      assert IngestEventQueue.get_table_size(sb) == 498
    end

    test "truncate all events in a queue", %{source_backend: sb} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sb, batch)
      assert :ok = IngestEventQueue.truncate(sb, :all, 50)
      assert IngestEventQueue.get_table_size(sb) == 50
      assert :ok = IngestEventQueue.truncate(sb, :all, 0)
      assert IngestEventQueue.get_table_size(sb) == 0
    end

    test "truncate ingested events in a queue", %{source_backend: sb} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sb, batch)
      assert {:ok, _} = IngestEventQueue.mark_ingested(sb, batch)
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 50)
      assert IngestEventQueue.get_table_size(sb) == 50
      assert IngestEventQueue.count_pending(sb) == 0
      assert :ok = IngestEventQueue.truncate(sb, :ingested, 0)
      assert IngestEventQueue.get_table_size(sb) == 0
    end

    test "truncate pending events in a queue", %{source_backend: sb} do
      batch =
        for _ <- 1..500 do
          build(:log_event)
        end

      # add as pending
      assert :ok = IngestEventQueue.add_to_table(sb, batch)
      assert :ok = IngestEventQueue.truncate(sb, :pending, 50)
      assert IngestEventQueue.count_pending(sb) == 50
      assert :ok = IngestEventQueue.truncate(sb, :pending, 0)
      assert IngestEventQueue.count_pending(sb) == 0
    end
  end

  test "BroadcastWorker broadcasts every n seconds" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    IngestEventQueue.upsert_tid({source, nil})
    IngestEventQueue.upsert_tid({source, backend})
    :timer.sleep(100)
    start_supervised!({BroadcastWorker, interval: 100})

    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, [le])
    IngestEventQueue.add_to_table({source, nil}, [le])
    :timer.sleep(300)
    assert Backends.local_pending_buffer_len(source) == 1
    assert Backends.local_pending_buffer_len(source, backend) == 1
    assert PubSubRates.Cache.get_cluster_buffers(source.id, backend.id) == 1
    assert PubSubRates.Cache.get_cluster_buffers(source.id, nil) == 1
  end

  test "DemandWorker with backend" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, [le])
    start_supervised!({DemandWorker, source: source, backend: backend})
    :timer.sleep(100)
    assert {:ok, [_]} = DemandWorker.fetch({source, backend}, 5)
    assert IngestEventQueue.get_table_size({source, backend}) == 1
    assert IngestEventQueue.count_pending({source, backend}) == 0
    assert Backends.local_pending_buffer_len(source, backend) == 0
  end

  test "QueueJanitor cleans up :ingested events" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    le = build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, [le])
    IngestEventQueue.mark_ingested({source, backend}, [le])
    assert IngestEventQueue.get_table_size({source, backend}) == 1

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, remainder: 0}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size({source, backend}) == 0
    assert IngestEventQueue.count_pending({source, backend}) == 0
  end

  test "QueueJanitor purges if exceeds max" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    batch = for _ <- 1..105, do: build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, batch)
    assert IngestEventQueue.get_table_size({source, backend}) == 105

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, max: 100, purge_ratio: 1.0}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size({source, backend}) == 0
  end

  test "QueueJanitor purges based on purge ratio" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    batch = for _ <- 1..100, do: build(:log_event, source: source)
    IngestEventQueue.add_to_table({source, backend}, batch)
    assert IngestEventQueue.get_table_size({source, backend}) == 100

    start_supervised!(
      {QueueJanitor, source: source, backend: backend, interval: 50, max: 90, purge_ratio: 0.5}
    )

    :timer.sleep(550)
    assert IngestEventQueue.get_table_size({source, backend}) == 50
  end

  test "MapperJanitor cleans up stale tids" do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)
    IngestEventQueue.upsert_tid({source, backend})
    tid = IngestEventQueue.get_tid({source, backend})
    :ets.delete(tid)
    start_supervised!({MapperJanitor, interval: 100})
    :timer.sleep(500)
    assert IngestEventQueue.get_table_size({source, backend}) == nil
    assert :ets.info(:ingest_event_queue_mapping, :size) == 0
  end

  describe "IngestEventQueue" do
    @tag :benchmark
    @tag timeout: :infinity
    @tag :skip

    # Benchmark results
    # drop_with_chunking is ~15.65x slower (100 chunk)
    # select and drop is far superior.
    # There is no significant difference in ips between different chunk sizes
    # reductions for drop_with_chunking is 1.5x higher
    # memory usage for both approaches are identical
    #
    # comparison against drop using a select-key matchspec vs select-object
    # selecting the key results in a very tiny ips improvement, not significant at high table sizes.
    test "drop" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
      sb = {source.id, backend.id}

      Benchee.run(
        %{
          # "drop with chunking, 100 chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sb, :all, to_drop, 100)
          # end,
          # "drop with chunking, 500 chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sb, :all, to_drop, 500)
          # end,
          # "drop with chunking, 1k chunks" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop_with_chunking(sb, :all, to_drop, 1000)
          # end,
          # "select and drop" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop(sb, :all, to_drop, nil)
          # end,
          # "select and drop with select-key" => fn {_input, to_drop} ->
          #   IngestEventQueue.drop(sb, :all, to_drop, :select_key)
          # end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sb, input)
          {input, 500}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    @tag :benchmark
    @tag timeout: :infinity
    @tag :skip
    # benchmark results:
    # using update_element to update the element inplace is hands down superior
    # uses >30-60x less memory, effect increases as with higher batch sizes
    # >8-10.5x more ips, consistent across all batch sizes
    # reductions are the same across all 3.
    test "mark_ingested" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
      sb = {source.id, backend.id}

      Benchee.run(
        %{
          # "mark with :ets.insert/2 batched" => fn {input, _} ->
          #   IngestEventQueue.mark_ingested(sb, input)
          # end,
          # "mark with :ets.insert/2 individually" => fn {input, to_drop} ->
          #   IngestEventQueue.mark_ingested_insert_individually(sb, input)
          # end,
          # "mark with :ets.update_element/2" => fn {input, to_drop} ->
          #   IngestEventQueue.mark_ingested_update_element(sb, input)
          # end,
        },
        inputs: %{
          "1k" => for(_ <- 1..1_000, do: build(:log_event)),
          "500" => for(_ <- 1..500, do: build(:log_event)),
          "100" => for(_ <- 1..100, do: build(:log_event)),
          "10" => for(_ <- 1..10, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sb, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end

    @tag :benchmark
    @tag timeout: :infinity
    @tag :skip
    # benchmark results
    # truncation traversal is around ~8x faster
    # memory usage for traversal is ~38x less for large queues
    # reductions  for traversals is 2x less
    test "truncate" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
      sb = {source.id, backend.id}

      Benchee.run(
        %{
          # "truncate/3 with match_object/3, insert/2, and match_delete/3" => fn {input, _} ->
          #   IngestEventQueue.truncate_no_traversal(sb, :all, 100)
          # end,
          "mark with :ets.select/3 and traversal" => fn {_input, _to_drop} ->
            IngestEventQueue.truncate(sb, :all, 100)
          end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sb, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end

  @tag :benchmark
  @tag timeout: :infinity
  @tag :skip
  # Post-benchmark results:
  # truncate is way slower at large queue sizes, ~x5 slower for 50k. 10k and 1k are comparably close for both.
  # slightly less reductions for drop
  # memeory consumption is identical.
  describe "QueueJanitor" do
    test "truncate vs drop" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, user: user)
      {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
      sb = {source, backend}

      state = %{
        source_id: source.id,
        backend_id: backend.id,
        remainder: 100,
        max: 500,
        purge_ratio: 0.1
      }

      Benchee.run(
        %{
          # "work - truncate" => fn {_input, _resource} ->
          #   QueueJanitor.do_work(state)
          # end,
          "work - drop" => fn {_input, _resource} ->
            QueueJanitor.do_drop(state)
          end
        },
        inputs: %{
          "50k" => for(_ <- 1..50_000, do: build(:log_event)),
          "10k" => for(_ <- 1..10_000, do: build(:log_event)),
          "1k" => for(_ <- 1..1_000, do: build(:log_event))
        },
        # insert the batch
        before_scenario: fn input ->
          :ets.delete_all_objects(tid)
          IngestEventQueue.add_to_table(sb, input)
          {input, nil}
        end,
        time: 3,
        warmup: 1,
        # memory_time: 3,
        reduction_time: 3,
        print: [configuration: false],
        # use extended_statistics to view units of work done
        formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
      )
    end
  end

  # @tag :benchmark
  # @tag timeout: :infinity
  # # @tag :skip
  # # Benchmark results
  # describe "DemandWorker" do
  #   test "fetch" do
  #     user = insert(:user)
  #     source = insert(:source, user: user)
  #     backend = insert(:backend, user: user)
  #     {:ok, tid} = IngestEventQueue.upsert_tid({source, backend})
  #     sb = {source.id, backend.id}

  #     Benchee.run(
  #       %{

  #         # "drop with chunking, 100 chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sb, :all, to_drop, 100)
  #         # end,
  #         # "drop with chunking, 500 chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sb, :all, to_drop, 500)
  #         # end,
  #         # "drop with chunking, 1k chunks" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop_with_chunking(sb, :all, to_drop, 1000)
  #         # end,
  #         # "select and drop" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop(sb, :all, to_drop, nil)
  #         # end,
  #         # "select and drop with select-key" => fn {_input, to_drop} ->
  #         #   IngestEventQueue.drop(sb, :all, to_drop, :select_key)
  #         # end
  #       },
  #       inputs: %{
  #         "50k" => for(_ <- 1..50_000, do: build(:log_event)),
  #         "10k" => for(_ <- 1..10_000, do: build(:log_event)),
  #         "1k" => for(_ <- 1..1_000, do: build(:log_event))
  #       },
  #       # insert the batch
  #       before_scenario: fn input ->
  #         :ets.delete_all_objects(tid)
  #         IngestEventQueue.add_to_table(sb, input)
  #         {input, 500}
  #       end,
  #       time: 3,
  #       warmup: 1,
  #       memory_time: 3,
  #       reduction_time: 3,
  #       print: [configuration: false],
  #       # use extended_statistics to view units of work done
  #       formatters: [{Benchee.Formatters.Console, extended_statistics: true}]
  #     )
  #   end
  # end
end
